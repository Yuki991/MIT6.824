package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	Type int
	Args interface{} // args
}

type OpResult struct {
	Result   interface{}
	Err      Err
	Identity RPCIdentification
}

type AppliedResultMsg struct {
	Result interface{}
	Term   int // term则是用来给等待的线程判断执行的是不是自己的cmd（对应的index）
	Err    Err // applied的cmd有可能执行失败，如server已经不负责这个shard了
}

// 处理re-config的流程：
// 1. 开始：raft apply一个Reconfig Command
// 2. 途中：raft apply一系列ShardInput和ShardOuput
// 3. 结束：收到下一个Reconfig Command / 检查发现所有需要的ShardInput和ShardOutput都成功了
// 注意：下一个Reconfig Command之前，一定会收到所有的ShardInput，而ShardOutput可能不会收到所有的
// （因为input成功一定是通过log知道的，output成功是通过log或者成功发送知道的）
type Config struct {
	Transition bool                // true: 正在从原来的config转变为新的config
	Config     *shardctrler.Config // 目前的config
	OldConfig  *shardctrler.Config // 表示transition中原来的config

	InShard  map[int]int // 记录需要传入的shards, key: shard, value: gid
	OutShard map[int]int // 记录需要传出的shards, key: shard, value: gid
}

type Shard struct {
	ShardID int               // id
	OK      bool              // true: 能提供服务；false: 需要从别的servers传输数据
	KVMap   map[string]string // key value map

	// TODO 每个shard单独维护一个map，记录client在这个shard上一个request的结果，
	// 这样做不是不行，就是空间开销大了点
	LastAppliedMap map[int64]OpResult // last result of clients
}

//
// 主要考虑两件事：重启 & shard转移
//
// shard转移时的requests要怎么处理？等待还是返回Err？
// 等待不科学，会直接阻塞后面所有的requests。返回ErrDataUpdating，告诉client还没准备好。
//
// 一次处理一个re-configuration（2 -> 3 -> 4）
// 能减少处理的麻烦应该，比如一个2->3等你发数据，你直接2->4，数据怎么转移？
//
// 一个replica group中谁来负责将数据传给目标replica group？
// group里每个server的状态不一定是相同的，取决于raft的appliedIndex。
// shard的状态应该“定格”在哪里？谁来detect和process re-configuration？
// 首先是Leader来决定应该没什么问题，要怎么通知followers我要把states定在哪？
// 还是通过Raft吧，把re-configuration写进log里，这样就能保证所有servers都知道应该将
// cmds执行到哪个位置，leader挂了之后新的leader也能继续re-configuration
// #################################################################
// ### 所有在re-configuration之后的cmds都按照新的configuration来处理 ###
// ################################################################
//
// 只有当上一个re-configuration完成之后才能开始下一个re-configuration。
// ##########################################################
// ### 即只有上一个done了之后才能向log里写入re-configuration。 ###
// #########################################################
// 比如C2还没完成，就发现有C3了，但此时不应该将re-C3写入log，
// 一旦写入就应该按照C3来处理requests，可能会有很多requests没人来处理（全在等更新），
// 这不太合理。
//
// group怎么传数据给另外一个group呢？
// 一个naive的idea：传给其中一个服务器然后它来广播，似乎可行，但它宕机了咋办。
// 还是通过Raft来做，数据传输也当作一个cmd写入到Log里，通过Raft写入到大多数服务器上，
// 这样就解决了一致性的问题（每个服务器都知道啥时候开始有数据，能处理相应的请求）
// TODO 这样做的问题在于log会十分臃肿（数据可能会非常大），raft中传输那一块可能会很吃力，
// 最好应该是做一个分块（这个之后再说吧）
//
// TODO 可能需要修改一下shard rebalance的做法
//
// group要怎么知道当前数据已经传输完毕了（完成re-configuration）？
// 轮询每一个需要变更的shard，是否传输完毕（传出/传入），由leader负责。
// leader发现已经ok了就往log里写一个entry通知followers已经完成re-configuration
// 如果leader宕机，新的leader也能知道当前的configuration，以及最新的configuration，
// 可以接手还没完成的re-configuration。
//
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int                 // group id
	ctrlers      []*labrpc.ClientEnd // servers of ctrlers
	ctrClerk     *shardctrler.Clerk  // 用于与ctrlers通信的clerk
	maxraftstate int                 // snapshot if log grows this big
	persister    *raft.Persister     // persister
	dead         int32

	//////////////////////
	//  state machine  //
	////////////////////

	// key: shardid, value: shard
	shardMap map[int]*Shard

	// configuration
	config Config

	/////////////////////////
	// temporary variable //
	///////////////////////

	// 用于执行完applied command后，通知对应的线程给client返回结果
	// key: log index, value: log term & result & err
	appliedResultChMap map[int]chan AppliedResultMsg

	appliedIndex int // 用于snapshot
}

// false: 服务器不负责该shard
func (kv *ShardKV) serveFor(shardID int) bool {
	if shardID < 0 || shardID >= len(kv.config.Config.Shards) {
		return false
	}
	if kv.config.Config.Shards[shardID] != kv.gid {
		return false
	}
	return true
}

func (kv *ShardKV) submitClientReqToRaft(op *Op, id RPCIdentification, key string) (interface{}, Err) {
	kv.mu.Lock()
	shardID := key2shard(key)

	Debug(dInfo, "G%v-S%v receive cmd: %v, shardID:%v, config:%v, shardMap:%v", kv.gid, kv.me, *op, shardID, kv.config, kv.shardMap)

	if !kv.serveFor(shardID) {
		// server不负责这个shard
		defer kv.mu.Unlock()
		return nil, ErrWrongGroup
	}
	if shard, ok := kv.shardMap[shardID]; !ok || !shard.OK {
		// server负责这个shard，但是数据还没有从其他server转移过来
		defer kv.mu.Unlock()
		return nil, ErrDataUpdating
	}

	if opResult := kv.shardMap[shardID].LastAppliedMap[id.ClerkID]; opResult.Identity.RPCID >= id.RPCID {
		// op已经执行过
		defer kv.mu.Unlock()
		if opResult.Identity.RPCID == id.RPCID {
			return opResult.Result, opResult.Err
		} else {
			return nil, ErrOutOfDate
		}
	}

	// submit cmd to raft
	index, term, isLeader := kv.rf.Start(*op)
	if !isLeader || kv.killed() {
		defer kv.mu.Unlock()
		return nil, ErrWrongLeader
	}

	// Debug(dInfo, "G%v-S%v submit cmd to raft, cmd:%v", kv.gid, kv.me, *op)

	// index对应的旧req执行失败
	if ch := kv.appliedResultChMap[index]; ch != nil {
		kv.appliedResultChMap[index] = nil
		appliedResultMsg := AppliedResultMsg{
			Result: nil,
			Term:   term,
			Err:    ErrWrongLeader,
		}
		go func() {
			ch <- appliedResultMsg
		}()
	}
	ch := make(chan AppliedResultMsg)
	kv.appliedResultChMap[index] = ch
	kv.mu.Unlock()

	// wait
	appliedResultMsg := <-ch
	if appliedResultMsg.Term == term {
		// applied的就是提交的cmd
		return appliedResultMsg.Result, appliedResultMsg.Err
	} else {
		// applied的是其他cmd
		return nil, ErrWrongLeader
	}
}

// Get handler
func (kv *ShardKV) GetHandler(args *GetArgs, reply *GetReply) {
	var op Op
	op.Type = OpGet
	op.Args = *args
	result, err := kv.submitClientReqToRaft(&op, args.Identity, args.Key)

	switch err {
	case OK:
		reply.Value = result.(string)
		reply.Err = OK
	default:
		reply.Value = ""
		reply.Err = err
	}
}

// Put&Append handler
func (kv *ShardKV) PutAppendHandler(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	op.Type = args.Op
	op.Args = *args
	_, err := kv.submitClientReqToRaft(&op, args.Identity, args.Key)

	reply.Err = err
}

// DataTransmit handler
// 其他group向该group传输一个shard
func (kv *ShardKV) DataTransmitHandler(args *ShardInputArgs, reply *ShardInputReply) {
	// 幂等性很容易保证, 只有第一次会accept，看一看shard的状态（是否OK）就知道了，所以直接丢进raft里
	// 有没有可能有两个groups给一个group发同一个shard的数据？

	kv.mu.Lock()

	// Debug(dInfo, "G%v-S%v receive data from G%v-S%v, args:%v", kv.gid, kv.me, args.Gid, args.Server, *args)

	if args.ConfigNum < kv.config.Config.Num {
		// wrong config number，也返回OK，表示我接收到你传输的shard了，虽然已经过时了
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	if args.ConfigNum > kv.config.Config.Num {
		// 这种情况表示发送方的config比接收方config的版本更新，接收方没有做好接收的准备， 返回Err
		kv.mu.Unlock()
		reply.Err = ErrWrong
		return
	}
	if shard, ok := kv.shardMap[args.Shard.ShardID]; ok && shard.OK {
		// 这个shard已经传输过了
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	argsCopy := ShardInputArgs{
		ConfigNum: args.ConfigNum,
		Shard:     *args.Shard.Copy(),
		Gid:       args.Gid,
		Server:    args.Server,
	}
	var op Op
	op.Type = OpShardInput
	op.Args = argsCopy

	kv.mu.Lock()

	// Debug(dInfo, "G%v-S%v receive data from G%v-S%v, submit to raft", kv.gid, kv.me, args.Gid, args.Server)

	// submit to raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader || kv.killed() {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	// index对应的旧req执行失败
	if ch := kv.appliedResultChMap[index]; ch != nil {
		kv.appliedResultChMap[index] = nil
		appliedResultMsg := AppliedResultMsg{
			Result: nil,
			Term:   term,
			Err:    ErrWrongLeader,
		}
		go func() {
			ch <- appliedResultMsg
		}()
	}
	ch := make(chan AppliedResultMsg)
	kv.appliedResultChMap[index] = ch
	kv.mu.Unlock()

	// wait
	appliedResultMsg := <-ch

	// defer func() {
	// 	Debug(dInfo, "G%v-S%v receive data from G%v-S%v, reply:%v", kv.gid, kv.me, args.Gid, args.Server, reply)
	// }()

	if appliedResultMsg.Term == term {
		// applied的就是提交的cmd
		reply.Err = appliedResultMsg.Err
		return
	} else {
		// applied的是其他cmd
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) execPutAppend(args *PutAppendArgs) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断server是否还负责这个shard
	shardID := key2shard(args.Key)
	if !kv.serveFor(shardID) {
		return nil, ErrWrongGroup
	}

	// 判断这个cmd是否曾经执行
	if opResult := kv.shardMap[shardID].LastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= args.Identity.RPCID {
		if opResult.Identity.RPCID == args.Identity.RPCID {
			return opResult.Result, opResult.Err
		} else {
			return nil, ErrOutOfDate
		}
	}

	// 执行put or append
	switch args.Op {
	case OpPut:
		kv.shardMap[shardID].KVMap[args.Key] = args.Value
	case OpAppend:
		kv.shardMap[shardID].KVMap[args.Key] += args.Value
	}

	// 保存执行结果
	opResult := OpResult{
		Result:   nil,
		Err:      OK,
		Identity: args.Identity,
	}
	kv.shardMap[shardID].LastAppliedMap[args.Identity.ClerkID] = opResult

	return nil, OK
}

func (kv *ShardKV) execGet(args *GetArgs) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断server是否还负责这个shard
	shardID := key2shard(args.Key)
	if !kv.serveFor(shardID) {
		return nil, ErrWrongGroup
	}

	// 判断这个cmd是否曾经执行
	if opResult := kv.shardMap[shardID].LastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= args.Identity.RPCID {
		if opResult.Identity.RPCID == args.Identity.RPCID {
			return opResult.Result, opResult.Err
		} else {
			return nil, ErrOutOfDate
		}
	}

	// 执行get
	result, ok := kv.shardMap[shardID].KVMap[args.Key]
	err := Err(If(ok, OK, ErrNoKey).(string))
	opResult := OpResult{
		Result:   result,
		Err:      err,
		Identity: args.Identity,
	}
	// 保存执行结果
	kv.shardMap[shardID].LastAppliedMap[args.Identity.ClerkID] = opResult

	return result, err
}

func (kv *ShardKV) execReconfig(args *ReconfigArgs) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Config.Num+1 != args.Config.Num {
		return nil, ErrNotNextConfig
	}
	if kv.config.Transition {
		// 如果有在transition，则结束该re-config，并重新开始新的re-config
		// （对所有servers来说）收到re-config之前一定已经收到了所有的InShard（config中属于自己的shards的数据）
		// 并且leader确定已经将所有需要发送的数据都成功传输给正确的groups了
		for shardId := range kv.config.OutShard {
			delete(kv.shardMap, shardId)
		}
		kv.config.OutShard = nil
		kv.config.Transition = false
	}

	// 开始re-config
	kv.config.Transition = true
	kv.config.OldConfig = kv.config.Config
	c := args.Config.Copy()
	kv.config.Config = &c

	kv.config.InShard = make(map[int]int)
	kv.config.OutShard = make(map[int]int)
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.config.Config.Shards[i] == kv.gid && kv.config.OldConfig.Shards[i] != kv.gid {
			// 需要传入该shard

			// 初始化对应的Shard
			kv.shardMap[i] = &Shard{
				ShardID:        i,
				OK:             false,
				KVMap:          make(map[string]string),
				LastAppliedMap: make(map[int64]OpResult),
			}

			// TODO 如果该shard之前没有groups负责，不需要等待数据传入
			if kv.config.OldConfig.Shards[i] != 0 {
				kv.config.InShard[i] = kv.config.OldConfig.Shards[i]
			} else {
				// 不需要等待数据传入，ok = true
				kv.shardMap[i].OK = true
			}
		}
		if kv.config.Config.Shards[i] != kv.gid && kv.config.OldConfig.Shards[i] == kv.gid {
			// 需要传出该shard

			// 如果该shard之后没有groups负责，不需要传出（不存在这种情况吧）
			if kv.config.Config.Shards[i] != 0 {
				kv.config.OutShard[i] = kv.config.Config.Shards[i]
			}
		}
	}

	// 存在不需要数据传输就能完成re-config的情况
	kv.checkReconfigDone()

	// Debug(dInfo, "G%v-S%v start reconfig: %v", kv.gid, kv.me, kv.config)

	return nil, OK
}

// 其他group向该group传入一个shard
func (kv *ShardKV) execShardInput(args *ShardInputArgs) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum < kv.config.Config.Num {
		// wrong config number，也返回OK，表示我接收到你传输的shard了，虽然已经过时了
		return nil, OK
	}
	if args.ConfigNum > kv.config.Config.Num {
		// 这种情况表示发送方的config比接收方config的版本更新，接收方没有做好接收的准备，返回Err
		return nil, ErrWrong
	}
	if shard, ok := kv.shardMap[args.Shard.ShardID]; ok && shard.OK {
		// 这个shard已经传输过了
		return nil, OK
	}

	// if _, ok := kv.shardMap[args.Shard.ShardID]; !ok {
	// 	Debug(dError, "G%v-S%v detect error, config:%v, shardMap:%v", kv.gid, kv.me, kv.config, kv.shardMap)
	// 	panic("Error")
	// }

	// copy
	shardID := args.Shard.ShardID
	for k, v := range args.Shard.KVMap {
		kv.shardMap[shardID].KVMap[k] = v
	}
	for k, v := range args.Shard.LastAppliedMap {
		kv.shardMap[shardID].LastAppliedMap[k] = v
	}
	// ok = true
	kv.shardMap[shardID].OK = true

	// 更新InShard
	delete(kv.config.InShard, shardID)

	// 检查是否完成reconfig
	kv.checkReconfigDone()

	return nil, OK
}

// 成功向其他group传输一个shard
func (kv *ShardKV) execShardOutput(args *ShardOutputArgs) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.config.Transition || args.ConfigNum != kv.config.Config.Num {
		// 过时的shard output信息
		return nil, OK
	}

	// 更新OutShard
	delete(kv.config.OutShard, args.ShardID)

	// 删除shard对应的数据
	delete(kv.shardMap, args.ShardID)

	// 检查是否完成reconfig
	kv.checkReconfigDone()

	return nil, OK
}

// 检查是否完成re-config，请确认调用前已经上锁
func (kv *ShardKV) checkReconfigDone() {
	if !kv.config.Transition {
		// 已经完成检查
		return
	}

	for range kv.config.InShard {
		return
	}
	for range kv.config.OutShard {
		return
	}

	// 已完成re-config，更新相应变量
	kv.config.Transition = false
	kv.config.OldConfig = nil
	kv.config.InShard = nil
	kv.config.OutShard = nil

	// 主动进行一次snapshot，清除snapshot中多余的数据
	if kv.maxraftstate != -1 {
		// encode state machine
		snapshot := kv.snapshot()
		// 通知raft执行snapshot，丢弃已经不需要的log entries
		kv.rf.Snapshot(kv.appliedIndex, snapshot)
	}

	Debug(dInfo, "G%v-S%v finish re-config num:%v", kv.gid, kv.me, kv.config.Config.Num)
}

// 等待raft apply cmds
func (kv *ShardKV) WaitForRaftAppliedMsg() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh

		// raft apply a cmd
		if applyMsg.CommandValid {
			var result interface{}
			var err Err
			op := applyMsg.Command.(Op)

			kv.mu.Lock()
			kv.appliedIndex = applyMsg.CommandIndex
			kv.mu.Unlock()

			kv.mu.Lock()
			Debug(dCommit, "G%v-S%v apply cmd: %v, config:%v", kv.gid, kv.me, op, kv.config)
			// Debug(dCommit, "G%v-S%v apply cmd, config:%v", kv.gid, kv.me, kv.config)
			kv.mu.Unlock()

			// TODO 好看点的写法？
			switch op.Type {
			case OpPut, OpAppend:
				args := op.Args.(PutAppendArgs)
				result, err = kv.execPutAppend(&args)
			case OpGet:
				args := op.Args.(GetArgs)
				result, err = kv.execGet(&args)
			case OpReconfig:
				args := op.Args.(ReconfigArgs)
				result, err = kv.execReconfig(&args)
				continue
			case OpShardInput:
				args := op.Args.(ShardInputArgs)
				result, err = kv.execShardInput(&args)
			case OpShardOutput:
				args := op.Args.(ShardOutputArgs)
				result, err = kv.execShardOutput(&args)
				continue
			default:
				continue
			}

			kv.mu.Lock()
			// 给index对应的chan发送msg告诉对应线程这个cmd执行完毕
			if ch := kv.appliedResultChMap[applyMsg.CommandIndex]; ch != nil {
				kv.appliedResultChMap[applyMsg.CommandIndex] = nil
				appliedResultMsg := AppliedResultMsg{
					Result: result,
					Term:   applyMsg.CommandTerm,
					Err:    err,
				}
				go func() {
					ch <- appliedResultMsg
				}()
			}

			// 检查是否需要snapshot
			if kv.maxraftstate != -1 && // maxraftstate != -1说明需要snapshot
				kv.persister.RaftStateSize() >= kv.maxraftstate {
				// encode state machine
				snapshot := kv.snapshot()
				// 通知raft执行snapshot，丢弃已经不需要的log entries
				kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
			}

			// 为什么这么大
			// cnt := 0
			// for _, shard := range kv.shardMap {
			// 	for range shard.KVMap {
			// 		cnt++
			// 	}
			// }
			// Debug(dCommit, "G%v-S%v apply cmd: %v, snapshot_size:%v, raftstate_size:%v, key_count:%v, config:%v, shardmap:%v", kv.gid, kv.me, op, kv.persister.SnapshotSize(), kv.persister.RaftStateSize(), cnt, kv.config, kv.shardMap)
			kv.mu.Unlock()
			continue
		}

		// raft require installing snapshot
		if applyMsg.SnapshotValid {
			// install snapshot
			// 1. 更新state machine
			// 2. 重置temporary variables，清理相关线程

			kv.mu.Lock()
			// decode snapshot
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)
			var shardMap map[int]*Shard
			var config Config
			if d.Decode(&shardMap) != nil || d.Decode(&config) != nil {
				// decode error
				// Debug(dError, "G%v-S%v decode snapshot error", kv.gid, kv.me)
				kv.mu.Unlock()
				continue
			} else {
				kv.shardMap = shardMap
				kv.config = config
			}

			// 重置temporary variables & 清理相关线程
			for _, ch := range kv.appliedResultChMap {
				appliedResultMsg := AppliedResultMsg{
					Term:   -1,
					Result: nil,
					Err:    ErrWrong,
				}
				go func(ch chan AppliedResultMsg) {
					ch <- appliedResultMsg
				}(ch)
			}
			kv.appliedResultChMap = make(map[int]chan AppliedResultMsg)

			kv.mu.Unlock()
			continue
		}
	}
}

// 向shardctrler获取config，判断是否需要re-config，需要的话向log写entry，由leader处理
func (kv *ShardKV) CheckConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.config.Transition {
			nowConfigNum := kv.config.Config.Num
			kv.mu.Unlock()

			config := kv.ctrClerk.Query(nowConfigNum + 1)

			Debug(dTimer, "G%v-S%v query config %v, result:%v", kv.gid, kv.me, nowConfigNum, config)

			kv.mu.Lock()
			if !kv.config.Transition && kv.config.Config.Num+1 == config.Num {
				op := Op{
					Type: OpReconfig,
					Args: ReconfigArgs{config},
				}
				kv.rf.Start(op)
			}
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func sendDataTransmitRPC(srv *labrpc.ClientEnd, args *ShardInputArgs, reply *ShardInputReply) bool {
	ok := srv.Call("ShardKV.DataTransmitHandler", args, reply)
	return ok
}

func (kv *ShardKV) DataTransmit(shardID int, gid int) Err {
	// TODO
	kv.mu.Lock()
	shard, ok := kv.shardMap[shardID]
	if !ok {
		// 没有在shardMap找到这个shardid的数据，说明已经成功传输了
		kv.mu.Unlock()
		return OK
	}

	args := ShardInputArgs{
		ConfigNum: kv.config.Config.Num,
		Shard:     *shard.Copy(),
		Gid:       kv.gid,
		Server:    kv.me,
	}
	kv.mu.Unlock()

	for {
		kv.mu.Lock()
		if _, ok := kv.shardMap[shardID]; !ok || kv.killed() {
			kv.mu.Unlock()
			return ErrWrong
		}

		if servers, ok := kv.config.Config.Groups[gid]; ok {
			kv.mu.Unlock()
			for _, server := range servers {
				srv := kv.make_end(server)
				var reply ShardInputReply

				// kv.mu.Lock()
				// Debug(dClient, "G%v-S%v send data transmit to %v", kv.gid, kv.me, server)
				// kv.mu.Unlock()

				// TODO timeout

				if _, ok := CallFunc(200*time.Millisecond, sendDataTransmitRPC, srv, &args, &reply); !ok {
					continue
				}

				switch reply.Err {
				case "", ErrWrongLeader:
					continue
				default:
					return reply.Err
				}
			}
		} else {
			kv.mu.Unlock()
		}

		// TODO timeout
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) CheckTransmitShards() {
	// 如果正在re-config，尝试将需要传出的shard传给正确的groups
	// group中所有的servers都会尝试传输shard，成功后会删除相应shard的数据
	// 另外，如果某个server是leader，则会尝试将“某个shard成功传输”写入log
	// server可以通过三个情况判断shard数据可以删除：
	// 		1. 传输shard获得回复OK
	// 		2. log的entry（对应的shard成功传输）
	// 		3. log中的reconfig（说明leader认为前一个config已经成功了）

	for !kv.killed() {
	Loop:
		for {
			kv.mu.Lock()
			if _, isLeader := kv.rf.GetState(); !kv.config.Transition || !isLeader || kv.killed() {
				kv.mu.Unlock()
				break
			}

			// Debug(dInfo, "G%v-S%v check transmit, config:%v, shardMap:%v", kv.gid, kv.me, kv.config, kv.shardMap)

			shardID, gid := -1, -1
			for k, v := range kv.config.OutShard {
				shardID, gid = k, v
				break
			}
			kv.mu.Unlock()

			if shardID < 0 {
				// 没有需要发送的shard
				break
			}

			// kv.mu.Lock()
			// Debug(dInfo, "G%v-S%v check transmit, shardID:%v, TargetGid:%v, config:%v, shardMap:%v", kv.gid, kv.me, shardID, gid, kv.config, kv.shardMap)
			// kv.mu.Unlock()

			// 发送shard，不断重试直至成功获得回应
			// TODO 一个优化，在某一个shard上卡住了（一直收不到回信），这时候可以先发其它的shard
			err := kv.DataTransmit(shardID, gid)

			kv.mu.Lock()
			switch err {
			case OK:
				// 成功传输（给目标group中的大多数servers），不再需要尝试传输该shard
				// 需要做：1. 往log中写入一条entry通知其它servers 2. 删除该shard相关数据 3. 检查是否完成re-config
				// group中的servers如何确定所有需要传出的shard已经传输成功了？

				// 尝试写入log
				args := ShardOutputArgs{
					ConfigNum: kv.config.Config.Num,
					ShardID:   shardID,
				}
				var op Op
				op.Type = OpShardOutput
				op.Args = args
				kv.rf.Start(op)

				// 删除shard相关数据
				delete(kv.config.OutShard, shardID)
				delete(kv.shardMap, shardID)

				// 检查是否完成re-config
				kv.checkReconfigDone()

				// break
				kv.mu.Unlock()
				break Loop
			case ErrWrong:
				// something wrong
				kv.mu.Unlock()
				break Loop
			default:
				kv.mu.Unlock()
				continue
			}
		}

		// TODO sleep时间？
		time.Sleep(100 * time.Millisecond)
	}
}

// encode state machine
func (kv *ShardKV) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardMap)
	e.Encode(kv.config)
	return w.Bytes()
}

func (kv *ShardKV) initializeFromSnapshot() {
	// 从snapshot初始化shard & config
	// 如果没有就以初始状态启动，config为0，shard为空

	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		// 在没有snapshot状态（初始状态）下启动
		kv.shardMap = make(map[int]*Shard)
		kv.config = Config{
			Transition: false,
			Config:     &shardctrler.Config{Num: 0},
		}
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.shardMap) != nil || d.Decode(&kv.config) != nil {
		// decode error
		kv.shardMap = make(map[int]*Shard)
		kv.config = Config{
			Transition: false,
			Config:     &shardctrler.Config{Num: 0},
		}
		// Debug(dError, "G%v-S%v initialization error", kv.gid, kv.me)
		panic("Snapshot Decode Error")
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// register用到的（需要encode/decode）的struct
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(ReconfigArgs{})
	labgob.Register(ShardInputArgs{})
	labgob.Register(ShardOutputArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.ctrClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.appliedIndex = 0
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.appliedResultChMap = make(map[int]chan AppliedResultMsg)
	kv.initializeFromSnapshot()

	go kv.WaitForRaftAppliedMsg()
	go kv.CheckConfig()
	go kv.CheckTransmitShards()

	return kv
}
