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

type Config struct {
	Transition bool                // true: 正在从原来的config转变为新的config
	Config     *shardctrler.Config // 目前的config
	OldConfig  *shardctrler.Config // 表示transition中原来的config

	InShard  map[int]int // 记录需要传入的shards, key: shard, value: gid
	OutShard map[int]int // 记录需要传出的shards, key: shard, value: gid
}

type Shard struct {
	ShardID   int               // id
	Preparing bool              // true: 能提供服务；false: 需要从别的servers传输数据
	KVMap     map[string]string // key value map
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
	// key: log index, value: log term & result
	appliedResultChMap map[int]chan AppliedResultMsg
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
	if !kv.serveFor(shardID) {
		// server不负责这个shard
		defer kv.mu.Unlock()
		return nil, ErrWrongGroup
	}
	if !kv.shardMap[shardID].Preparing {
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
	// 幂等性很容易保证, 只有第一次会accept，看一看preparing的状态就知道了，所以直接丢进raft里
	// 有没有可能有两个groups给一个group发同一个shard的数据？
	var argsCopy ShardInputArgs
	argsCopy.ConfigNum = args.ConfigNum
	argsCopy.Shard = *args.Shard.Copy()
	var op Op
	op.Type = OpShardInput
	op.Args = argsCopy

	kv.mu.Lock()
	// submit to raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader || kv.killed() {
		defer kv.mu.Unlock()
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
	err := If(ok, OK, ErrNoKey).(Err)
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

	if kv.config.Transition {
		return nil, ErrConfigTransNow
	}
	if kv.config.Config.Num+1 != args.Config.Num {
		return nil, ErrNotNextConfig
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
			kv.config.InShard[i] = kv.config.OldConfig.Shards[i]
			// 初始化对应的Shard
			kv.shardMap[i] = &Shard{
				ShardID:        i,
				Preparing:      true,
				KVMap:          make(map[string]string),
				LastAppliedMap: make(map[int64]OpResult),
			}
		}
		if kv.config.Config.Shards[i] != kv.gid && kv.config.OldConfig.Shards[i] == kv.gid {
			// 需要传出该shard
			kv.config.OutShard[i] = kv.config.Config.Shards[i]
		}
	}
	return nil, OK
}

// 其他group向该group传入一个shard
func (kv *ShardKV) execShardInput(args *ShardInputArgs) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum != kv.config.Config.Num {
		// wrong config number，也返回OK，表示我接收到你传输的shard了
		return nil, OK
	}
	if !kv.shardMap[args.Shard.ShardID].Preparing {
		// 这个shard已经传输过了
		return nil, OK
	}

	// copy
	shardID := args.Shard.ShardID
	for k, v := range args.Shard.KVMap {
		kv.shardMap[shardID].KVMap[k] = v
	}
	for k, v := range args.Shard.LastAppliedMap {
		kv.shardMap[shardID].LastAppliedMap[k] = v
	}
	kv.shardMap[shardID].Preparing = false

	// 更新InShard
	delete(kv.config.InShard, shardID)

	// 检查是否完成reconfig
	go func() {
		kv.checkReconfigDone()
	}()

	return nil, OK
}

// 成功向其他group传输一个shard
func (kv *ShardKV) execShardOutput(args *ShardOutputArgs) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 更新OutShard
	delete(kv.config.OutShard, args.ShardID)

	// 删除shard对应的数据
	delete(kv.shardMap, args.ShardID)

	// 检查是否完成reconfig
	go func() {
		kv.checkReconfigDone()
	}()

	return nil, OK
}

// 检查是否完成re-config
func (kv *ShardKV) checkReconfigDone() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for range kv.config.InShard {
		return
	}
	for range kv.config.OutShard {
		return
	}

	// 已完成re-config
	kv.config.Transition = false
	kv.config.OldConfig = nil
	kv.config.InShard = nil
	kv.config.OutShard = nil
}

// 等待raft apply
func (kv *ShardKV) WaitForRaftAppliedMsg() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh

		// raft apply a cmd
		if applyMsg.CommandValid {
			var result interface{}
			var err Err
			op := applyMsg.Command.(Op)

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
			kv.mu.Unlock()

			// TODO snapshot
			continue
		}

		// raft require installing snapshot
		if applyMsg.SnapshotValid {
			// TODO
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

func (kv *ShardKV) DataTransmit(shardID int, gid int) Err {
	// TODO 状态不太对，感觉会写出问题
	kv.mu.Lock()
	shard, ok := kv.shardMap[shardID]
	if !ok {
		// 没有在shardMap找到这个shardid的数据，说明已经成功传输了
		defer kv.mu.Unlock()
		return OK
	}

	var args ShardInputArgs
	args.ConfigNum = kv.config.Config.Num
	args.Shard = *shard.Copy()
	kv.mu.Unlock()

	for {
		kv.mu.Lock()
		if _, ok := kv.shardMap[shardID]; !ok || kv.killed() {
			kv.mu.Unlock()
			return ErrWrong
		}
		kv.mu.Unlock()

		kv.mu.Lock()
		if servers, ok := kv.config.Config.Groups[gid]; ok {
			kv.mu.Unlock()
			for _, server := range servers {
				srv := kv.make_end(server)
				var reply ShardInputReply

				// TODO timeout
				ok := CallFunc(200*time.Millisecond, sendDataTransmit, srv, &args, &reply)
				if !ok || reply.Err == ErrWrongLeader {
					continue
				}
				return reply.Err
			}
		} else {
			kv.mu.Unlock()
		}

		// TODO
		time.Sleep(100 * time.Millisecond)
	}
}

func sendDataTransmit(srv *labrpc.ClientEnd, args *ShardInputArgs, reply *ShardInputReply) {
	srv.Call("ShardKV.DataTransmitHandler", args, reply)
}

func (kv *ShardKV) CheckTransmitShards() {
	// TODO 如果正在re-config，尝试将需要传出的shard传给正确的groups
	// 先实现成由leader处理，可以尝试改成followers也transmit shards

	for !kv.killed() {
		for {
			kv.mu.Lock()
			if !kv.config.Transition {
				kv.mu.Unlock()
				break
			}

			shardID, gid := -1, -1
			for k, v := range kv.config.InShard {
				shardID, gid = k, v
				break
			}
			kv.mu.Unlock()

			if shardID < 0 {
				// 没有需要发送的shard
				break
			}

			// 发送shard，不断重试直至成功获得回应
			err := kv.DataTransmit(shardID, gid)
			if err == OK {
				// 成功传输（给目标group中的大多数servers），不再需要尝试传输该shard
				// TODO 需要做：1. 往log中写入一条entry通知其它servers 2. 删除该shard相关数据
			} else {
				// 由于已经传输等原因导致失败
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
	// TODO
	// shard & config
	// 初始config怎么设置？
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
	// Your code here, if desired.
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
	// TODO register用到的（需要encode/decode）的struct
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.ctrClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.appliedResultChMap = make(map[int]chan AppliedResultMsg)
	kv.initializeFromSnapshot()

	go kv.WaitForRaftAppliedMsg()
	go kv.CheckConfig()
	go kv.CheckTransmitShards()

	return kv
}
