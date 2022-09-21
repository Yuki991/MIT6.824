package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister
	dead      int32 // set by Kill()

	////////////////////
	// state machine //
	//////////////////

	configs []Config // indexed by config num

	// 用于记录每个client最后执行的cmd以及记录执行结果，
	// 保证幂等性，注意如果client过多会出（内存）问题
	// key: client_id
	// value: rpc result
	lastAppliedMap map[int64]OpResult

	/////////////////////////
	// temporary variable //
	///////////////////////

	// 长时间运行之后这个map也会变得过大，需要一段时间后重置一下
	// 用于执行完applied command后，通知对应的线程给client返回结果
	// key: log index
	// value: log term & result
	appliedResultChMap map[int]chan AppliedResultMsg
}

type Op struct {
	Type int         // join 0 / leave 1 / move 2 / query 3
	Args interface{} // args
}

type OpResult struct {
	Result   interface{}
	Identity RPCIdentification
}

type AppliedResultMsg struct {
	Result interface{}
	Term   int
}

func (sc *ShardCtrler) submitCmdToRaft(op *Op, id RPCIdentification) (interface{}, Err) {
	sc.mu.Lock()
	if opResult := sc.lastAppliedMap[id.ClerkID]; opResult.Identity.RPCID >= id.RPCID {
		// op已经执行过
		defer sc.mu.Unlock()
		if opResult.Identity.RPCID == id.RPCID {
			return opResult.Result, OK
		} else {
			return nil, ErrOutOfDate
		}
	}

	// submit cmd to raft
	index, term, isLeader := sc.rf.Start(*op)
	if !isLeader || sc.killed() {
		defer sc.mu.Unlock()
		return nil, ErrWrongLeader
	}

	Debug(dError, "S%d submit command to raft, index:%v, term:%v, op:%v", sc.me, index, term, op)
	// fmt.Printf("S%d submit command to raft, index:%v, term:%v, op:%v", sc.me, index, term, op)

	// index对应的原来的rpc肯定执行失败
	if ch := sc.appliedResultChMap[index]; ch != nil {
		sc.appliedResultChMap[index] = nil
		appliedResultMsg := AppliedResultMsg{
			Term:   term,
			Result: nil,
		}
		go func() {
			ch <- appliedResultMsg
		}()
	}
	// 新建chan放到map对应index上
	ch := make(chan AppliedResultMsg)
	sc.appliedResultChMap[index] = ch
	sc.mu.Unlock()

	appliedResultMsg := <-ch
	if appliedResultMsg.Term == term {
		return appliedResultMsg.Result, OK
	} else {
		return nil, ErrWrongLeader
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	Debug(dTest, "S%d receive join request, args:%v", sc.me, args)
	op := Op{
		Type: OpJoin,
		Args: *args,
	}

	_, err := sc.submitCmdToRaft(&op, args.Identity)
	if err == ErrWrongLeader {
		reply.WrongLeader = true
		reply.Err = err
	} else {
		reply.WrongLeader = false
		reply.Err = err
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	Debug(dTest, "S%d receive leave request, args:%v", sc.me, args)
	op := Op{
		Type: OpLeave,
		Args: *args,
	}

	_, err := sc.submitCmdToRaft(&op, args.Identity)
	if err == ErrWrongLeader {
		reply.WrongLeader = true
		reply.Err = err
	} else {
		reply.WrongLeader = false
		reply.Err = err
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	Debug(dTest, "S%d receive move request, args:%v", sc.me, args)
	op := Op{
		Type: OpMove,
		Args: *args,
	}

	_, err := sc.submitCmdToRaft(&op, args.Identity)
	if err == ErrWrongLeader {
		reply.WrongLeader = true
		reply.Err = err
	} else {
		reply.WrongLeader = false
		reply.Err = err
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	Debug(dTest, "S%d receive query request, args:%v", sc.me, args)
	op := Op{
		Type: OpQuery,
		Args: *args,
	}

	result, err := sc.submitCmdToRaft(&op, args.Identity)
	switch err {
	case OK:
		reply.Config = sc.configs[result.(int)]
		reply.WrongLeader = false
		reply.Err = err
	case ErrWrongLeader:
		reply.Config = Config{}
		reply.WrongLeader = true
		reply.Err = err
	default:
		reply.Config = Config{}
		reply.WrongLeader = false
		reply.Err = err
	}
}

// re-balance，需要是“确定的”算法
// 因为数据量很小，所以直接用暴力了，将最多的放一个到最少的那里
func (sc *ShardCtrler) rebalance(c *Config) {
	arr := make([]int, 0)
	cntMap := make(map[int]int)
	for k := range c.Groups {
		cntMap[k] = 0
		arr = append(arr, k)
	}
	for i := range c.Shards {
		if _, ok := cntMap[c.Shards[i]]; !ok {
			c.Shards[i] = 0
		} else {
			cntMap[c.Shards[i]]++
		}
	}
	if len(arr) == 0 {
		return
	}
	// arr元素顺序是不确定的，sort一下确定顺序
	sort.Ints(arr)

	for {
		maxGid, minGid := arr[0], arr[0]
		for _, v := range arr {
			if cntMap[v] > cntMap[maxGid] {
				maxGid = v
			}
			if cntMap[v] < cntMap[minGid] {
				minGid = v
			}
		}

		var x int
		x = 0
		for x < len(c.Shards) && c.Shards[x] != 0 {
			x++
		}
		if x < len(c.Shards) {
			// 还有没分配的shard，直接分给最少的
			c.Shards[x] = minGid
			cntMap[minGid]++
			continue
		}

		if cntMap[maxGid]-cntMap[minGid] <= 1 {
			// 全部分配完，并且balance
			break
		}

		x = 0
		for c.Shards[x] != maxGid {
			x++
		}
		c.Shards[x] = minGid
		cntMap[maxGid]--
		cntMap[minGid]++
	}
	Debug(dError, "S%v rebalance, shards:%v, group:%v, cntMap:%v", sc.me, c.Shards, c.Groups, cntMap)
}

func (sc *ShardCtrler) execJoin(args *JoinArgs) interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 判断是否已经执行过
	if opResult := sc.lastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= args.Identity.RPCID {
		return opResult.Result
	}

	// 执行join
	config := sc.configs[len(sc.configs)-1].Copy()
	config.Num++
	for k, v := range args.Servers {
		config.Groups[k] = v
	}
	sc.rebalance(&config)

	sc.configs = append(sc.configs, config)

	// 更新lastAppliedMap
	opResult := OpResult{
		Result:   nil,
		Identity: args.Identity,
	}
	sc.lastAppliedMap[opResult.Identity.ClerkID] = opResult

	return nil
}

func (sc *ShardCtrler) execLeave(args *LeaveArgs) interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 判断是否已经执行过
	if opResult := sc.lastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= args.Identity.RPCID {
		return opResult.Result
	}

	// 执行leave
	config := sc.configs[len(sc.configs)-1].Copy()
	config.Num++
	for _, v := range args.GIDs {
		delete(config.Groups, v)
	}
	sc.rebalance(&config)

	sc.configs = append(sc.configs, config)

	// 更新lastAppliedMap
	opResult := OpResult{
		Result:   nil,
		Identity: args.Identity,
	}
	sc.lastAppliedMap[opResult.Identity.ClerkID] = opResult

	return nil
}

func (sc *ShardCtrler) execMove(args *MoveArgs) interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 判断是否已经执行过
	if opResult := sc.lastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= args.Identity.RPCID {
		return opResult.Result
	}

	// 执行move
	config := sc.configs[len(sc.configs)-1].Copy()
	config.Num++
	config.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, config)
	// 更新lastAppliedMap
	opResult := OpResult{
		Result:   nil,
		Identity: args.Identity,
	}
	sc.lastAppliedMap[opResult.Identity.ClerkID] = opResult

	return nil
}

func (sc *ShardCtrler) execQuery(args *QueryArgs) interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 判断是否已经执行过
	if opResult := sc.lastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= args.Identity.RPCID {
		return opResult.Result
	}

	// 执行Query
	var result int
	if args.Num < 0 || args.Num >= len(sc.configs) {
		result = len(sc.configs) - 1
	} else {
		result = args.Num
	}
	// 更新lastAppliedMap
	opResult := OpResult{
		Result:   result,
		Identity: args.Identity,
	}
	sc.lastAppliedMap[opResult.Identity.ClerkID] = opResult

	return result
}

func (sc *ShardCtrler) WaitForAppliedMsg() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh

		// raft apply a cmd
		if applyMsg.CommandValid {
			var result interface{}
			op := applyMsg.Command.(Op)

			switch op.Type {
			case OpJoin:
				args := op.Args.(JoinArgs)
				result = sc.execJoin(&args)
			case OpLeave:
				args := op.Args.(LeaveArgs)
				result = sc.execLeave(&args)
			case OpMove:
				args := op.Args.(MoveArgs)
				result = sc.execMove(&args)
			case OpQuery:
				args := op.Args.(QueryArgs)
				result = sc.execQuery(&args)
			}

			Debug(dInfo, "S%d apply command:%v", sc.me, op)

			sc.mu.Lock()
			// 给index对应的chan发送msg告诉对应线程cmd已执行完毕
			if ch := sc.appliedResultChMap[applyMsg.CommandIndex]; ch != nil {
				sc.appliedResultChMap[applyMsg.CommandIndex] = nil
				appliedResultMsg := AppliedResultMsg{
					Term:   applyMsg.CommandTerm,
					Result: result,
				}
				go func() {
					ch <- appliedResultMsg
				}()
			}
			sc.mu.Unlock()
			continue
		}

		// raft install snapshot
		if applyMsg.SnapshotValid {
			continue
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	// register
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastAppliedMap = make(map[int64]OpResult)
	sc.appliedResultChMap = make(map[int]chan AppliedResultMsg)
	go sc.WaitForAppliedMsg()
	return sc
}
