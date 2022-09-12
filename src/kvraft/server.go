package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Op       string            // the type of operation (get/put/append)
	Key      string            // key
	Value    string            // value
	Identity RPCIdentification // 唯一标识
}

// 记录rpc的结果
type OpResult struct {
	Value    *string           // value，传指针
	Identity RPCIdentification // 唯一标识
}

// 用于执行完applied command后，通知对应的线程给client返回结果
// 注意这个string可能很大，记得传指针（这种中间状态全部应该传指针）
type AppliedResultMsg struct {
	// Index int // index
	Term  int     // term
	Value *string // value，传指针
}

//
// 向raft提交command并不是直接执行，需要等待command在大多数servers的log上复制完成之后
// （在command放到了state machine上执行之后）才是执行完成，再之后才能够将result返回给client
// 在等待命令执行这段时间里，会有其他request到来，怎么维护这个顺序
// 另外，怎么知道提交的command已经被执行了
// （注意raft提交了一个command并不意味着一定有一个rpc request在等待返回，有可能是宕机后重启）
// 完成了command但是因为宕机没有把response发回给client，然后client又发了一个request，
// 此时应该直接返回前一次的执行结果，这种情况又应该怎么实现
//
// 还有，需要维护一个state machine，需要哪些states？
// 1. 一个key value map
// 2. 需要知道哪些指令是已经执行过的，或者是写入了log但还没apply的（不对，后者应该对sm没有影响）
//    如果存在commands committed但未applied，然后宕机重启，这时候client发来了一个之前已经
//    committed但是还没有applied的command怎么办（恢复state之后server应该是不知道这个command
//    已经写到了log里但是还没执行）
//    // 可能的方案：
//    // 首先可以用一个map来记录所有cmd的执行结果，由于可以假定同一时刻一个client只会发送一条cmd，
//    // 所以只需要记录最后一个成功执行的cmd的id与结果，比id小的request直接不管，等于的直接返回结果
//    // 对于可能存在已经commit但未applied的cmd，考虑到这些cmd相应的index一定<=当前commtitIndex，
//    // 所以最保险的（最笨的）方法就是等待<=当前commitIndex所有cmd都执行完毕后，再看是否需要执行到来的cmd
//    // 但是这个与设计不符呀，设计目标肯定是不希望上层service读raft的参数
//    // 宕机重启要怎么做（先不考虑snapshot的事）？
//    // 这时候就会出现有大量committed但未apply到state machine上的cmds，
//    // 所以还是需要等待appliedIndex=当前commitIndex才能提供service
//    // 还有个问题，leader宕机了，别的leader需要notice已经commit但未apply的cmds
//
//    可能的方案2：
//    允许相同的request写进log，只要保证它们不会被执行两次就行。
//    那么每个cmd除了需要记录每个操作是什么之外，还需要记录一个唯一标识
//    state machine中额外记录：
//    每个client最后执行的cmd的时间戳，只有大于该时间戳的cmd会被执行，等于的直接返回结果
//    TODO 这样可能会导致一个cmd多次写入log，这种情况应该出现不多？
//
// cmd的顺序怎么定？是按照它们达到leader的顺序，还是它们的发出顺序？
// 到达顺序，发出顺序怎么实现呢？我这里的事务都执行完了，你告诉我这个cmd应该在你之前完成（因为网络原因等
// 导致cmd到达时间晚），做不到。
//
// 怎么知道某个handler提交的cmd已经被执行可以返回了呢？
// 最简单的就是开一个chan，等待结果放进这个chan里。
// 这里有个问题，一个leader接收到了很多cmds，然后开了很多线程等待执行结果
// 这时候leader过期了，需要个办法中断这些线程并返回Error
// 解决办法：就让线程等待，直到有相同的index的cmd applied，这时候去检测applied的cmd和我的cmd是否相同，
// 不同的话就说明当时leader已经换人了，返回Error。（注意，其实不需要比较cmd，只需要比较Term是否相同，
// 因为Raft保证Index相同且Term相同的话对应的log entry一定相同）
// 所以最后的解决方案就是，每次开个chan等待执行结果，把这个chan放进一个map里（map的key是index），
// 每次执行完一个cmd就把result放进对应的index的chan里。有没有可能同时有两个线程等待相同index？
// 有可能。但此时这两个线程对应的log entry肯定不是同一个term，所以旧的term的那一个线程可以直接返回Error了
// （因为log里已经没有它了，不可能被执行）
//
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// state machine //

	// key value map，实际存储key和value的map
	kvMap map[string]string
	// TODO 用于记录每个client最后执行的cmd以及记录执行结果，
	// 保证幂等性，注意如果client过多会出（内存）问题
	// key: client_id
	// value: op result
	lastAppliedMap map[int]OpResult

	// temporary variable //

	// 用于执行完applied command后，通知对应的线程给client返回结果
	// key: log index
	// value: log term & result
	appliedResultChMap map[int]chan AppliedResultMsg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	Debug(dInfo, "S%d receive Get request, args:%v", kv.me, args)

	op := Op{
		Op:       OpGet,
		Key:      args.Key,
		Value:    "",
		Identity: args.Identity,
	}

	// 判断这个request是否曾经执行过，若是则直接返回结果
	kv.mu.Lock()
	if opResult := kv.lastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= op.Identity.RPCID {
		// 已经执行过
		defer kv.mu.Unlock()
		if opResult.Identity.RPCID == op.Identity.RPCID {
			reply.Err = OK
			reply.Value = *opResult.Value
		} else {
			// 过时的请求，不处理应该也没问题
			reply.Err = ErrOutOfDate
		}
		return
	}
	kv.mu.Unlock()

	// 将command提交到raft中
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 该server不是leader，返回Err
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	Debug(dInfo, "S%d send Get to raft, args:%v", kv.me, args)

	// 提交command后新建一个chan放入appliedResultChMap中，然后等待raft apply command
	// 如果map中已有别的chan，则放入msg告诉该chan的对应线程command执行失败
	kv.mu.Lock()
	if ch := kv.appliedResultChMap[index]; ch != nil {
		kv.appliedResultChMap[index] = nil
		appliedResultMsg := AppliedResultMsg{
			Term:  term,
			Value: nil,
		}
		go func() {
			ch <- appliedResultMsg
		}()
	}
	// 新建chan放入map中
	ch := make(chan AppliedResultMsg)
	kv.appliedResultChMap[index] = ch
	kv.mu.Unlock()

	// 等待结果
	appliedResultMsg := <-ch
	if appliedResultMsg.Term == term {
		// command正确执行完毕
		reply.Err = OK
		reply.Value = *appliedResultMsg.Value
	} else {
		// 该index下的log entry对应的command不是提交的command
		reply.Err = ErrWrongLeader
		reply.Value = ""
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Debug(dInfo, "S%d receive PutAppend request, args:%v", kv.me, args)

	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		Identity: args.Identity,
	}

	// 判断这个request是否曾经执行过，若是则直接返回结果
	kv.mu.Lock()
	if opResult := kv.lastAppliedMap[args.Identity.ClerkID]; opResult.Identity.RPCID >= op.Identity.RPCID {
		// 已经执行过
		defer kv.mu.Unlock()
		if opResult.Identity.RPCID == op.Identity.RPCID {
			reply.Err = OK
		} else {
			// 过时的请求，不处理应该也没问题
			reply.Err = ErrOutOfDate
		}
		return
	}
	kv.mu.Unlock()

	// 将command提交到raft中
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 该server不是leader，返回Err
		reply.Err = ErrWrongLeader
		return
	}

	Debug(dInfo, "S%d send PutAppend to raft, args:%v", kv.me, args)

	// 提交command后新建一个chan放入appliedResultChMap中，然后等待raft apply command
	// 如果map中已有别的chan，则放入msg告诉该chan的对应线程command执行失败
	kv.mu.Lock()
	if ch := kv.appliedResultChMap[index]; ch != nil {
		kv.appliedResultChMap[index] = nil
		appliedResultMsg := AppliedResultMsg{
			Term:  term,
			Value: nil,
		}
		go func() {
			ch <- appliedResultMsg
		}()
	}
	// 新建chan放入map中
	ch := make(chan AppliedResultMsg)
	kv.appliedResultChMap[index] = ch
	kv.mu.Unlock()

	// 等待结果
	appliedResultMsg := <-ch
	if appliedResultMsg.Term == term {
		// command正确执行完毕
		reply.Err = OK
	} else {
		// 该index下的log entry对应的command不是提交的command
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) execGet(op *Op) *string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断这个cmd是否已经执行过（需要这个判断是因为可能有相同的command写入到log中）
	if opResult := kv.lastAppliedMap[op.Identity.ClerkID]; opResult.Identity.RPCID >= op.Identity.RPCID {
		// 已经执行过
		return opResult.Value
	}

	// 执行Get
	value := kv.kvMap[op.Key]
	opResult := OpResult{
		Value:    &value,
		Identity: op.Identity,
	}
	// 更新client最后处理的cmd的信息
	kv.lastAppliedMap[op.Identity.ClerkID] = opResult
	// return
	return opResult.Value
}

func (kv *KVServer) execPutAppend(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断这个cmd是否已经执行过（需要这个判断是因为可能有相同的command写入到log中）
	if opResult := kv.lastAppliedMap[op.Identity.ClerkID]; opResult.Identity.RPCID >= op.Identity.RPCID {
		// 已经执行过
		return
	}

	// 执行put/append
	switch op.Op {
	case OpPut:
		kv.kvMap[op.Key] = op.Value
	case OpAppend:
		kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
	}
	opResult := OpResult{
		Value:    nil, // putappend没有返回值
		Identity: op.Identity,
	}
	// 更新client最后处理的cmd的信息
	kv.lastAppliedMap[op.Identity.ClerkID] = opResult
}

// long-running work, waiting for applyMsg from raft
func (kv *KVServer) WaitForAppliedMsg() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh

		// raft apply a command
		if applyMsg.CommandValid {
			var value *string
			op := applyMsg.Command.(Op)

			// 执行applied cmd
			switch op.Op {
			case OpGet:
				value = kv.execGet(&op)
			case OpPut, OpAppend:
				kv.execPutAppend(&op)
			}

			Debug(dCommit, "S%d apply a command:%v, current kv:%v", kv.me, op, kv.kvMap)

			// 如果有index对应的channel在等待，就发送一条message过去
			kv.mu.Lock()
			if ch := kv.appliedResultChMap[applyMsg.CommandIndex]; ch != nil {
				kv.appliedResultChMap[applyMsg.CommandIndex] = nil
				appliedResultMsg := AppliedResultMsg{
					Term:  applyMsg.CommandTerm,
					Value: value,
				}
				go func() {
					ch <- appliedResultMsg
				}()
			}
			kv.mu.Unlock()
			continue
		}

		// raft install snapshot
		if applyMsg.SnapshotValid {
			// TODO
			continue
		}
	}
}

func (kv *KVServer) DebugTicker() {
	start := time.Now()
	for time.Since(start).Seconds() < 30 {
		time.Sleep(100 * time.Millisecond)

		kv.mu.Lock()
		// Debug(dTimer, "S%d killed:%v", kv.me, kv.killed())
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.lastAppliedMap = make(map[int]OpResult)
	kv.appliedResultChMap = make(map[int]chan AppliedResultMsg)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.WaitForAppliedMsg()
	// go kv.DebugTicker()

	return kv
}
