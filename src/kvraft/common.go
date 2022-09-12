package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutOfDate   = "ErrOutOfDate"
)

const (
	CountDivisor = 1000000007
)

const (
	OpGet       = "Get"
	OpPut       = "Put"
	OpAppend    = "Append"
	OpPutAppend = "PutAppend"
)

type Err string

// Put or Append
// TODO 除了Op,Key,Value还需要什么？
// 需要用于判断这个request是否已经“出现过”的额外信息
// （因网络延迟/leader换人等原因导致的request超时而重发的requests）
//
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Identity RPCIdentification // 用于识别rpc request
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	Identity RPCIdentification // 用于识别rpc request
}

type GetReply struct {
	Err   Err // OK/ErrNoKey/ErrWrongLeader，只有当请求正确执行完毕才会返回OK
	Value string
}
