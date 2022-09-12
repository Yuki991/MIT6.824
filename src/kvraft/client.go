package kvraft

import (
	"crypto/rand"
	"math/big"
	"reflect"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu       sync.Mutex // lock
	rpcCount int        // 用于给rpc编号，从1开始，太大了会溢出，取个余
}

// 用于Server判别是否是同一个RPC request
//
type RPCIdentification struct {
	ClerkID int // clerk id，比如ip + port，考虑到这里是单机所以直接用clerk实例的地址了
	RPCID   int // rpc编号，合法编号从1开始
}

func (ck *Clerk) GetNewRPCIdentification() RPCIdentification {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// 取余防止过大，先取余后加1保证编号从1开始
	ck.rpcCount = ck.rpcCount%CountDivisor + 1
	return RPCIdentification{
		ClerkID: int(reflect.ValueOf(ck).Pointer()), // 返回Clerk实例地址作为ClerkID
		RPCID:   ck.rpcCount,
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// TODO You'll have to add code here.
	ck.rpcCount = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// TODO You will have to modify this function.
	args := GetArgs{
		Key:      key,
		Identity: ck.GetNewRPCIdentification(),
	}
	reply := GetReply{}

	// TODO 发送给server的指令只有当正确执行时才会返回OK
	// 返回Err说明：
	// 1. 执行错误，ErrNoKey
	// 2. 该server不是Leader，需要发给其它servers
	// 3. 执行超时，可能的原因：Leader过时了/Server爆炸了
	for {
		// leader过时了可能导致leader id在当前循环变量之前，所以需要一个外层循环
		for i := range ck.servers {
			if !ck.sendGetRPC(i, &args, &reply) {
				// rpc没有收到回复，尝试下一个server
				continue
			}

			switch reply.Err {
			case OK:
				// 成功执行并返回
				return reply.Value
			case ErrNoKey:
				// 成功执行但数据库中不存在该Key
				return ""
			case ErrWrongLeader:
				// 发送对象不是leader，尝试下一个server
				continue
			}
		}
	}

	// return ""
}

// 向server发送get rpc
func (ck *Clerk) sendGetRPC(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// TODO You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Identity: ck.GetNewRPCIdentification(),
	}
	reply := PutAppendReply{}

	// TODO 发送给server的指令只有当正确执行时才会返回OK
	// 返回Err说明：
	// 1. 执行错误，ErrNoKey
	// 2. 该server不是Leader，需要发给其它servers
	// 3. 执行超时，可能的原因：Leader过时了/Server爆炸了
	for {
		// leader过时了可能导致leader id在当前循环变量之前，所以需要一个外层循环
		// TODO 每次都重新去试哪个是leader有点蠢的，可以记录上次成功时的leader是谁
		for i := range ck.servers {
			if !ck.sendPutAppendRPC(i, &args, &reply) {
				// rpc没有收到回复，尝试下一个server
				continue
			}

			switch reply.Err {
			case OK:
				// 成功执行
				return
			case ErrWrongLeader:
				// 发送对象不是leader，尝试下一个server
				continue
			}
		}
	}
}

// send putappend rpc
func (ck *Clerk) sendPutAppendRPC(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
