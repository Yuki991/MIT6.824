package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// mu         sync.Mutex // lock
	rpcCount   int // 用于给rpc编号，从1开始，太大了会溢出，取个余
	lastLeader int // 记录上一次成功发送请求时的leader
}

// 用于Server判别是否是同一个RPC request
//
type RPCIdentification struct {
	ClerkID int // clerk id，比如ip + port，考虑到这里是单机所以直接用clerk实例的地址了
	RPCID   int // rpc编号，合法编号从1开始
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// timeout: 超时时间
// f:       传入的函数
// args:    传入的参数
// 功能：    传入一个函数及参数，用该参数调用函数，如果函数没有在timeout时间内结束，则返回false，否则返回true
func CallFunc(timeout time.Duration, f interface{}, args ...interface{}) bool {
	ch := make(chan bool)
	_f := reflect.ValueOf(f)
	_args := make([]reflect.Value, len(args))
	for i := range args {
		_args[i] = reflect.ValueOf(args[i])
	}

	go func() {
		_f.Call(_args)
		ch <- true
	}()
	go func() {
		time.Sleep(timeout)
		ch <- false
	}()

	ok := <-ch
	go func() {
		// 保证前面两个goroutine能够结束
		<-ch
	}()
	return ok
}

func (ck *Clerk) GetNewRPCIdentification() RPCIdentification {
	// ck.mu.Lock()
	// defer ck.mu.Unlock()
	// 取余防止过大，先取余后加1保证编号从1开始
	ck.rpcCount = ck.rpcCount%CountDivisor + 1
	return RPCIdentification{
		ClerkID: int(reflect.ValueOf(ck).Pointer()), // 返回Clerk实例地址作为ClerkID
		RPCID:   ck.rpcCount,
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.rpcCount = 0
	ck.lastLeader = 0
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

	Debug(dError, "client %v create new request, args:%v", reflect.ValueOf(ck).Pointer(), args)
	defer Debug(dError, "client %v finish request", reflect.ValueOf(ck).Pointer())

	// 发送给server的指令只有当正确执行时才会返回OK
	// 返回Err说明：
	// 1. 执行错误，ErrNoKey
	// 2. 该server不是Leader，需要发给其它servers
	// 3. 执行超时，可能的原因：Leader过时了/Server爆炸了
	for {
		// leader过时了可能导致leader id在当前循环变量之前，所以需要一个外层循环
		for i := range ck.servers {
			server := (ck.lastLeader + i) % len(ck.servers)
			reply := GetReply{}
			Debug(dError, "client %v send new request to S%d", reflect.ValueOf(ck).Pointer(), server)
			// 这个果然跟之前有一样的问题，如果server断开连接导致没有返回，这个会一直阻塞...需要自己手动写一个超时处理
			if ok := CallFunc(300*time.Millisecond, ck.sendGetRPC, server, &args, &reply); !ok {
				// if !ck.sendGetRPC(server, &args, &reply) {
				// rpc没有收到回复，尝试下一个server
				continue
			}

			Debug(dError, "client %v receive response from S%d, reply: %v", reflect.ValueOf(ck).Pointer(), server, reply)

			switch reply.Err {
			case OK:
				// 成功执行并返回，更新lastLeader
				ck.lastLeader = server
				return reply.Value
			case ErrNoKey:
				// 成功执行但数据库中不存在该Key
				return ""
			case ErrWrongLeader:
				// 发送对象不是leader，尝试下一个server
				continue
			case ErrOutOfDate:
				str := fmt.Sprintf("Something wrong. Reply:%v", reply)
				panic(str)
			default:
				// 不知道出于什么原因（仿照网络错误？），reply是空的
				continue
			}
		}
		// 没有找到leader，可能是还没选出新的leader，阻塞一小段时间
		time.Sleep(10 * time.Millisecond)
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
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Identity: ck.GetNewRPCIdentification(),
	}

	Debug(dError, "client %v create new request, args:%v", reflect.ValueOf(ck).Pointer(), args)
	defer Debug(dError, "client %v finish request", reflect.ValueOf(ck).Pointer())

	// 发送给server的指令只有当正确执行时才会返回OK
	// 返回Err说明：
	// 1. 执行错误，ErrNoKey
	// 2. 该server不是Leader，需要发给其它servers
	// 3. 执行超时，可能的原因：Leader过时了/Server爆炸了
	for {
		// leader过时了可能导致leader id在当前循环变量之前，所以需要一个外层循环
		// TODO 每次都重新去试哪个是leader有点蠢的，可以记录上次成功时的leader是谁
		// 这个写法还有个问题就是如果当前没有leader，它会发送大量无用的请求
		for i := range ck.servers {
			server := (i + ck.lastLeader) % len(ck.servers)
			reply := PutAppendReply{}
			Debug(dError, "client %v send new request to S%d", reflect.ValueOf(ck).Pointer(), server)
			if ok := CallFunc(300*time.Millisecond, ck.sendPutAppendRPC, server, &args, &reply); !ok || reply.Err == "" {
				// if !ck.sendPutAppendRPC(server, &args, &reply) {
				// rpc没有收到回复，尝试下一个server
				continue
			}

			Debug(dError, "client receive PutAppend reply:%v, args:%v", reply, args)

			switch reply.Err {
			case OK:
				// 成功执行，更新lastLeader
				ck.lastLeader = server
				return
			case ErrWrongLeader:
				// 发送对象不是leader，尝试下一个server
				continue
			case ErrOutOfDate:
				str := fmt.Sprintf("Something wrong. Reply:%v", reply)
				panic(str)
			default:
				continue
			}
		}
		// 没有找到leader，阻塞一小段时间
		time.Sleep(10 * time.Millisecond)
	}
}

// send putappend rpc
func (ck *Clerk) sendPutAppendRPC(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
