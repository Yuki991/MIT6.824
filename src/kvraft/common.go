package kvraft

import (
	"reflect"
	"time"
)

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

// timeout: 超时时间
// f:       传入的函数
// args:    传入的参数
// 功能：    传入一个函数及参数，用该参数调用函数，如果函数没有在timeout时间内结束，则返回false，否则返回true
func CallFunc(timeout time.Duration, f interface{}, args ...interface{}) ([]interface{}, bool) {
	ch := make(chan bool)
	_f := reflect.ValueOf(f)
	var _r []reflect.Value
	_args := make([]reflect.Value, len(args))
	for i := range args {
		_args[i] = reflect.ValueOf(args[i])
	}

	go func() {
		_r = _f.Call(_args)
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

	if ok {
		result := make([]interface{}, len(_r))
		for i, v := range _r {
			result[i] = (interface{})(v)
		}
		return result, true
	}
	return nil, false
}
