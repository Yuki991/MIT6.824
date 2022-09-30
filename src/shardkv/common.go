package shardkv

import (
	"fmt"
	"reflect"
	"time"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrWrong          = "Wrong" // something wrong
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrDataUpdating   = "ErrDataUpdating"
	ErrOutOfDate      = "ErrOutOfDate"
	ErrConfigTransNow = "ErrConfigTransNow" // 正在re-config
	ErrNotNextConfig  = "ErrNotNextConfig"  // 不是"下一个"config，拒绝re-config
)

const (
	OpPut         = iota // operation
	OpAppend             // operation
	OpGet                // operation
	OpReconfig           // 开始re-config
	OpShardInput         // 接收一个shard
	OpShardOutput        // 成功向正确的group传输一个shard
)

const (
	RPCCountDivisor = 1000000007
)

type RPCIdentification struct {
	ClerkID int64 // clerk id，比如ip + port
	RPCID   int   // rpc编号，合法编号从1开始
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       int // OpPut or OpAppend
	Identity RPCIdentification
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	Identity RPCIdentification
}

type GetReply struct {
	Err   Err
	Value string
}

type ReconfigArgs struct {
	Config shardctrler.Config
}

type ReconfigDoneArgs struct {
	ConfigNum int
}

type ShardInputArgs struct {
	ConfigNum int   // 当前config num
	Shard     Shard // data
	// 不需要正确性验证，下面两个可以优化掉
	Gid    int // 发送方的gid
	Server int // 发送方的server name
}

type ShardInputReply struct {
	Err Err
}

type ShardOutputArgs struct {
	ConfigNum int // config num，用于辨识是否已经过时
	ShardID   int
}

func (s *Shard) Copy() *Shard {
	r := Shard{
		ShardID:        s.ShardID,
		OK:             s.OK,
		KVMap:          make(map[string]string),
		LastAppliedMap: make(map[int64]OpResult),
	}
	for k, v := range s.KVMap {
		r.KVMap[k] = v
	}
	for k, v := range s.LastAppliedMap {
		r.LastAppliedMap[k] = v
	}
	return &r
}

func If(condition bool, trueVal interface{}, falseVal interface{}) interface{} {
	if condition {
		return trueVal
	}
	return falseVal
}

// 用给定参数args调用f，超过timeout直接返回
func CallFunc(timeout time.Duration, f interface{}, args ...interface{}) ([]interface{}, bool) {
	ch := make(chan bool)
	_f := reflect.ValueOf(f)
	var _r []reflect.Value
	_args := make([]reflect.Value, len(args))
	for i, v := range args {
		_args[i] = reflect.ValueOf(v)
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

func (s *Shard) String() string {
	var str string
	// str += fmt.Sprintf("%v", s.OK)
	str += "["
	for k := range s.KVMap {
		str += k + ", "
	}
	str += "]"
	return str
}

func (c Config) String() string {
	var str string
	str = "{"
	str += fmt.Sprintf("Trans:%v, ", c.Transition)
	str += fmt.Sprintf("Num:%v, ", c.Config.Num)
	str += fmt.Sprintf("ConfigShard:%v, ", c.Config.Shards)
	str += fmt.Sprintf("Inshard:%v, OutShard:%v, ", c.InShard, c.OutShard)
	str += "}"
	return str
}

func (op Op) String() string {
	var str string
	str = "{"
	switch op.Type {
	case OpPut:
		str += "Put, "
	case OpAppend:
		args := op.Args.(PutAppendArgs)
		str += "Append, " + fmt.Sprintf("key:%v, value:%v", args.Key, args.Value)
	case OpGet:
		str += "Get, "
	case OpReconfig:
		str += "Reconfig, "
	case OpShardInput:
		str += "ShardInput, "
	case OpShardOutput:
		str += "ShardOutput, "
	}
	// str += fmt.Sprintf("%v", op.Args)
	str += " }"
	return str
}
