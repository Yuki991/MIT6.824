package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	// TODO
	clerkID  int64 // clerkID，目前做法是赋予一个极大的随机数(调用nrand)，小概率出错
	rpcCount int   // rpc编号，从1开始
	// TODO lastLeader
}

func (ck *Clerk) GetNewRPCIdentification() RPCIdentification {
	ck.rpcCount = ck.rpcCount%RPCCountDivisor + 1
	return RPCIdentification{
		ClerkID: ck.clerkID,
		RPCID:   ck.rpcCount,
	}
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// TODO
	ck.clerkID = nrand()
	ck.rpcCount = 0
	return ck
}

func (ck *Clerk) sendGetRPC(server *labrpc.ClientEnd, args *GetArgs, reply *GetReply) {
	server.Call("ShardKV.GetHandler", args, reply)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		Identity: ck.GetNewRPCIdentification(),
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
		Loop:
			for si := 0; si < len(servers); si++ {
				var reply GetReply
				srv := ck.make_end(servers[si])

				if ok := CallFunc(200*time.Millisecond, ck.sendGetRPC, srv, &args, &reply); !ok {
					continue
				}

				switch reply.Err {
				case OK, ErrNoKey:
					return reply.Value
				case ErrWrongGroup:
					break Loop
				default:
					continue
				}
			}
		}
		// TODO
		time.Sleep(10 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

func (ck *Clerk) sendPutAppendRPC(server *labrpc.ClientEnd, args *PutAppendArgs, reply *PutAppendReply) {
	server.Call("ShardKV.PutAppendHandler", args, reply)
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op int) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Identity: ck.GetNewRPCIdentification(),
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
		Loop:
			for si := 0; si < len(servers); si++ {
				var reply PutAppendReply
				srv := ck.make_end(servers[si])

				if ok := CallFunc(200*time.Millisecond, ck.sendPutAppendRPC, srv, &args, &reply); !ok {
					continue
				}

				switch reply.Err {
				case OK:
					return
				case ErrWrongGroup:
					break Loop
				default:
					continue
				}
			}
		}
		// TODO
		time.Sleep(10 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
