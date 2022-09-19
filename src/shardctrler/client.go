package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"reflect"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	clerkID    int64
	rpcCount   int
	lastLeader int
}

func CallFunc(timeout time.Duration, f interface{}, args ...interface{}) bool {
	ch := make(chan bool)
	_f := reflect.ValueOf(f)
	_args := make([]reflect.Value, len(args))
	for i, v := range args {
		_args[i] = reflect.ValueOf(v)
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
		<-ch
	}()
	return ok
}

func (ck *Clerk) GetNewRPCIdentification() RPCIdentification {
	ck.rpcCount = ck.rpcCount%CountDivisor + 1
	return RPCIdentification{
		ClerkID: ck.clerkID,
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

	// TODO 随机数作为id，小概率出问题
	ck.clerkID = nrand()
	ck.rpcCount = 0
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) sendQueryRPC(server int, args *QueryArgs, reply *QueryReply) bool {
	ok := ck.servers[server].Call("ShardCtrler.Query", args, reply)
	return ok
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{
		Num:      num,
		Identity: ck.GetNewRPCIdentification(),
	}

	for {
		// try each known server.
		for i := range ck.servers {
			var reply QueryReply
			server := (i + ck.lastLeader) % len(ck.servers)

			if ok := CallFunc(300*time.Millisecond, ck.sendQueryRPC, server, &args, &reply); !ok {
				continue
			}

			switch reply.Err {
			case OK:
				ck.lastLeader = server
				Debug(dError, "C%v query, args:%v, reply:%v", ck.clerkID, args, reply)
				return reply.Config
			case ErrWrongLeader:
				continue
			default:
				continue
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) sendJoinRPC(server int, args *JoinArgs, reply *JoinReply) bool {
	ok := ck.servers[server].Call("ShardCtrler.Join", args, reply)
	return ok
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{
		Servers:  servers,
		Identity: ck.GetNewRPCIdentification(),
	}

	for {
		// try each known server.
		for i := range ck.servers {
			var reply JoinReply
			server := (i + ck.lastLeader) % len(ck.servers)

			if ok := CallFunc(300*time.Millisecond, ck.sendJoinRPC, server, &args, &reply); !ok {
				continue
			}

			switch reply.Err {
			case OK:
				ck.lastLeader = server
				return
			case ErrWrongLeader:
				continue
			default:
				continue
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) sendLeaveRPC(server int, args *LeaveArgs, reply *LeaveReply) bool {
	ok := ck.servers[server].Call("ShardCtrler.Leave", args, reply)
	return ok
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{
		GIDs:     gids,
		Identity: ck.GetNewRPCIdentification(),
	}

	for {
		// try each known server.
		for i := range ck.servers {
			var reply LeaveReply
			server := (i + ck.lastLeader) % len(ck.servers)

			if ok := CallFunc(300*time.Millisecond, ck.sendLeaveRPC, server, &args, &reply); !ok {
				continue
			}

			switch reply.Err {
			case OK:
				ck.lastLeader = server
				return
			case ErrWrongLeader:
				continue
			default:
				continue
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) sendMoveRPC(server int, args *MoveArgs, reply *MoveReply) bool {
	ok := ck.servers[server].Call("ShardCtrler.Move", args, reply)
	return ok
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		Identity: ck.GetNewRPCIdentification(),
	}

	for {
		// try each known server.
		for i := range ck.servers {
			var reply MoveReply
			server := (i + ck.lastLeader) % len(ck.servers)

			if ok := CallFunc(300*time.Millisecond, ck.sendMoveRPC, server, &args, &reply); !ok {
				continue
			}

			switch reply.Err {
			case OK:
				ck.lastLeader = server
				return
			case ErrWrongLeader:
				continue
			default:
				continue
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}
