package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutOfDate   = "ErrOutOfDate"
)

const (
	OpJoin  = 0
	OpLeave = 1
	OpMove  = 2
	OpQuery = 3
)

const (
	CountDivisor = 1000000007
)

type RPCIdentification struct {
	ClerkID int64 // clerk id，比如ip + port
	RPCID   int   // rpc编号，合法编号从1开始
}

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	Identity RPCIdentification
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	Identity RPCIdentification
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	Identity RPCIdentification
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	Identity RPCIdentification
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (c *Config) copy() Config {
	r := Config{
		Num:    c.Num,
		Groups: make(map[int][]string),
	}
	for i := range c.Shards {
		r.Shards[i] = c.Shards[i]
	}
	for k, v := range c.Groups {
		r.Groups[k] = v
	}
	return r
}
