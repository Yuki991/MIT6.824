package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	CurrentTerm int // latest term server has seen
	VotedFor    int // candidate index that received vote in current term
	Log         Log // log entries

	// volatile state on all servers
	CommitIndex int // index of highest log entry known to be committed
	LastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// auxiliary variables if desired
	applyCh           chan ApplyMsg // channel which is used to send message to service
	electionTimeout   bool          // true means If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate
	serverState       ServerState   // indicate the current state of server
	lastNewEntryIndex int           // index of last received from the leader of currentTerm
	snapshot          []byte        // 这个可以不存，如果installSnapshot需要，可以从stable storage里读取。先存着吧，方便
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = (rf.serverState == Leader)
	return term, isleader
}

// save raft state and snapshot to stable storage
func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	rfState := w.Bytes()
	rf.persister.SaveStateAndSnapshot(rfState, snapshot)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// 需要在所有修改了需要持久化的state的地方加上persist调用
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	// e.Encode(rf.LastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		rf.Log.LastIncludedIndex = 0
		rf.Log.LastIncludedTerm = 0
		rf.Log.Log = []LogEntry{}
		rf.snapshot = []byte{}
		rf.CommitIndex = 0
		rf.LastApplied = 0
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// TODO Error了要做何处理？
		//Debug(dError, "S%d read persist error.", rf.me)
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		rf.Log.LastIncludedIndex = 0
		rf.Log.LastIncludedTerm = 0
		rf.Log.Log = []LogEntry{}
		rf.snapshot = []byte{}
		rf.CommitIndex = 0
		rf.LastApplied = 0
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.snapshot = rf.persister.ReadSnapshot()
		// 重启时commitIndex和lastApplied设置为lastIncludedIndex
		rf.CommitIndex = rf.Log.LastIncludedIndex
		rf.LastApplied = rf.Log.LastIncludedIndex
		// TODO 将snapshot发送给service，需要吗，还是说service自己去读？
		// go func() {
		// }()

		// lastApplied不应该被持久化
		// 不需要，如果application没挂，他应该知道哪些command是执行过的
		// 如果application挂了，它就需要log replay
		// 还有一种方式是application告诉raft有哪些entries是已经aplied的
		// rf.LastApplied = lastApplied
		// rf.CommitIndex = rf.LastApplied
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// TODO Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dError, "S%d discard log entries before index %d, log:%v", rf.me, index, rf.Log)
	rf.Log.Discard(index)
	rf.snapshot = snapshot
	rf.saveStateAndSnapshot(snapshot)
	// Debug(dError, "S%d finish discard log entries before index %d, log:%v", rf.me, index, rf.Log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for "candidate" to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//Debug(dVote, "S%d <- S%d, receive requestVote: %v", rf.me, args.CandidateId, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term)
	}
	if lastLogIndex := rf.Log.GetLastEntryIndex(); (rf.VotedFor < 0 || rf.VotedFor == args.CandidateId) &&
		(rf.Log.GetTerm(lastLogIndex) < args.LastLogTerm ||
			(rf.Log.GetTerm(lastLogIndex) == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)) {
		// vote for candidate
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		// TODO 这样写的话前面convertToFollower会persist一次，这里又persist一次，好蠢呀
		rf.persist()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	LeaderCommit int        // leader's commitIndex
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for "leader" to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// 为了让leader快速定位follower的log entries从哪里开始缺了，额外加上的一个字段
	// 按照论文里说的应该是不需要的，实际情况中不会出现server经常挂掉以及log里出现大量的不一致
	PossibleNextIndex int // follower向leader返回可能的nextIndex，-1表示follower没有requestArgs中的index对应的term的数据
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Debug(dLeader, "S%d <- S%d, receive AppendEntries RPC: %v", rf.me, args.LeaderId, args)

	// 用defer小心点，有可能会死锁的
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		return
	}

	// reiceived info from leader
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term)
	}
	// change server state to follower because received info from new leader
	// 这个别删，有可能当前term是candidate，然后discover一个当前term的leader
	rf.serverState = Follower

	// update election timeout flag
	rf.electionTimeout = false

	// append log
	if len(args.Entries) > 0 {
		// 有没有可能出现相同index，相同term，但是内容不同？
		// 不可能，Log Matching Property保证了这一点
		// 通俗的理解就是，这个term都是由同一个leader发的，不可能不同
		if args.PrevLogIndex <= rf.Log.GetLastEntryIndex() &&
			args.PrevLogIndex >= rf.Log.LastIncludedIndex &&
			rf.Log.GetTerm(args.PrevLogIndex) == args.PrevLogTerm {

			// discover same index and same term, accept appendEntries RPC
			reply.Success = true

			// 加了一条lastNewEntryIndex <= args.PrevLogIndex
			// 能够保在接受相同index的appendEntries RPC请求的时候
			// 不会覆盖别的请求的结果
			if rf.lastNewEntryIndex < args.PrevLogIndex+len(args.Entries) {
				rf.Log.Append(args.PrevLogIndex, args.Entries)

				// Debug(dError, "S%d change log, logLength:%v, lastNewEntryIndex:%v, prevLogIndex:%v, entriesLength:%v, log:%v, args:%v", rf.me, rf.Log.GetLastEntryIndex(), rf.lastNewEntryIndex, args.PrevLogIndex, len(args.Entries), rf.Log, args)

				// update last new entry index
				rf.lastNewEntryIndex = args.PrevLogIndex + len(args.Entries)

				// 持久化，rf.log发生了变化
				rf.persist()
			}
		}
	}

	// update commitIndex
	if args.LeaderCommit > rf.CommitIndex {
		newCommitIndex := args.LeaderCommit
		if rf.lastNewEntryIndex < newCommitIndex {
			newCommitIndex = rf.lastNewEntryIndex
		}
		rf.CommitIndex = newCommitIndex
	}

	if len(args.Entries) > 0 {
		// 加快nextIndex收敛，heartbeat不处理
		// TODO 这一段可能有些问题
		if args.PrevLogIndex > rf.Log.GetLastEntryIndex() {
			// nextIndex至多为last log entry index + 1
			reply.PossibleNextIndex = rf.Log.GetLastEntryIndex() + 1
		} else if args.PrevLogIndex < rf.Log.LastIncludedIndex {
			// 这个应该不可能，应该是过期数据（除去hearbeat...）
			// Debug(dError, "S%d receive appendRPC:%v, log:%v", rf.me, args, rf.Log)
			// panic("error")
			reply.PossibleNextIndex = rf.Log.GetLastEntryIndex() + 1
		} else if rf.Log.GetTerm(args.PrevLogIndex) > args.PrevLogTerm {
			// 如果follower的在某个index上的term大于leader对应位置上的term
			// 说明follower这个term的所有数据肯定都可以删了（这个term上的数据肯定是不一致的）
			reply.PossibleNextIndex = rf.Log.GetLessAndEqualTermIndex(args.PrevLogIndex, args.PrevLogTerm)
			if reply.PossibleNextIndex <= rf.Log.LastIncludedIndex {
				reply.PossibleNextIndex = rf.Log.LastIncludedIndex + 1
			}
		} else if rf.Log.GetTerm(args.PrevLogIndex) < args.PrevLogTerm {
			// 如果follower的在某个index上的term小于leader对应位置上的term
			// 说明leader在这个term上的数据follower都没有
			// 返回-1表示follower没有PrevLogTerm这个term的数据
			reply.PossibleNextIndex = -1
		} else {
			// prevLogIndex上的数据也没有
			reply.PossibleNextIndex = args.PrevLogIndex
		}
	}
}

// send AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//Debug(dTerm, "S%d receive command, state:%v, term: %v, log length:%v, command:%v", rf.me, rf.serverState, rf.CurrentTerm, len(rf.Log), command)

	// initialization
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.Log.GetLastEntryIndex() + 1
	term := rf.CurrentTerm
	isLeader := (rf.serverState == Leader)

	// append command to log if server believes it is the leader
	if isLeader && !rf.killed() {
		// Debug(dLeader, "S%d receive command", rf.me)
		rf.Log.AppendSingle(LogEntry{term, command})
		rf.persist()
	}

	// TODO 非leader要不要把command转发给leader？

	return index, term, isLeader
}

// RPC request or response contains term T > currentTerm
// change state, and persist to stable storage
// not lock here, check whether get lock before call this func
func (rf *Raft) convertToFollower(term int) {
	rf.serverState = Follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.lastNewEntryIndex = 0
	rf.persist()
}

// time out, conver follower to candidate and start election
func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	// increate currentTerm
	rf.CurrentTerm++
	// vote for self
	rf.VotedFor = rf.me
	// change server state to candidate
	rf.serverState = Candidate
	// persist
	rf.persist()
	//
	// reset election timer，这个到底应该怎么做
	// ticker开goroutine执行convertToCandidate，不需要重置这个操作
	//
	var voteCnt int32 = 1
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.Log.GetLastEntryIndex(),
		LastLogTerm:  rf.Log.GetTerm(rf.Log.GetLastEntryIndex()),
	}
	rf.mu.Unlock()

	//Debug(dVote, "S%d start election, currentTerm:%d", rf.me, rf.CurrentTerm)

	// true : 收到一个reply，并且已有半数以上赞同；false: 收到一个reply
	replyCh := make(chan bool)
	// 标识election是否结束
	done := make(chan struct{})
	// 开一个gorountine用于统计有多少个reply，并且如果已经收到半数以上赞同就可以直接往后走
	go func() {
		reqNum := len(rf.peers) - 1
		for i := 0; i < reqNum; i++ {
			if <-replyCh {
				done <- struct{}{}
			}
		}
		if atomic.LoadInt32(&voteCnt) <= int32(len(rf.peers))/2 {
			done <- struct{}{}
		}
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			//Debug(dTimer, "S%d <- S%d, requestVote request:%v", server, rf.me, requestVoteArgs)
			// 没仔细看labrpc里network的实现
			// 这里的sendRequestVote跟它描述的不太一致，如果target server宕机了，会被阻塞住
			// 而不是过一段时间会返回（也有可能是它设置的timeout太长了）
			// 总之如果用waitGroup等等所有的routine结束就会出问题（明明收到半数以上的票却不能结束）
			// Debug(dInfo, "S%d -> S%d send request vote", rf.me, server)
			if rf.sendRequestVote(server, &requestVoteArgs, &reply) {
				//Debug(dVote, "S%d <- S%d, requestVote reply:%v", rf.me, server, reply)

				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.convertToFollower(reply.Term)
				}
				rf.mu.Unlock()

				if reply.VoteGranted && atomic.AddInt32(&voteCnt, 1) > int32(len(rf.peers))/2 {
					replyCh <- true
				} else {
					replyCh <- false
				}
			}
		}(i)
	}
	// wait for election finished
	<-done

	//Debug(dTimer, "S%d election voting finished, voteCnt:%d, state:%v", rf.me, voteCnt, rf.serverState)

	// 保证只有一个election goroutine能执行convertToLeader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if atomic.LoadInt32(&voteCnt) > int32(len(rf.peers))/2 && rf.serverState == Candidate {
		// TODO 有没有可能当选leader的同时收到了一个更大的term，然后被改成了follower
		rf.serverState = Leader
		go rf.convertToLeader()
	}
}

// candidate convert to leader, start leader work
func (rf *Raft) convertToLeader() {
	//Debug(dLeader, "S%d become leader", rf.me)

	// initialize nextIndex and matchIndex
	rf.mu.Lock()
	for i := range rf.peers {
		// initialized to leader last log index + 1
		rf.nextIndex[i] = rf.Log.GetLastEntryIndex() + 1
		// initialized to 0
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	// goroutine checks if last log index >= nextIndex for a follower
	// if not, send heartbeat periodically
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// 定时发送heartbeat
		go func(server int) {
			// 这句是有问题的，刚成为leader准备发第一个heartbeat，然后对方挂了
			// 结果就会卡死在这里
			// rf.sendHeartbeat(server)

			// TODO 限制goroutine个数
			ch := make(chan struct{}, 40)
			for {
				// TODO 每隔一段时间发送心跳包，间隔设多少？
				rf.mu.Lock()
				if rf.serverState != Leader || rf.killed() {
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Unlock()
				}

				// TODO 这个可能会出问题，比如一直阻塞但是一直在开goroutine，然后爆了
				// 不开goroutine就可能会导致之前的一直阻塞，发不了新的
				// 讲道理这个调用如果一段时间没有结束应该直接返回才对
				ch <- struct{}{}
				go func() {
					rf.sendHeartbeat(server)
					<-ch
				}()

				time.Sleep(150 * time.Millisecond)
			}
		}(i)

		// 定时检查是否需要发送AppendEntriesRPC给followers
		go func(server int) {
			// 限制goroutine个数
			ch := make(chan struct{}, 10)
			for {
				// 每隔一段时间检查是否需要append entries
				time.Sleep(20 * time.Millisecond)
				rf.mu.Lock()
				if rf.serverState != Leader || rf.killed() {
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Unlock()
				}

				ch <- struct{}{}
				go func() {
					rf.checkAppendEntries(server)
					<-ch
				}()
			}
		}(i)
	}

	for {
		// 每隔一段时间检查是否还担任leader
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.serverState != Leader || rf.killed() {
			rf.mu.Unlock()
			break
		} else {
			rf.mu.Unlock()
		}

		// 检查是否需要更新commitIndex
		rf.mu.Lock()
		var arr []int = make([]int, len(rf.matchIndex))
		mIndex := len(rf.peers)/2 - 1
		copy(arr, rf.matchIndex)
		rf.mu.Unlock()

		sort.Slice(arr, func(i, j int) bool {
			return arr[i] > arr[j]
		})

		// Debug(dInfo, "S%d, ads:%v, state:%v, logLength:%v term: %v, nextIndex:%v, matchIndex:%v, arr: %v, mIndex:%v, log:%v",
		// rf.me, &rf, rf.serverState, rf.Log.GetLastEntryIndex(), rf.CurrentTerm, rf.nextIndex, rf.matchIndex, arr, mIndex, rf.Log)

		rf.mu.Lock()
		if newCommitIndex := arr[mIndex]; newCommitIndex > rf.CommitIndex &&
			rf.Log.GetTerm(newCommitIndex) == rf.CurrentTerm {
			rf.CommitIndex = newCommitIndex
		}
		rf.mu.Unlock()
	}
}

// leader send heartbeat to each server
func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: rf.CommitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	// Debug(dTimer, "S%d <- S%d, send Heartbeat: %v", server, rf.me, args)
	// Debug(dError, "S%d -> S%d send heartbeat", rf.me, server)
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		//Debug(dTimer, "S%d <- S%d, receive Heartbeat Response: %v", rf.me, server, reply)
		if reply.Term > rf.CurrentTerm {
			rf.convertToFollower(reply.Term)
		}
		rf.mu.Unlock()
	}
}

// check whether to send AppendEntries RPC
func (rf *Raft) checkAppendEntries(server int) {
	// TODO 这样写可能会导致阻塞大量goroutine
	// 这样写还有个问题
	// 比如有K个routine尝试在index=3之后加东西，如果成功的话，全部都会成功...
	// 然后就导致nextIndex这些都出错了
	///////////////////////////////////////////////////////////////

	// 标识上一个AppendEntries成功
	// accpetFlag := false
	for {
		rf.mu.Lock()
		// no need to append entries or isn't leader, break
		if rf.Log.GetLastEntryIndex() < rf.nextIndex[server] ||
			rf.serverState != Leader ||
			rf.killed() {
			rf.mu.Unlock()
			break
		}

		/////////////////////////////////////////////////////////////////////////
		//                     send InstallSnapshot RPC                       //
		///////////////////////////////////////////////////////////////////////

		if rf.nextIndex[server] <= rf.Log.LastIncludedIndex {
			// 需要发送的entry已经被discard掉了，这时候转变为给follower发送InstallSnapshot RPC
			args := InstallSnapshotArgs{
				Term:              rf.CurrentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.Log.LastIncludedIndex,
				LastIncludedTerm:  rf.Log.LastIncludedTerm,
				Snapshot:          rf.snapshot,
			}
			rf.mu.Unlock()

			reply := InstallSnapshotReply{}
			// Debug(dWarn, "S%d -> S%d install snapshot, args:%v", rf.me, server, args)
			if !rf.sendInstallSnapshop(server, &args, &reply) {
				break
			}

			rf.mu.Lock()
			if reply.Term > rf.CurrentTerm {
				rf.convertToFollower(reply.Term)
			}
			// TODO install snapshot之后重设nextIndex和matchIndex
			// 这个在多线程下会有点问题，刚重设完就被其他线程改了
			rf.nextIndex[server] = rf.Log.LastIncludedIndex + 1
			rf.matchIndex[server] = rf.Log.LastIncludedIndex
			rf.mu.Unlock()
			break
		}

		/////////////////////////////////////////////////////////////////////////
		//                      send AppendEntries RPC                        //
		///////////////////////////////////////////////////////////////////////

		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.Log.GetTerm(rf.nextIndex[server] - 1),
			Entries:      []LogEntry{},
			LeaderCommit: rf.CommitIndex,
		}

		// TODO 目前暂时删除了accpetFlag，作用不是很大
		// if accpetFlag {
		// 上一个AppendEntreis成功，尝试将log中prevlogindex后面的所有entries都发送出去
		args.Entries = make([]LogEntry, rf.Log.GetLastEntryIndex()-args.PrevLogIndex)
		copy(args.Entries, rf.Log.GetSlice(args.PrevLogIndex+1, rf.Log.GetLastEntryIndex()+1))
		// Debug(dError, "S%d prepare append entries, L:%v, R:%v, args:%v, log:%v", rf.me, args.PrevLogIndex+1, rf.Log.GetLastEntryIndex()+1, args, rf.Log)
		// } else {
		// 	// 只发送一个log entries
		// 	args.Entries = make([]LogEntry, 1)
		// 	args.Entries[0] = rf.Log.Get(args.PrevLogIndex + 1)
		// }

		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		// 有没有可能会阻塞在这的？
		//Debug(dLeader, "S%d <- S%d, send AppendEntries RPC: %v", server, rf.me, args)
		// Debug(dWarn, "S%d -> S%d append entries, args:%v", rf.me, server, args)
		if !rf.sendAppendEntries(server, &args, &reply) {
			// no response, break
			break
		}

		rf.mu.Lock()
		// discover new leader
		if reply.Term > rf.CurrentTerm {
			rf.convertToFollower(reply.Term)
		}

		if reply.Success {
			if args.PrevLogIndex+len(args.Entries)+1 > rf.nextIndex[server] {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				// accpetFlag = true
			}
		} else {
			// TODO snapshot被接受之后，重新将nextIndex调成最大，这一句可能导致出问题
			// if args.PrevLogIndex+1 == rf.nextIndex[server] {
			// 改成下面这种写法，因为matchIndex必定是单调不下降的，所以不会出问题
			// prevLogIndex <= matchIndex说明这个线程的进度肯定是落后最新的线程的
			if args.PrevLogIndex > rf.matchIndex[server] {
				// rf.nextIndex[server]--

				// follower返回PossibleNextIndex告诉leader可能的nextIndex，加快收敛
				if reply.PossibleNextIndex == -1 {
					// follower没有这PrevLogTerm这个term的数据
					// 找到第一个index对应的term不等于PrevLogTerm
					prevTermIndex := rf.Log.GetPrevTermIndex(args.PrevLogIndex, args.PrevLogTerm)
					// prevTermIndex返回结果<0，表示想要发送的已经被discard，这时候在for的下一轮发送InstallSnapshot RPC
					rf.nextIndex[server] = prevTermIndex + 1
				} else {
					rf.nextIndex[server] = reply.PossibleNextIndex
				}
				// accpetFlag = false
			} else {
				// 别的routine已经成功返回，直接退出，lock释放
				rf.mu.Unlock()
				break
			}
		}
		//Debug(dLeader, "S%d <- S%d, receive AppendEntries response: %v, nextIndex:%v",
		// rf.me, server, reply, rf.nextIndex)
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	// 注意实现中不打算使用分段发送snapshot，data就是整个snapshot
	// 所以不需要offset和done字段
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Snapshot          []byte // raw bytes of the snapshot chunk, starting at offset
	// Offset            int    // byte offset where chunk is positioned in the snapshot file
	// Done              bool   // true is this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// install snapshot handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.CurrentTerm {
		// change currentTerm and persist
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	// Debug(dWarn, "S%d <- S%d receive install snapshot, args:%v, log:%v", rf.me, args.LeaderId, args, rf.Log)

	if args.LastIncludedIndex <= rf.Log.LastIncludedIndex {
		// args.snapshot比目前版本还旧，直接return
		return
	}
	if args.LastIncludedIndex <= rf.Log.GetLastEntryIndex() &&
		rf.Log.GetTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Figure 13. 6.
		// 此时follower缺的log entries肯定是leader有的，不需要install snapshot
		return
	}

	// install snapshot
	rf.snapshot = args.Snapshot
	// 清空log
	rf.Log.DiscardAll(args.LastIncludedIndex, args.LastIncludedTerm)
	// commitIndex和appliedIndex都调整成lastIncludedIndex
	rf.CommitIndex = args.LastIncludedIndex
	rf.LastApplied = args.LastIncludedIndex
	// lastNewEntryIndex清0
	rf.lastNewEntryIndex = 0

	// TODO snapshot发送给application
	// 注意这时候锁还没有被释放，要是阻塞了会导致整个程序卡死在这了，要不要改成用goroutine？
	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		// Debug(dWarn, "S%d send snapshot to applyCh, args:%v", rf.me, applyMsg)
		rf.applyCh <- applyMsg
		// Debug(dWarn, "S%d finish send snapshot to applyCh, args:%v", rf.me, applyMsg)
	}()
	rf.saveStateAndSnapshot(args.Snapshot)
}

func (rf *Raft) sendInstallSnapshop(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//Debug(dWarn, "S%d is killed.", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(time.Now().Unix())

	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 有没有办法写成定时器，并且支持重置
		rf.mu.Lock()
		// reset timeout flag
		rf.electionTimeout = true
		rf.mu.Unlock()

		sleepTime := rand.Intn(200) + 200
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		// Debug(dInfo, "S%d ticker, ads:%v, killed:%v, state: %v, term: %v, logLength: %v, commitIndex:%v, lastApplied:%v, lastNewEntryIndex:%v, nextIndex:%v",
		// rf.me, &rf, rf.killed(), rf.serverState, rf.CurrentTerm, rf.Log.GetLastEntryIndex(), rf.CommitIndex, rf.LastApplied, rf.lastNewEntryIndex, rf.nextIndex)

		_, isLeader := rf.GetState()
		rf.mu.Lock()
		isTimeout := rf.electionTimeout
		rf.mu.Unlock()
		if !isLeader && isTimeout {
			go rf.convertToCandidate()
		}
	}
}

func (rf *Raft) updateLastApplied() {
	for !rf.killed() {
		time.Sleep(500 * time.Microsecond)

		for {
			rf.mu.Lock()
			if rf.LastApplied >= rf.CommitIndex {
				rf.mu.Unlock()
				break
			}

			rf.LastApplied++
			// Debug(dError, "S%d apply log[%d]:%v, lastNewEntryIndex:%v, commitIndex:%v, logLength:%v",
			// rf.me, rf.LastApplied, rf.Log.Get(rf.LastApplied), rf.lastNewEntryIndex, rf.CommitIndex, rf.Log.GetLastEntryIndex())
			applyMsg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.Log.Get(rf.LastApplied).Command,
				CommandIndex:  rf.LastApplied,
				CommandTerm:   rf.Log.GetTerm(rf.LastApplied),
				SnapshotValid: false,
			}
			rf.mu.Unlock()

			rf.applyCh <- applyMsg
			// //Debug(dError, "S%d apply log[%d]:%v Succeed.", rf.me, rf.LastApplied, rf.Log[rf.LastApplied])
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// check whether all variables are initialized
	rf.serverState = Follower
	rf.lastNewEntryIndex = 0

	// 开一个定时器检查是否有需要apply到service的log entry(lastApplied < commitIndex)
	// 在有了snapshot之后，commitIndex和lastApplied在重启时应该设成lastIncludedIndex
	// rf.CommitIndex = 0
	// rf.LastApplied = 0
	go rf.updateLastApplied()

	// TODO 如果server数目会改变，这样写就有问题了
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
