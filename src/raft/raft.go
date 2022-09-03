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

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int         // term of log entry
	Command interface{} // Command
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
	currentTerm int        // latest term server has seen
	votedFor    int        // candidate index that received vote in current term
	log         []LogEntry // log entries

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	// TODO 暂时还没有处理这个last applied
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// TODO auxiliary variables if needed
	electionTimeout bool          // true means If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate
	serverState     ServerState   // indicate the current state of server
	applyCh         chan ApplyMsg // used to send message to tell the service there is a new committed command
	leaderId        int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.serverState == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	Debug(dVote, "S%d <- S%d, receive requestVote: %v", rf.me, args.CandidateId, args)
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	if lastLogIndex := len(rf.log) - 1; (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		(rf.log[lastLogIndex].Term < args.LastLogTerm ||
			(rf.log[lastLogIndex].Term == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)) {
		// vote for candidate
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
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
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for "leader" to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debug(dLeader, "S%d <- S%d, receive AppendEntries RPC: %v", rf.me, args.LeaderId, args)

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	// 用defer小心点，有可能会死锁的
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reiceived info from leader
	if args.Term > rf.currentTerm {
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
		if args.PrevLogIndex <= len(rf.log)-1 &&
			rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {

			// discover same index and same term, accept appendEntries RPC
			reply.Success = true

			// append logs, discard conflict logs
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		}
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := args.LeaderCommit
		if len(rf.log)-1 < newCommitIndex {
			newCommitIndex = len(rf.log) - 1
		}
		rf.commitIndex = newCommitIndex
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
	// Your code here (2B).

	Debug(dTerm, "S%d receive command, state:%v, term: %v, log length:%v, command:%v", rf.me, rf.serverState, rf.currentTerm, len(rf.log), command)

	// initialization
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.serverState == Leader)

	// append command to log if server believes it is the leader
	// TODO 需要判断这条command是否已经在log里，防止因为网络原因使得同一条log执行两次吗？
	if isLeader {
		rf.log = append(rf.log, LogEntry{term, command})
	}

	// TODO 非leader要不要把command转发给leader？

	return index, term, isLeader
}

// RPC request or response contains term T > currentTerm
// not lock here, check whether get lock before call this func
func (rf *Raft) convertToFollower(term int) {
	rf.serverState = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

// time out, conver follower to candidate and start election
func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	// increate currentTerm
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me
	// change server state to candidate
	rf.serverState = Candidate
	//
	// reset election timer，这个到底应该怎么做
	// ticker开goroutine执行convertToCandidate，不需要重置这个操作
	//
	var voteCnt int32 = 1
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	Debug(dVote, "S%d start election, currentTerm:%d", rf.me, rf.currentTerm)

	// TODO 这样写要等所有的request返回，但实际上只需要majority vote 就可以确定了
	// 现在是用一个goroutine实现的election，有没有可能同时有两个goroutine卡在这个位置，
	// 然后同时到下面的是否选举成功的判断的位置？ 有可能

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
		if voteCnt <= int32(len(rf.peers))/2 {
			done <- struct{}{}
		}
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			Debug(dTimer, "S%d <- S%d, requestVote request:%v", server, rf.me, requestVoteArgs)
			// 没仔细看labrpc里network的实现
			// 这里的sendRequestVote跟它描述的不太一致，如果target server宕机了，会被阻塞住
			// 而不是过一段时间会返回（也有可能是它设置的timeout太长了）
			// 总之如果用waitGroup等等所有的routine结束就会出问题（明明收到半数以上的票却不能结束）
			if rf.sendRequestVote(server, &requestVoteArgs, &reply) {
				Debug(dVote, "S%d <- S%d, requestVote reply:%v", rf.me, server, reply)

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
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

	Debug(dTimer, "S%d election voting finished, voteCnt:%d, state:%v", rf.me, voteCnt, rf.serverState)

	// 保证只有一个election goroutine能执行convertToLeader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voteCnt > int32(len(rf.peers))/2 && rf.serverState == Candidate {
		// TODO 有没有可能当选leader的同时收到了一个更大的term，然后被改成了follower
		rf.serverState = Leader
		go rf.convertToLeader()
	}
}

// candidate convert to leader, start leader work
func (rf *Raft) convertToLeader() {
	// Upon election: send initial empty AppendEntries RPCs(heartbeat)
	// to each server; repeat during idle periods to prevent election timeouts.
	// TODO If command received from client: append entry to log,
	// respond after entry applied to state machine
	// TODO If last log index >= nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	//		if successful: update nextIndex and matchIndex for follower
	//		if AppendEntries fails because of log inconsistency:
	//			decrement nextIndex and retry
	// TODO If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] >= N, and log[N].term == currentTerm:
	// set commitIndex = N

	Debug(dLeader, "S%d become leader", rf.me)

	// initialize nextIndex and matchIndex
	rf.mu.Lock()
	for i := range rf.peers {
		// initialized to leader last log index + 1
		rf.nextIndex[i] = len(rf.log)
		// initialized to 0
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	// 标识目前达成commit一致的index（参见raft figure2）
	// 同时也是heartbeat中发送的LeaderCommit(LeaderCommit不能直接发leader的commitIndex)
	N := 0

	// goroutine checks if last log index >= nextIndex for a follower
	// if not, send heartbeat periodically
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.sendHeartbeat(server, N)

			// send message to channel periodically
			go func() {
				for {
					// TODO 每隔一段时间发送心跳包，间隔设多少？
					time.Sleep(150 * time.Millisecond)
					if rf.serverState != Leader {
						break
					}

					// TODO 这个可能会出问题，比如一直阻塞但是一直在开goroutine，然后爆了
					// 不开goroutine就可能会导致之前的一直阻塞，发不了新的
					// 讲道理这个调用如果一段时间没有结束应该直接返回才对
					rf.sendHeartbeat(server, N)
				}
			}()

			for {
				// 每隔一段时间检查是否需要append entries
				time.Sleep(10 * time.Millisecond)
				if rf.serverState != Leader {
					break
				}

				rf.checkAppendEntries(server)
			}
		}(i)
	}

	for {
		// 每隔一段时间检查是否还担任leader
		time.Sleep(10 * time.Millisecond)

		if rf.serverState != Leader {
			break
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

		// Debug(dInfo, "S%d, term: %v, nextIndex:%v, matchIndex:%v, arr: %v, mIndex:%v",
		// rf.me, rf.currentTerm, rf.nextIndex, rf.matchIndex, arr, mIndex)

		rf.mu.Lock()
		if newCommitIndex := arr[mIndex]; newCommitIndex > N &&
			newCommitIndex > rf.commitIndex &&
			rf.log[newCommitIndex].Term == rf.currentTerm {
			N = newCommitIndex
			rf.commitIndex = newCommitIndex
		}
		rf.mu.Unlock()
	}
}

// leader send heartbeat to each server
func (rf *Raft) sendHeartbeat(server, LeaderCommit int) {
	go func() {
		rf.mu.Lock()
		// TODO heartbeat里prevlogindex和prevlogterm应该可以随便设，反正没用
		// 设了效率会高一点，更新nextIndex
		appendEntriesArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{},
			LeaderCommit: LeaderCommit,
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		Debug(dTimer, "S%d <- S%d, send Heartbeat: %v", server, rf.me, appendEntriesArgs)
		if rf.sendAppendEntries(server, &appendEntriesArgs, &reply) {
			rf.mu.Lock()
			Debug(dTimer, "S%d <- S%d, receive Heartbeat Response: %v", rf.me, server, reply)
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	}()
}

// check whether to send AppendEntries RPC
func (rf *Raft) checkAppendEntries(server int) {
	// TOOD 这样写可能会导致阻塞大量goroutine
	go func() {
		for {
			rf.mu.Lock()
			// no need to append entries or isn't leader, break
			if len(rf.log)-1 < rf.nextIndex[server] || rf.serverState != Leader {
				rf.mu.Unlock()
				break
			}

			Debug(dLeader, "S%d -> S%d, try to append entries", rf.me, server)

			// TODO 一次性发送多个？现在暂时是一次发送一个
			appendEntriesArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
				Entries:      []LogEntry{rf.log[rf.nextIndex[server]]},
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			// TODO 有没有可能会阻塞在这的？
			Debug(dLeader, "S%d <- S%d, send AppendEntries RPC: %v", server, rf.me, appendEntriesArgs)
			if !rf.sendAppendEntries(server, &appendEntriesArgs, &reply) {
				// no response, break
				break
			}

			// update nextIndex and matchIndex
			rf.mu.Lock()
			// TODO 注意，现在是一次append一个，一次append多个的话这里也要做相应修改
			if reply.Success {
				rf.matchIndex[server] = rf.nextIndex[server]
				rf.nextIndex[server]++
			} else {
				rf.nextIndex[server]--
			}
			Debug(dLeader, "S%d <- S%d, receive AppendEntries response: %v, nextIndex:%v",
				rf.me, server, reply, rf.nextIndex)

			// discover new leader
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	}()
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
	Debug(dWarn, "S%d is killed.", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(time.Now().Unix())

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// TODO How long should the time delay be set
		// TODO 这种写法会允许存在两个heartbeat之间间隔 > timeout（一个靠前一个靠后）
		// 有没有办法写成定时器，并且支持重置
		rf.mu.Lock()
		// reset timeout flag
		rf.electionTimeout = true
		rf.mu.Unlock()

		sleepTime := rand.Intn(200) + 200
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		Debug(dInfo, "S%d ticker, killed:%v, state: %v, term: %v, log: %v, commitIndex:%v",
			rf.me, rf.killed(), rf.serverState, rf.currentTerm, rf.log, rf.commitIndex)
		if _, isLeader := rf.GetState(); !isLeader && rf.electionTimeout {
			go rf.convertToCandidate()
		}
	}
}

func (rf *Raft) updateLastApplied() {
	for rf.killed() == false {
		time.Sleep(20 * time.Millisecond)

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			Debug(dError, "S%d apply log[%d]:%v", rf.me, rf.lastApplied, rf.log[rf.lastApplied])
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg
			Debug(dError, "S%d apply log[%d]:%v Succeed.", rf.me, rf.lastApplied, rf.log[rf.lastApplied])
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

	// TODO Your initialization code here (2A, 2B, 2C).
	// check whether all variables are initialized
	rf.serverState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0

	// 开一个定时器检查是否有需要apply到service的log entry(lastApplied < commitIndex)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
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
