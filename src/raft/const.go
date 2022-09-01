package raft

type ServerState int

const (
	Follower ServerState = iota + 0
	Candidate
	Leader
)
