package raft

import "time"
import "math/rand"

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// send from candate to follower
	// lab 2A
	Term 		 int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry, use to promise new leader contains all committed log entries in previous term
	LastLogTerm  int // term if candidate's last log entry
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	// return from follower to candidate
	// lab 2A
	Term 		int // current Term, for candidate to update itself, if there is a latest term, candidate update its term
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// lab 2A
	// for all servers
	rf.mu.Lock()
	Debug(dVote, "S%d received requestvote from S%d in term%d",rf.me,args.CandidateId,args.Term)
	defer rf.mu.Unlock()
	// reject the stale vote request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.isLeader = false
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		Debug(dVote, "S%d voted for S%d in term%d",rf.me,args.CandidateId,rf.currentTerm)
		return
	}

	// for follower
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		if args.LastLogTerm >= rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex {
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			Debug(dVote, "S%d voted for S%d in term%d",rf.me,args.CandidateId,rf.currentTerm)
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

}

func (rf *Raft) sendRequestVote(server int) bool {
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogTerm, rf.lastLogIndex}
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.voteNum++
			// win the election
			if rf.voteNum*2 > len(rf.peers) {
				Debug(dLeader,"S%d win the election in term%d",rf.me,rf.currentTerm)
				rf.isLeader = true
				go rf.heartbeat()
			}
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.electionTimeout = 150 + (rand.Int63() % 150)
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.currentTerm++
	Debug(dTerm, "S%d StartNewElection in term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for id, _  := range(rf.peers) {
		if id!=rf.me {
			go func(peerid int) {
				rf.sendRequestVote(peerid)
			}(id)
		}
	}
}

// tikcer for election timeout checking
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		if !( rf.heartbeatReceived || rf.isLeader ){ // election timeout
			go rf.startNewElection()
		}

		rf.heartbeatReceived = false // reset heartbeat checking
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
	}
}