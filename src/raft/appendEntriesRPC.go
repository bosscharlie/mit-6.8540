package raft

import "time"

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term 		 int 		// leader's term
	LeaderId 	 int 		// so follower can redirect clients's Request
	PrevLogIndex int
	PrevLogTerm  int 		// Log Matching
	Entries 	 []LogEntry // log entries to store, empty for heartbeat, may send more tham one for efiiciency
	LeaderCommit int 		// leader's commitIndex, means which command can be applied
}

// AppendEntries RPC arguments structure.
type AppendEntriesReply struct {
	Term 	int 	// currentTerm, for leader to update itself. If a leader carshed and recover, send heartbeat to others, notice that it is stale
	Success bool 	// true if follower contained every prevlogindex and prevlogterm, used to increase the nextIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply * AppendEntriesReply) {
	// Debug(dTimer,"S%d received heartbeat from S%d with term%d in term%d",rf.me,args.LeaderId,args.Term,rf.currentTerm)
	// stale leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if the RPC is not heartbeat, do log replication
	if len(args.Entries) > 0 {
		// TODO: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        // args.PrevLogIndex==0 means accept the fisrt entry of leader
		if args.PrevLogIndex<len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
		// TODO: append new entry
		// If an existing entry conflicts with a new one (same index but different terms), 
		// delete the existing entry and all thatfollow it
        if args.PrevLogIndex >= 0 {
            rf.log = rf.log[:args.PrevLogIndex]
        }
        // add new entry to local log
        rf.log = append(rf.log, args.Entries...)
        // rf.lastLogTerm = rf.log[len(rf.log)-1].Term
        // rf.lastLogIndex = len(rf.log)-1
	}

	// convert to follower
	rf.currentTerm = args.Term
	rf.heartbeatReceived = true
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.isLeader = false
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
        // wake up the commit apply go routine
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int) bool {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
    prevIndex := rf.nextIndex[server]-1
	args.PrevLogIndex = prevIndex
	args.PrevLogTerm = rf.log[prevIndex].Term
    // args.PrevLogTerm = -1
	args.Entries = rf.log[args.PrevLogIndex+1:len(rf.log)]
	args.LeaderCommit = rf.commitIndex
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok && rf.isLeader {
        // meet a new server, transfer to follower
		if reply.Term > rf.currentTerm {
            rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.isLeader = false
			rf.votedFor = -1
            rf.mu.Unlock()
            return ok
		}
        // if success, count and commit
        if reply.Success {
            logIndex := args.PrevLogIndex+1
            if logIndex<len(rf.log) && rf.log[logIndex].Term == rf.currentTerm {
                rf.logAcceptCnt[logIndex]++
                if rf.logAcceptCnt[logIndex]*2 > len(rf.peers) {
                    rf.commitIndex = logIndex
                }
            }
        } else{
            // if !success, decrease lastIndex
            rf.mu.Lock()
            rf.nextIndex[server]--
            rf.mu.Unlock()
        }
	}
	return ok
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false && rf.isLeader{
		for id,_ := range(rf.peers) {
			if id != rf.me {
				go func(peerid int) {
					rf.sendAppendEntries(peerid)
				}(id)
			}
		}
		time.Sleep(time.Duration(rf.heartbeatDuration) * time.Millisecond)
	}
}