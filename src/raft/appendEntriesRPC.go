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
	// stale leader rejoin, with a high term because of retry elect new leader, but have a stale commitIndex
    // Debug(dInfo, "S%d receive rpc from S%d with %dlogs ans prevIndex %d", rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex)
    Debug(dInfo, "S%d receive rpc from S%d of term%d in term%d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
    Debug(dInfo, "S%d receive LeaderCommit%d, self commit%d", rf.me, args.LeaderCommit, rf.commitIndex)
    if rf.isLeader && (args.LeaderCommit > rf.commitIndex || (args.LeaderCommit == rf.commitIndex && args.Term>rf.currentTerm)){
        rf.mu.Lock()
        // Debug(dLeader, "S%d receive heartbeat from leader%d and convert to follower", rf.me, args.LeaderId)
        rf.currentTerm = args.Term
        rf.heartbeatReceived = true
        rf.leaderId = args.LeaderId
        rf.votedFor = args.LeaderId
        rf.isLeader = false
        Debug(dWarn, "S%d give up to be leader", rf.me)
        rf.mu.Unlock()
    }

    if args.PrevLogIndex < rf.commitIndex {
        Debug(dWarn, "S%d prevIndex < commitIndex", rf.me)
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }

	if args.Term < rf.currentTerm {
        Debug(dWarn, "S%d term %d > S%d args.Term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

    // Debug(dLeader, "S%d receive heartbeat from leader%d", rf.me, args.LeaderId)
    // convert to follower
    
    // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        reply.Success = false
        Debug(dWarn, "S%d has %d logs", rf.me, len(rf.log))
        Debug(dWarn, "S%d receive unmatched log with prevIndex%d and term%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
        rf.currentTerm = args.Term
        rf.heartbeatReceived = true
        rf.leaderId = args.LeaderId
        rf.votedFor = args.LeaderId
        rf.isLeader = false
        return
    }
	// if the RPC is not heartbeat, do log replication
	if len(args.Entries) > 0 {
        // Debug(dLog,"S%d receive %d logs from leader S%d with Index%d in term%d with num%d",rf.me, len(args.Entries), args.LeaderId, args.Entries[0].CommandIndex,rf.currentTerm,args.Entries[0].Command)
		Debug(dLog, "S%d receive index%d num%d in term%d from S%d", rf.me, args.Entries[0].CommandIndex, args.Entries[0].Command, args.Entries[0].Term, args.LeaderId)
        // If an existing entry conflicts with a new one (same index but different terms), 
		// delete the existing entry and all thatfollow it
        if args.PrevLogIndex >= 0 {
            rf.log = rf.log[:args.PrevLogIndex+1]
        }
        // add new entry to local log
        // Debug(dLog, "S%d log len%d update with args.Entries%d", rf.me, len(rf.log), len(args.Entries))
        rf.log = append(rf.log, args.Entries...)
        // rf.log = append(rf.log, args.Entries[0])
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
        // Debug(dLog, "S%d commit update commit Index%d", rf.me, rf.commitIndex)
        // wake up the commit apply go routine
	}

    rf.mu.Lock()
    rf.currentTerm = args.Term
    Debug(dTerm, "S%d term changed to %d", rf.me, args.Term)
	rf.heartbeatReceived = true
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.isLeader = false
    rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int) bool {
    rf.mu.Lock()
    if !rf.isLeader {
        rf.mu.Unlock()
        return false
    }
    rf.mu.Unlock()
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
    prevIndex := rf.nextIndex[server]-1
	args.PrevLogIndex = prevIndex
	args.PrevLogTerm = rf.log[prevIndex].Term
	// args.Entries = rf.log[args.PrevLogIndex+1:len(rf.log)]
    args.Entries = []LogEntry{}
    nextToSend := len(rf.log)
    if args.PrevLogIndex+1 < len(rf.log) {
        // args.Entries = append(args.Entries, rf.log[args.PrevLogIndex+1])
        args.Entries = rf.log[args.PrevLogIndex+1:len(rf.log)]
    }
	args.LeaderCommit = rf.commitIndex
	reply := AppendEntriesReply{}
    // Debug(dInfo, "S%d send rpc to S%d with prevIndex%d and %dlogs", rf.me, server, prevIndex, len(args.Entries))
    // Debug(dInfo, "S%d %v send rpc to S%d with endname:%v in term%d", rf.me, rf.peers[rf.me].Endname, server, rf.peers[server].Endname, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok && rf.isLeader {
        // meet a new server, transfer to follower
		if reply.Term > rf.currentTerm {
            rf.mu.Lock()
			rf.currentTerm = args.Term
            rf.leaderId = args.LeaderId
            rf.votedFor = -1
            rf.isLeader = false
            // Debug(dWarn, "S%d give up to be leader by find a newer server, term in %d", rf.me, reply.Term)
            rf.mu.Unlock()
            return ok
		}
        // if success, count and commit
        if len(args.Entries) > 0 {
            if reply.Success {
                rf.mu.Lock()
                rf.nextIndex[server]=nextToSend
                // logIndex := args.PrevLogIndex+1
                logIndex := nextToSend-1
                if logIndex > rf.commitIndex && logIndex<len(rf.log) && rf.log[logIndex].Term == rf.currentTerm {
                    rf.logAcceptCnt[logIndex]++
                    // Debug(dSnap, "S%d receive accept from S%d", rf.me, server)
                    if rf.logAcceptCnt[logIndex]*2 > len(rf.peers) {
                        rf.commitIndex = logIndex
                        Debug(dLog, "S%d commit log%d", rf.me, logIndex)
                    }
                }
                rf.mu.Unlock()
            } else{
                // if !success, decrease lastIndex
                rf.mu.Lock()
                // Debug(dLog, "S%d decrese nextIndex to %d for S%d", rf.me, rf.nextIndex[server], server)
                rf.nextIndex[server]--
                rf.mu.Unlock()
            }
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
                    // Debug(dLeader, "S%d send heartbeat to S%d in term%d", rf.me, peerid, rf.currentTerm)
				}(id)
			}
		}
		time.Sleep(time.Duration(rf.heartbeatDuration) * time.Millisecond)
	}
}