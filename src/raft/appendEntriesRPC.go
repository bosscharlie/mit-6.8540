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
    Dindex  bool    // if leader should decrease index
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply * AppendEntriesReply) {
	// stale leader rejoin, with a high term because of keep retrying new election, but have a stale commitIndex
    if rf.isLeader && (args.LeaderCommit > rf.commitIndex || (args.LeaderCommit == rf.commitIndex && args.Term>rf.currentTerm)){
        rf.mu.Lock()
        rf.currentTerm = args.Term
        rf.heartbeatReceived = true
        rf.leaderId = args.LeaderId
        rf.votedFor = args.LeaderId
        rf.isLeader = false
        Debug(dLeader, "S%d give up to be leader", rf.me)
        rf.mu.Unlock()
    }

    // reject stale leader, overwrite the committed log
    if  args.Term < rf.currentTerm || args.PrevLogIndex < rf.commitIndex {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }
    
    // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        reply.Success = false
        Debug(dLog, "S%d receive unmatched log with prevIndex%d and term%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
        reply.Dindex = true
        rf.currentTerm = args.Term
        rf.heartbeatReceived = true
        rf.leaderId = args.LeaderId
        rf.votedFor = args.LeaderId
        rf.isLeader = false
        return
    }

	// if the RPC is not heartbeat, do log replication
	if len(args.Entries) > 0 {
        Debug(dLog,"S%d receive %d logs from leader S%d in term%d",rf.me, len(args.Entries), rf.currentTerm)
        // If an existing entry conflicts with a new one (same index but different terms), 
		// delete the existing entry and all that follow it
        if args.PrevLogIndex >= 0 {
            rf.log = rf.log[:args.PrevLogIndex+1]
        }
        // add new entries to local log
        rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1) // update commitIndex
	}

    rf.mu.Lock()
    rf.currentTerm = args.Term
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
    args.Entries = []LogEntry{}
    nextToSend := len(rf.log)
    if args.PrevLogIndex+1 < len(rf.log) {
        args.Entries = rf.log[args.PrevLogIndex+1:len(rf.log)]
    }
	args.LeaderCommit = rf.commitIndex
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok && rf.isLeader {
        // meet a new server, transfer to follower
		if reply.Term > rf.currentTerm {
            rf.mu.Lock()
			rf.currentTerm = args.Term
            rf.leaderId = args.LeaderId
            rf.votedFor = -1
            rf.isLeader = false
            rf.mu.Unlock()
            return ok
		}
        // if success, count and commit
        if len(args.Entries) > 0 {
            if reply.Success {
                rf.mu.Lock()
                rf.nextIndex[server]=nextToSend
                logIndex := nextToSend-1
                // only count logs in current term
                if logIndex > rf.commitIndex && logIndex<len(rf.log) && rf.log[logIndex].Term == rf.currentTerm {
                    rf.logAcceptCnt[logIndex]++
                    if rf.logAcceptCnt[logIndex]*2 > len(rf.peers) {
                        rf.commitIndex = logIndex
                        Debug(dLog, "S%d commit log%d", rf.me, logIndex)
                    }
                }
                rf.mu.Unlock()
            } else{
                rf.mu.Lock()
                if reply.Dindex {
                    // Log consistency check failed, decrease the nextIndex
                    rf.nextIndex[server]--
                }
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
				}(id)
			}
		}
		time.Sleep(time.Duration(rf.heartbeatDuration) * time.Millisecond)
	}
}