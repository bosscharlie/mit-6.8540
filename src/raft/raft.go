package raft

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

func Min(a,b int) int {
	if a <= b {
		return a
	}
	return b
}

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
	Term 			int
	Command 		interface{}
	CommandIndex	int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	leaderId    int  // leader's id of current term
	currentTerm int  // lastest term server has seen
	votedFor 	int	 // candidateId that recerived vote in current term	
	voteNum     int  // vote from other peers
	isLeader	bool // this raft server is a leader or not
    
    heartbeatReceived 	bool // is there any heartbeat received from peers, use to check if there need to start a leader election
	commitIndex 		int  // index of last committed entry
    appliedIndex        int  // index of latest applied entry
	log					[]LogEntry
    logAcceptCnt        map[int]int
    nextIndex           []int // record nextIndex for each other server

	// config
	heartbeatDuration int64 // heartbeat timeout config
	electionTimeout   int64 // election timeout config
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if !rf.isLeader {
        return -1, -1, false
    }
    entry := LogEntry{ rf.currentTerm, command, len(rf.log)}
    Debug(dLog, "S%d start with command %d in term %d with num%d", rf.me, entry.CommandIndex, entry.Term, command)
    rf.log = append(rf.log, entry)
    rf.logAcceptCnt[len(rf.log)-1]=1
	return entry.CommandIndex, entry.Term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyMsgChk(applyCh chan ApplyMsg) {
    for !rf.killed() {
        if rf.commitIndex > rf.appliedIndex {
            for i:=rf.appliedIndex+1; i<=rf.commitIndex; i++ {
                msg := ApplyMsg{}
                msg.CommandValid = true
                msg.Command = rf.log[i].Command
                msg.CommandIndex = rf.log[i].CommandIndex
                Debug(dLog, "S%d apply msg%d", rf.me, msg.CommandIndex)
                applyCh <- msg
                rf.appliedIndex++
            }
        }
        time.Sleep(10 * time.Millisecond)
    }
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1 // vote for none
	rf.isLeader = false
	rf.commitIndex = 0
    rf.appliedIndex = 0
	rf.heartbeatReceived = false
    // init with a dummy entry
	rf.log = []LogEntry{LogEntry{-1,-1,-1}}
    rf.logAcceptCnt = make(map[int]int)
	rf.voteNum = 0
    // init next index array with value 1
    rf.nextIndex = make([]int, len(rf.peers))
    for i:=0; i<len(rf.nextIndex); i++ {
        rf.nextIndex[i]=1;
    }

	rf.heartbeatDuration = 100
	rf.electionTimeout = 300 + (rand.Int63() % 160)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	Debug(dInfo, "S%d start", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.applyMsgChk(applyCh)

	return rf
}
