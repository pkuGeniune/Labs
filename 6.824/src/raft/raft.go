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

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"
import "fmt"



import "bytes"
import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// a type for log entry
type LogEntry struct{

	Term      int
	Command   interface{}

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
	currentTerm  int
	voteFor      int             // -1 represents not voted yet
	logEntries   []LogEntry          /* a slice for string will be better? */
	isLeader     bool
	leaderExsist bool
	totalServers int 
	majority     int   
    voteFlag     bool            //used to avoid split vote 


	//volatile state on all servers
	lastLogIndex int
	commitIndex  int
	lastApplied  int

	//volatile state on a leader
	nextIndex    map[int]int               //initialized to leader last log index + 1
	matchIndex   map[int]int               // initialized to 0, or what ever


    //addition
    applyCh      chan ApplyMsg
    entryACK     map[int]int
    appendMu     []*sync.Mutex


	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	isleader = rf.isLeader
	//fmt.Printf("rf.me: %v, rf.currentTerm %v, rf.isleader %v   \n", rf.me, rf.currentTerm, rf.isLeader)

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


    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.voteFor)
    e.Encode(rf.logEntries)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)

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

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var voteFor     int
    var logEntries  []LogEntry
    if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logEntries) != nil{
        panic("Fail to decode state")

    } else {
        rf.currentTerm = currentTerm
        rf.voteFor = voteFor
        rf.logEntries = logEntries

        rf.lastLogIndex = len(rf.logEntries) - 1
    }


}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term          int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoteGranted  string

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer  rf.persist()
    if !(args.Term > rf.currentTerm) {
        reply.VoteGranted = "Denied"
    } else {
        if args.Term > rf.currentTerm{
            rf.isLeader = false
            rf.currentTerm = args.Term
        }
        upToDate := true
        if rf.lastLogIndex != -1{
            if args.LastLogTerm < rf.logEntries[rf.lastLogIndex].Term {
                upToDate = false
            }
            if args.LastLogTerm == rf.logEntries[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex {
                upToDate = false
            }
        }
        if upToDate {
            reply.VoteGranted = "Granted"
            rf.voteFlag = true
            rf.voteFor = args.CandidateID
            //fmt.Printf("%v vote for %v at term %v\n", rf.me, args.CandidateID, rf.currentTerm)
        } else {
            reply.VoteGranted = "Denied"
        }
    }
    reply.Term = rf.currentTerm
   

	// reply false if args. Term < currentTerm

	// if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)


}



type AppendEntriesArgs struct {

	LeaderID  int
	Term      int

	PrevLogIndex   int
	PrevLogTerm    int
	Entries        []LogEntry
	LeaderCommit   int
}

type AppendEntriesReply struct {
	Term    int
	State   string
    Xterm   int
    Xindex  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){

    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    //fmt.Printf("server: %v, args.PrevLogIndex: %v\n", rf.me, args.PrevLogIndex)

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    } else {
        rf.leaderExsist = true
        rf.isLeader = false
        rf.currentTerm = args.Term
        reply.Term = args.Term
        reply.State = "OK"

        //有东西
        //if len(args.Entries) != 0{

            //不包含 preLogEntry, Xterm = -1
            if rf.lastLogIndex < args.PrevLogIndex{
                reply.Xterm = -1
                reply.Xindex = rf.lastLogIndex
                reply.State = "fail"
                return
            } else{  //有 preEntry, 看term 是否一样
                //-1单独处理，避免越界，直接接收
                if args.PrevLogIndex < 0 {
                    rf.logEntries = rf.logEntries[:0]
                    rf.logEntries = append(rf.logEntries, args.Entries...)
                    rf.lastLogIndex = len(args.Entries) - 1
                    reply.State = "OK"

                } else {
                    //term 不一致，只保留 index之前的记录  Xindex 为 第一个是 Xterm 的记录
                    if args.PrevLogTerm != rf.logEntries[args.PrevLogIndex].Term {

                        reply.Xterm = rf.logEntries[args.PrevLogIndex].Term
                        for j := 0; j < args.PrevLogIndex; j++ {
                            if rf.logEntries[j].Term == reply.Xterm {
                                reply.Xindex = j
                                break
                            }
                        }
                        
                        rf.logEntries = rf.logEntries[:args.PrevLogIndex]
                        rf.lastLogIndex = args.PrevLogIndex - 1


                       
                        reply.State = "fail"
                        return
                    } else { // term 一致， 从index开始更新
                        rf.logEntries = rf.logEntries[:args.PrevLogIndex + 1]
                        rf.logEntries = append(rf.logEntries, args.Entries...)
                        rf.lastLogIndex = args.PrevLogIndex + len(args.Entries)
                        reply.State = "OK"
                    }

                }

            }

        //}

        //如果是 heartbeat，就可以 commit了？这里不对啊
        // follower 有未commit的entry
        // leader 还没接受新的 entry，就发了一个新的 Leadercommit，这就会出错
        // 需要处理一下，去掉 if 应该就可以了？

        //然后处理一下 commit
        precommit := rf.commitIndex
        if rf.lastLogIndex < args.LeaderCommit {
            rf.commitIndex = rf.lastLogIndex
        } else {
            rf.commitIndex = args.LeaderCommit
        }
        if rf.commitIndex > precommit {
            entries := rf.logEntries[precommit + 1: rf.commitIndex + 1]
            for i, v := range entries {
                msg := ApplyMsg{}
                msg.CommandValid = true
                msg.CommandIndex = precommit + i + 2
                msg.Command = v.Command
                rf.applyCh <- msg

            }
           
            rf.lastApplied = rf.commitIndex
        }
    }

    
//reply false term < currentTerm       a stale leader?
//reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
//if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
//append any new entries not already in the log
//if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    return

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if !rf.isLeader{
        return 0, 0, false
    } else {
        newlog := LogEntry{}
        newlog.Term = rf.currentTerm
        newlog.Command = command
        rf.logEntries = append(rf.logEntries, newlog)
        rf.lastLogIndex++
        rf.entryACK[rf.lastLogIndex] = 1
        rf.persist()

        return rf.lastLogIndex + 1, rf.currentTerm, true

    }

	//if it is not leader, return false
    fmt.Println("Control should never reach here.")

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

    // Your initialization code here (2A, 2B, 2C).


	//rf initialization

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.totalServers = len(peers)
	rf.majority = (rf.totalServers + 1) / 2
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.leaderExsist = false
	rf.lastLogIndex = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
    rf.voteFlag = false
    rf.applyCh = applyCh

	//rf.logEntries = make([]logEntry, 1)
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)


    for i := 0; i < rf.totalServers; i++ {
        m := new(sync.Mutex)
        rf.appendMu = append(rf.appendMu, m)
    }






    go rf.StateManage()
    go rf.EntryManage()

	




	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) StateManage(){

    time.Sleep(electionTimeout())

    for {

        rf.mu.Lock()
        if rf.isLeader || rf.leaderExsist || rf.voteFlag {
            //fmt.Printf("%v, isLeader: %v, leaderExsist: %v, voteFlag: %v, term: %v\n", rf.me, rf.isLeader, rf.leaderExsist, rf.voteFlag, rf.currentTerm)
            rf.voteFlag = false
            rf.leaderExsist = false
            rf.mu.Unlock()
            time.Sleep(electionTimeout())
        } else {
            rf.currentTerm++
            rf.voteFor = rf.me
            rf.persist()
            //fmt.Printf("%v start election, term: %v\n", rf.me, rf.currentTerm)

            cond := sync.NewCond(&rf.mu)
            competeTerm := rf.currentTerm
            lastIndex := rf.lastLogIndex
            lastTerm  := 0
            if lastIndex != -1{
                lastTerm = rf.logEntries[lastIndex].Term
            }
            count := 1
            stale := false
            finish := 1


            for i, v := range rf.peers {

                if i == rf.me{
                    continue
                }

                go func (i int, v *labrpc.ClientEnd) {

                    args := RequestVoteArgs{}
                    reply := RequestVoteReply{}
                    args.Term = competeTerm
                    args.CandidateID = rf.me
                    args.LastLogIndex = lastIndex
                    args.LastLogTerm = lastTerm
                    ok := v.Call("Raft.RequestVote", &args, &reply)
                    rf.mu.Lock()
                    defer rf.mu.Unlock()
                    if ok {
                        if reply.VoteGranted == "Granted" {
                            count++
                        } else {
                            if reply.Term > rf.currentTerm {
                                stale = true
                            }
                        }

                    }
                    finish++
                    // 这里会不会阻塞 啊
                    cond.Broadcast()

                }(i, v)
            }

            t0 := time.Now()

            timeout := electionTimeout()
            for count < rf.majority && finish < rf.totalServers && (time.Since(t0) < timeout) && !stale{
                // 一定要确保，会有其他进程来唤醒这个 cond，不然这里就卡住了啊
                //上边的 broadcast 一开始放在了 if ok {} 的里面，一个bug 整一晚上......
                cond.Wait()
            }

            if count >= rf.majority && competeTerm == rf.currentTerm {
                rf.isLeader = true

                //fmt.Printf("%v becomes leader at term %v\n", rf.me, rf.currentTerm)

                for i, _ := range rf.nextIndex {
                    rf.nextIndex[i] = rf.lastLogIndex + 1
                    rf.matchIndex[i] = 0
                }

                //这个地方，如果再开机是没有的，但应该也不影响
                rf.entryACK = make(map[int]int)

                // a go func to commit entries when it becomes leader
                go func (rf *Raft){

                    for {

                        rf.mu.Lock()
                        // 不是leader，返回，下次再竞选的时候启动
                        if !rf.isLeader {
                            rf.mu.Unlock()
                            return
                        } else {

                            precommit := rf.commitIndex

                            for i, v := range rf.entryACK {
                                //fmt.Printf("i: %v, v: %v\n", i, v)
                                if v > rf.majority || v == rf.majority {
                                    if rf.commitIndex < i {
                                        rf.commitIndex = i
                                    }
                                    delete(rf.entryACK, i)
                                }

                            }



                            if rf.commitIndex > precommit {
                                //fmt.Printf("%v commit %v at term %v\n", rf.me, rf.commitIndex, rf.currentTerm)
                                entries := rf.logEntries[precommit + 1: rf.commitIndex + 1]
                                for i, v := range entries {
                                    msg := ApplyMsg{}
                                    msg.CommandValid = true
                                    msg.CommandIndex = precommit + i + 2
                                    msg.Command = v.Command
                                    rf.applyCh <- msg

                                }
                                //  last Applied 其实没什么用啊
                                rf.lastApplied = rf.commitIndex
                            }

                        }
                        rf.mu.Unlock()
                        time.Sleep(50 * time.Millisecond)

                    }

                }(rf)

            } else {      //lose，如不不是因为超时，再休眠一会
                t1 := timeout - time.Since(t0)
                if t1 > 0 {
                    time.Sleep(t1)
                }
                
            }

            rf.mu.Unlock()




        }





    }


}

func (rf *Raft) EntryManage(){
    for {
        rf.mu.Lock()
        if !rf.isLeader {
            rf.mu.Unlock()
            time.Sleep(100 * time.Millisecond)
        } else {

            term := rf.currentTerm           //之后，真正的 term 可能会变大
            commit := rf.commitIndex          // commit Index，可能会大，也可能会小(初始化时为 -1。但应该不会，那就是另一个 server 了，会重新跑一个 线程)
            rf.mu.Unlock()

            for i, v := range rf.peers {

                if i == rf.me {
                    continue
                }

                go func (i int, v *labrpc.ClientEnd, term int, commit int){


                    //  3） 提前加锁，读数据，释放锁，管用嘛？
                    //  如果刚读完，接着被改？ 
                    //  lastLogIndex 不影响，没发完的下一个 loop 再发
                    //  nextIndex，只会被这个 routine 改变， 且只有会 获得 appendMu 的进程可以改变， 而且改变了之后，那个 routine 也已经跑完了
                    //  结论，有用
                    rf.mu.Lock()
                    nextIndex := rf.nextIndex[i]        // 会有并发的，nextIndex 之后可能会大，也可能会小
                    lastIndex := rf.lastLogIndex        // last只考虑增大
                    rf.mu.Unlock()


                    //  1） (之前)这里读数据没锁！  但是放到锁后面，就会死锁
                    //   如果要要发的数据项，就拿一个锁。 还是可能产生好多 有相同 nextIndex 和 lastIndex 的进程......
                    if !(nextIndex > lastIndex){
                        rf.appendMu[i].Lock()
                        defer rf.appendMu[i].Unlock()
                    }


                    args := AppendEntriesArgs{}
                    reply := AppendEntriesReply{}

                    args.Term = term
                    args.LeaderCommit = commit
                    rf.mu.Lock()
                    // 2） 如果 appendMu 放在这，然后后面会释放 mu，但不会释放 appendMu, 后面的进程如果拿了 mu，又需求 appendMu， 就死锁了......
                    // 啊，真的吐了
                    //  
                    



                    // 这里 nextIndex 和 lastIndex  用旧的好还是用新的好？
                    // 应该用 旧的， 不然旧的发现不需要添加东西，没拿锁，这里发现需要发东西，就没锁了，race
                    // 用旧的有么有问题
                    // 如果依照旧的，应该发个 heartbeat，那下面也应该发 heartbeat
                    // 如果有要发的，那么 nextIndex 是不会变的， lastIndex 只可能增大，没事
                    args.PrevLogIndex = nextIndex - 1


                    // used to debug
                    if !rf.isLeader {
                        rf.mu.Unlock()
                        return
                    }


                    if nextIndex > 0 {
                        if args.PrevLogIndex > rf.lastLogIndex {
                            fmt.Printf("server: %v, args.pre: %v, lastIndex: %v，isleader? :%v\n", rf.me, args.PrevLogIndex, rf.lastLogIndex, rf.isLeader)
                        }


                        args.PrevLogTerm = rf.logEntries[args.PrevLogIndex].Term
                        if !(lastIndex < nextIndex){
                            entries := make([]LogEntry, len(rf.logEntries[(args.PrevLogIndex + 1):]))
                            copy(entries, rf.logEntries[(args.PrevLogIndex + 1):])
                            args.Entries = entries
                        }
                    } else {
                    	// 确保应该也发 heartbeat
                    	if nextIndex <= lastIndex{
                    		args.PrevLogTerm = 0
                        	_entries := make([]LogEntry, len(rf.logEntries))
                        	copy(_entries, rf.logEntries)
                        	args.Entries = _entries
                    	}
                        
                    }
                    //这里解锁，提高效率，预防死锁
                    rf.mu.Unlock()
                    //但是有个问题，可能导致两个 下面的模块还没完成，新的 routine 又开始了，这样 rf.nextIndex 就可能加多次，尤其是 如果 v call 不到，更容易出现多次叠加
                    //再填一个锁好了......  即 rf.appenMu[i]
                    //但这样，heartbeat 又可能会很慢...... 一次 call 不到，会等很久才会call另一个，这样，一个disconnect的leader重新接入的时候，会要等很久才能下台
                    //所以只在有 entry 要发的时候 才 lock



                    ok := v.Call("Raft.AppendEntries", &args, &reply)
                    if ok {
                        rf.mu.Lock()
                        if reply.Term > rf.currentTerm {
                            rf.isLeader = false

                            rf.currentTerm = reply.Term
                            rf.persist()
                        } else if reply.State == "OK" {
                            for j := nextIndex; j < nextIndex + len(args.Entries); j++ {
                                _, _ok := rf.entryACK[j]
                                if _ok {
                                    rf.entryACK[j]++
                                }
                            }
                            rf.nextIndex[i] = nextIndex + len(args.Entries)
                            rf.matchIndex[i] = rf.nextIndex[i] - 1
                        } else {

                            if reply.Xterm == -1 {
                                rf.nextIndex[i] = reply.Xindex + 1
                            } else {


                                f_Term := false
                                j := 0

                                // used to debug
                                if !rf.isLeader {
                                	rf.mu.Unlock()
                                	return
                                }
                       			if args.PrevLogIndex > rf.lastLogIndex {
                           			fmt.Printf("server: %v, args.pre: %v, lastIndex: %v，isleader? :%v\n", rf.me, args.PrevLogIndex, rf.lastLogIndex, rf.isLeader)
                        		}

                                for j = args.PrevLogIndex; j >= 0; j-- {
                                    if rf.logEntries[j].Term == reply.Xterm {
                                        f_Term = true
                                        break
                                    } 
                                }

                                if f_Term {
                                    rf.nextIndex[i] = j + 1
                                } else {
                                    rf.nextIndex[i] = reply.Xindex
                                }

                            }


                        }
                        rf.mu.Unlock()

                    }
      

                    return
                }(i, v, term, commit)

            }
            time.Sleep(80 * time.Millisecond)

        }

    }
}

func electionTimeout() time.Duration{
    return time.Duration(300 + rand.Intn(300)) * time.Millisecond
}