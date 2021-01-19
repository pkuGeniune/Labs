package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key        string
	Value      string 
	// Get, Append, or Put
	Type       string

	// used to detect duplicated request
	// server maintains a map, map each clientID to the last request it applied
	// if a coming request having ID not greater than the value in map
	// server reply without applying
	ClientId   int64
	RequestId  int

}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// key-value pair
	table   map[string]string

	// map clientId to its lastAppliedReuest
	lastAppliedReq map[int64]int

	// map a command's logIndex to its channel
	waitingList map[int]chan Op

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{}
	op.Key = args.Key
	op.Value = ""
	op.Type = "Get"
	op.ClientId = args.Me
	op.RequestId = args.RequestId

	// transmit the command to Raft via its Interface Start()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {

		// wait command to be applied
		_OK := kv.waitApply(op, index)

		if _OK {

			kv.mu.Lock()
			value, ok := kv.table[args.Key]
			kv.mu.Unlock()

			//可能已经过去有一段时间了，再查一下是不是 leader吧
			//如果不是leader了，会出错吗
			//无所谓，如果是 _OK 的，那就已经是 commit 的了，问谁都会一样

			if ok {
				reply.Value = value
				return
			} else {
				reply.Err = ErrNoKey
				return
			}


		} else {
			// 不行再试一次
			reply.Err = ErrWrongLeader
		}


	}


}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.ClientId = args.Me
	op.RequestId = args.RequestId

	// we can modify the start func to return an extra leader
	// but it is not allowed
	// thus an extra lock and read is required
	// or the client can simply choose the next server and give it a try
	/*  index, _, isLeader, Leader := kv.rf.Start(op) */

	// In this lab, ask the client to choose the next server is efficient enough
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		/*  reply.CurLeader = Leader  */
	} else {

		// wait command to be applied
		_OK := kv.waitApply(op, index)

		if _OK {
			reply.Err = OK
		} else {
			// 不行再试一次
			reply.Err = ErrWrongLeader
		}


	}



}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.table = make(map[string]string)
	kv.waitingList = make(map[int]chan Op)
	kv.lastAppliedReq = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	// 从 applyCh 接受信息，然后应用到 table 上
	// 不管是不是 server都是要进行的
	go func(kv *KVServer) {

		for msg := range kv.applyCh {
			

			// probably not happen
			if !msg.CommandValid {
                continue
            }

            op := msg.Command.(Op)
            kv.mu.Lock()
            if kv.duplicateRequest(op) {
            	kv.mu.Unlock()
                continue
            }

            switch op.Type {
            case "Put":
            	kv.table[op.Key] = op.Value
            case "Append":
            	kv.table[op.Key] += op.Value
            }

            kv.lastAppliedReq[op.ClientId] = op.RequestId

            //不用 if 应该也没事吧
            if ch, ok := kv.waitingList[msg.CommandIndex]; ok{
            	ch <- op
            }
            kv.mu.Unlock()

		}

	} (kv)

	return kv
}


//  wait for raft to apply the op
//  if the server is no longer a leader, return false
//  otherwise apply the op and return true
func (kv *KVServer) waitApply(op Op, index int) bool{

	_OK := false

	//  already applied, return true
	kv.mu.Lock()
	if kv.duplicateRequest(op) {
		kv.mu.Unlock()
		return true
	} else {

		// may not be a leader later, so it is necessary to set a timeout time 
		// if it failed this time, the client can simply retry it
		// another alternative is to check weather it is a leader for a period of time.
		timeout := 500 * time.Millisecond

		// make a chan
		// each time the server receives an ApplyMsg from Raft
		// it writes to the corresponding chan to unblock its

		if _, ok := kv.waitingList[index]; !ok {
			kv.waitingList[index] = make(chan Op, 1)
		}

		ch := kv.waitingList[index]

		kv.mu.Unlock()


		//

		select {

		case note := <- ch:
			// chan closed 
			if note.ClientId != op.ClientId || note.RequestId != op.RequestId {
				_OK = false
			} else {
				_OK = true
			}

		case <- time.After(timeout):
			_OK  = false

		}

		kv.mu.Lock()
		if _, ok := kv.waitingList[index]; ok {
			delete(kv.waitingList, index)
		}
		kv.mu.Unlock()


	}

	return _OK

}

func (kv *KVServer) duplicateRequest(op Op) bool{

	ClientId := op.ClientId
	RequestId := op.RequestId

	// 注意锁不能放在这里面
	// 释放了锁，后面再操作的时候，不能保证不是重复的了

	// 有关于这个 client 的记录，并且RequestId 相等，return true
	if v, ok := kv.lastAppliedReq[ClientId]; ok {
		if v == RequestId {
			return true
		}
	}

	return false

}










