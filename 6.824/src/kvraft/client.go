package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// keep the current leader. if no leader failure, 
	// it can provide better performance
	curLeader     int

	requestId      int
	// used for server to identify client
	me            int64

	serverNum     int


}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.curLeader = 0
	ck.requestId = 0
	ck.serverNum = len(ck.servers)

	// how can a leader identify the Clerk?
	// at first, me = 0;
	// each time the leader find a new clerk (ck.me = 0), allocate a unique number to it.
	// however, it adds a lot of complexity 
	// In real world, we could use IP:port number as a unique identifier
	// for simplisity here, use a random int
	ck.me = nrand()

	return ck


}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	ck.requestId++

	for {

		args := GetArgs{}
		reply := GetReply{}
		args.Key = key
		// require a lock? involve concurrency?
		// probably not
		args.RequestId = ck.requestId
		args.Me = ck.me
		ok := ck.servers[ck.curLeader].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.curLeader = (ck.curLeader + 1) % ck.serverNum
			continue
		} else {
			return reply.Value
		}



	}

	fmt.Println("Control should never reach here.")
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.requestId++

	for {

		args := PutAppendArgs{}
		reply := PutAppendReply{}
		args.Key = key
		args.Value = value
		args.Op = op
		args.RequestId = ck.requestId
		args.Me = ck.me

		ok := ck.servers[ck.curLeader].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader{
			ck.curLeader = (ck.curLeader + 1) % ck.serverNum
			continue
		} else {
			return
		}

	}




}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
