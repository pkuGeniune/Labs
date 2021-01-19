package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Worker_req struct{
	State string
	Info string
	Redseq int
}
type Master_rpy struct{
	Job string
	Filename string
	TaskSeq int
	NReduce int
	NMap int
	CNT int
}

type TestArgs struct{
	X int
	Y string
}

type TestReply struct{
	X int
	Y string
}

type CrashDetectArgs struct{
	WorkType string     //map or reduce
	WorkSeq string      //work sequence

}

type CrashDetectRpy struct{
	Info string
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
