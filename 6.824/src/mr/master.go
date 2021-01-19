package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"
import "strconv"


type Master struct {
	// Your definitions here.
	// 0, 1, 2, represents not-start, processing, finished, respectively.
	mapWorklist map[string]int
	// map file to its sequence   
	mapWorkSeq map[string]int  
	 // as with maplist  
	redWorklist map[int]int     
	NReduce, NMap int            
	mux sync.Mutex
	//used to test RPC, not used in the lab
	taskcnt int   
	// 1 represents working, 0 represents failure              
	mapWorkerState map[int]int  
	redWorkerState map[int]int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Distribute(args *Worker_req, reply *Master_rpy) error {      //handler to schedule work

	m.mux.Lock()
	defer m.mux.Unlock()

	m.taskcnt++               //used for test, ignore

	//fmt.Println(m.mapWorkSeq["pg-dorian_gray.txt"])


	switch args.State{
	case "idle":

		f := true
		for str, sta := range m.mapWorklist{        
			//fmt.Println(str)
			if sta == 0 {
				reply.Job = "map"
				reply.Filename = str
				reply.TaskSeq = m.mapWorkSeq[str]            //这里的taskSeq = 0 时，在worker 里看不到改动	
				//fmt.Println(str, m.mapWorkSeq[str], reply.TaskSeq)

				reply.NReduce = m.NReduce
				reply.CNT = m.taskcnt
				m.mapWorklist[str] = 1

				return nil
			}
			if sta == 1{
				f = false
			}
		}

		
		if !f {    //all map works are scheduled but some of them aren't finished
			reply.Job = "wait"
			reply.CNT = m.taskcnt
			return nil
		} else {         //map finished

			for i, sta := range m.redWorklist{
				if sta == 0 {
					reply.Job = "reduce"
					reply.Filename  = ""
					reply.TaskSeq = i
					reply.NMap = m.NMap
					reply.CNT = m.taskcnt
					m.redWorklist[i] = 1
					return nil
				}
				if sta == 1{
					f = false
				}
			}
		}

		if !f {   //all reduce works are scheduled but some of them aren't finished
			reply.Job = "wait"
			reply.CNT = m.taskcnt
			return nil
		} 

		//reduce finished, tell the worker to exit
		reply.CNT = m.taskcnt
		reply.Job = "finish"

	case "done_map":
		m.mapWorklist[args.Info] = 2
		reply.TaskSeq = 0
		return nil

	case "done_reduce":
		m.redWorklist[args.Redseq] = 2
		//fmt.Println(args.Redseq)
		return nil


	}
	return nil
}


//handler to handle failure
func (m *Master) Workeralive(args *CrashDetectArgs, reply *CrashDetectRpy) error {     
	//worker call the func periodically to set its state in server alive, 
	//and master set their state to dead per longer period
	if args.WorkType == "map" {
		m.mux.Lock()
		defer m.mux.Unlock()
		v, _ := strconv.Atoi(args.WorkSeq)
		m.mapWorkerState[v] = 1
		return nil
	}
	if args.WorkType == "reduce" {
		m.mux.Lock()
		defer m.mux.Unlock()
		v, _ := strconv.Atoi(args.WorkSeq)
		m.redWorkerState[v] = 1
		return nil
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//for the test of RPC
func (m *Master) TestRPC(args *TestArgs, reply *TestReply) error {
	reply.X = args.X
	reply.Y = args.Y
	fmt.Println(reply.X, reply.Y)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire Job has finished.
//
func (m *Master) Done() bool {   //simply check whether all reduce works are done
	ret := true

	// Your code here.
	m.mux.Lock()       
	defer m.mux.Unlock()   //its not that matter if you don't lock here
	for _, v := range m.redWorklist{   
		if v != 2 {return false}
	}

	time.Sleep(time.Second)  //wait to send a finish message to worker to make them terminate



	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, NReduce int) *Master {
	m := Master{}
	m.mapWorklist = make(map[string]int)
	m.mapWorkSeq = make(map[string]int)    
	m.redWorklist = make(map[int]int) 
	m.mapWorkerState = make(map[int]int)
	m.redWorkerState = make(map[int]int)
	//initializations
	for i, _ := range m.mapWorkerState{
		m.mapWorkerState[i] = 0;
	}
	for i, _ := range m.redWorkerState{
		m.redWorkerState[i] = 0
	}
	for i, v := range files{
		m.mapWorklist[v] = 0
		m.mapWorkSeq[v] = i + 1
	}
	for i := 1; i <= NReduce; i++ {
		m.redWorklist[i] = 0;        
	}
	m.NReduce = NReduce
	m.NMap = len(files)

	m.server()

	go func(){         // a go routine to find failure
		for true {
			m.mux.Lock()

			//check if any mapwork that is processing does not response in the last 15 seconds
			for i, v := range m.mapWorklist{
				// if the task is processing
				if v == 1 {
					t := m.mapWorkSeq[i]
					// and thw corresponding worker does not contact with master in 15 seconds
					if m.mapWorkerState[t] == 0{
						m.mapWorklist[i] = 0
						for j := 1; j <= m.NReduce; j++{
							tname := "mr-" + strconv.Itoa(t) + "-" + strconv.Itoa(j)
							os.Remove(tname)
						}
					}
					m.mapWorkerState[t] = 0
				}
			}

			//check if any mapwork that is processing does not response in the last 15 seconds
			for i, v := range m.redWorklist{
				if v == 1 {
					if m.redWorkerState[i] == 0{
						m.redWorklist[i] = 0
						tname := "mr-out-" + strconv.Itoa(i)
						os.Remove(tname)

					}
					m.redWorkerState[i] = 0
				}
			}


			m.mux.Unlock()
			time.Sleep(15 * time.Second)
		}
	}()


	return &m
}
