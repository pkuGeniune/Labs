package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "sort"
import "time"
import "os"
import "strconv"
import	"unicode"
import	"strings"
import "sync"
//import "io"



// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type workstate struct{
	worktype string
	workseq string
	mux sync.Mutex
} 

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	/*    a test for RPC, and find that 0 and "" can not be transmitted
	t_args := TestArgs{0, ""}
	t_rpy := TestReply{0, ""}
	for true {
		call("Master.TestRPC", &t_args, &t_rpy)
		fmt.Println(t_rpy.X, t_rpy.Y)
		t_args.X = 1
		t_args.Y = "1"
		call("Master.TestRPC", &t_args, &t_rpy)
		fmt.Println(t_rpy.X, t_rpy.Y)
		t_args.X = 0
		t_args.Y = "2"
		time.Sleep(time.Second)
	}*/
	// Your worker implementation here.

	c_args := CrashDetectArgs{"", ""}
	c_reply := CrashDetectRpy{""}
	ws := workstate{}
	go func (){         // to periodically infor the master it is alive
		ws.mux.Lock()
		for ws.worktype != "done" {

			if ws.worktype != "idle"{    // map or reduce work
				c_args.WorkType = ws.worktype
				c_args.WorkSeq = ws.workseq

				call("Master.Workeralive", &c_args, &c_reply)
			}
			ws.mux.Unlock()
			time.Sleep(time.Second)
			ws.mux.Lock()
		}
	}()

	args := Worker_req{"idle", "", 0}
	reply := Master_rpy{}
	call("Master.Distribute", &args, &reply)                  //有时候0会传不过来，无法生成 0 的中间文件
	//fmt.Println("taskseq:", reply.TaskSeq, "taskcnt:", reply.CNT, "filename:", reply.Filename)
	for true {
	switch reply.Job{

	case "map":
		//read the contents
		ws.mux.Lock()
		ws.worktype = reply.Job
		ws.workseq = strconv.Itoa(reply.TaskSeq)
		ws.mux.Unlock()
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()
		kva := mapf(reply.Filename, string(content)) 

		//then store them to somewhere

		files := make([]*os.File, reply.NReduce)
		
		for i := 0; i < reply.NReduce; i++{
			//fmt.Println(reply.TaskSeq)
			oname := "mr-" + strconv.Itoa(reply.TaskSeq) + "-" + strconv.Itoa(i + 1)
			ofile, _ := os.Create(oname)
			files[i] = ofile
			//fmt.Println(oname)
		}

		for _, v := range kva {
			i := ihash(v.Key) % reply.NReduce
			fmt.Fprintf(files[i], "%v %v\n", v.Key, v.Value)
		}
		for i := 0; i < reply.NReduce; i++{
			files[i].Close()
		}
		args = Worker_req{"done_map", reply.Filename, 0}
		ws.mux.Lock()
		ws.worktype = "idle"
		ws.workseq = ""
		ws.mux.Unlock()
		call("Master.Distribute", &args, &reply)
		//fmt.Println("taskseq:", reply.TaskSeq, "taskcnt:", reply.CNT, "filename:", reply.Filename)


	case "reduce":
		ws.mux.Lock()
		ws.worktype = reply.Job
		ws.workseq = strconv.Itoa(reply.TaskSeq)
		ws.mux.Unlock()
		//worker.reduce
		intermediate := []KeyValue{}
		v := KeyValue{}

		//read all intermediate files
		for i := 0; i < reply.NMap; i++{
			inter_name := "mr-" + strconv.Itoa(i + 1) + "-" + strconv.Itoa(reply.TaskSeq)  
			//fmt.Println(inter_name)

			////
			file, err := os.Open(inter_name)
			content, err := ioutil.ReadAll(file)
			if err != nil {
				fmt.Println("Open file error")
			}
			file.Close()

			ff := func(r rune) bool { return unicode.IsSpace(r) }

		// split contents into an array of words.
			words := strings.FieldsFunc(string(content), ff)
		/////
			l := len(words)
			for i := 0; i < l; i += 2{
				v.Key = words[i]
				v.Value = words[i + 1]
				intermediate = append(intermediate, v)
			}



		}
		sort.Sort(ByKey(intermediate))
		oname := "mr-out-" + strconv.Itoa(reply.TaskSeq)
		ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-i.
	//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
		//fmt.Println("done_reduce:", reply.TaskSeq)
		ws.mux.Lock()
		ws.worktype = "idle"
		ws.workseq = ""
		ws.mux.Unlock()
		args = Worker_req{"done_reduce", "", reply.TaskSeq}
		//fmt.Println(reply.TaskSeq)
		call("Master.Distribute", &args, &reply)

	case "wait":
		time.Sleep(time.Second)

	case "finish":
		return
	}

	args.State = "idle"
	call("Master.Distribute", &args, &reply)
	//fmt.Println("TaskSeq:", reply.TaskSeq, "taskcnt:", reply.CNT, "filename:", reply.Filename)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}



//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
