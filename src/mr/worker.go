package mr

import "fmt"
import "log"
import "os"
import "sort"
import "time"
import "strings"
import "strconv"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"
import "encoding/json"

var NReduce int
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByHash []KeyValue

// for sorting by key.
func (a ByHash) Len() int           { return len(a) }
func (a ByHash) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
// sort by bucket
func (a ByHash) Less(i, j int) bool { return ihash(a[i].Key)%NReduce < ihash(a[j].Key)%NReduce }

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
// sort by bucket
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// log.Printf("Worker launch success.\n")
	// pass the input, and transfer input to mapf ans reducef
	for {
		reqsucc, jobType, filename, jobId, mapNum:= CallForJob()
		if !reqsucc {
			// log.Printf("Apply for job request failed!\n")
			return
		}
		// log.Printf("Got %v job %d with file %v.\n", jobType, jobId, filename)
		if jobType == "map" {
			err := MapJob(mapf, filename, jobId)
			if err!=nil{
				log.Fatalf("Map task %v for %v failed!\n", jobId, filename)
			}
			// finished := JobCompleteNotice(jobId,jobType)
			// if finished {
			// 	log.Printf("Job %d with type %v excuted successfully.\n", jobId, jobType)
			// } else {
			// 	log.Printf("Job %d with type %v excuted failed.\n", jobId, jobType)
			// }
		} else if jobType == "reduce"{
			err := ReduceJob(reducef, jobId, mapNum)
			if err != nil{
				log.Fatalf("Reduce task %v failed!\n", jobId)
			}
			// finished := JobCompleteNotice(jobId,jobType)
			// if finished {
			// 	log.Printf("Job %d with type %v excuted successfully.\n", jobId, jobType)
			// } else {
			// 	log.Printf("Job %d with type %v excuted failed.\n", jobId, jobType)
			// }
		} else if jobType == "none"{
			// log.Printf("No task at current time.\n")
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
	}
}

func MapJob (mapf func(string, string) []KeyValue, filename string, taskid int) error {
	file, err:= os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	sort.Sort(ByHash(intermediate))
	i:=0
	var filenames []string
	for i<len(intermediate) {
		j := i+1
		// find kv pair in same bucket
		for j<len(intermediate) && ihash(intermediate[j].Key)%NReduce==ihash(intermediate[i].Key)%NReduce {
			j++
		}
		// interfname := "../mr-tmp-file/mr_intermediate_"+strconv.Itoa(taskid)+"_"+strconv.Itoa(ihash(intermediate[i].Key)%NReduce)
		interfname := "mr-intermediate-"+strconv.Itoa(taskid)+"-"+strconv.Itoa(ihash(intermediate[i].Key)%NReduce)+"-tmp"
		filenames = append(filenames,interfname)
		ofile, err := os.OpenFile(interfname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, os.ModePerm)
		if err != nil {
			log.Fatalf("cannot open %v", interfname)
		}
		// write to intermediate file
		enc := json.NewEncoder(ofile)
		for k:=i; k<j; k++{
			err := enc.Encode(&intermediate[k])
			if err!=nil {
				log.Fatalf("json format writing failed with err %v",err)
				// fmt.Println(err)
			}
		}
		ofile.Close()
		i=j
	}
	finished := JobCompleteNotice(taskid,"map")
	if finished {
		for _, tmpname := range(filenames) {
			err := os.Rename(tmpname,strings.TrimSuffix(tmpname,"-tmp"))
			if err!=nil{
				log.Printf("Intermediate file store failed.\n")
			}
		}
	} else {
		for _, tmpname := range(filenames) {
			err := os.Remove(tmpname)
			if err!=nil {
				log.Fatalf("Remove tmp file failed.\n")
			}
		}
		// log.Printf("Job %d with type %v excuted failed.\n", taskid, "map")
	}
	return nil
}

func ReduceJob(reducef func(string, []string) string, reduceId int, mapNum int) error {
	intermediate := []KeyValue{}
	i := 0
	for i<mapNum {
		// iname := "../mr-tmp-file/mr_intermediate_"+strconv.Itoa(i)+"_"+strconv.Itoa(reduceId)
		iname := "mr-intermediate-"+strconv.Itoa(i)+"-"+strconv.Itoa(reduceId)
		ifile,err := os.OpenFile(iname, os.O_CREATE|os.O_RDONLY|os.O_APPEND, os.ModePerm)
		if err != nil{
			log.Fatalf("cannot open %v with error %v", iname, err)
			return err
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		i++
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"+strconv.Itoa(reduceId)+"tmp"
	// ofile, _ := os.Create(oname)
	ofile, _ := os.OpenFile(oname,os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, os.ModePerm)
	i = 0
	for i < len(intermediate) {
		j := i+1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		values := []string{}
		for k:=i; k<j; k++ {
			values = append(values,intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i=j
	}
	finished := JobCompleteNotice(reduceId,"reduce")
	if finished {
		err := os.Rename(oname,strings.TrimSuffix(oname,"-tmp"))
		if err!=nil{
			log.Printf("Intermediate file store failed.\n")
		}
	} else {
		err := os.Remove(oname)
		if err!=nil {
			log.Fatalf("Remove tmp file failed.\n")
		}
		// log.Printf("Job %d with type %v excuted failed.\n", reduceId, "reduce")
	}
	return nil
}

func CallForJob() (bool,string,string,int,int) {
	args := Args{}
	// build args
	reply := JobAssignReply{}
	ok := call("Coordinator.JobRequest", &args, &reply)
	if ok {
		NReduce = reply.NReduce
		return ok, reply.JobType, reply.FileName, reply.TaskId, reply.MapNum
	} else {
		// fmt.Printf("Call failed!\n")
		return ok,"", "", -1, 0
	}
}

func JobCompleteNotice(jobId int, jobType string) bool {
	args := CompleteNoticeArgs{}
	args.JobId = jobId
	args.JobType = jobType
	reply := CompleteNoticeReply{}
	ok := call("Coordinator.JobCompleteNotice", &args, &reply)
	if ok {
		return reply.Succeed
	} else {
		// fmt.Printf("Call failed!\n")
		return ok
	}
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		// log.Printf("Cannot coonect to Coordinator, worker exit.\n")
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}