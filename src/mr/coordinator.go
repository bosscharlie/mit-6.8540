package mr

import "fmt"
import "log"
import "net"
import "os"
import "time"
import "net/rpc"
import "net/http"

type Status int

const(
	IDLE Status = 0
	INPROGRESS Status =1
	COMPLETED Status =2
)

type Job struct {
	filename string
	id int
	jobType string
	startTime time.Time
	// status Status
}

type Coordinator struct {
	// Your definitions here.
	mapNum int
	reduceNum int
	phase string
	MapJobs []Job
	ReduceJobs []Job
	// InProgressJobs []Job
	InProgressJobs map[int]Job
	CompletedJobs []Job
	TimeChecker time.Ticker
	MapAssignChannel chan Job
	JobDoneChannel chan int
	// TimeCheckChannel chan int
	TimeoutChecker *time.Ticker
}

func (c* Coordinator) ChannelOp(){
	for {
		// log.Printf("Map: %v, Reduce: %v, Idle: %v, Done: %v\n",len(c.MapJobs), len(c.ReduceJobs), len(c.InProgressJobs), len(c.CompletedJobs))
		select {
			case <-c.MapAssignChannel:
				// log.Printf("Receive a job request.\n")
				
				// if len(c.MapJobs)!=0{
				if c.phase=="map" && len(c.MapJobs)!=0 {
					// Assign a map job
					jobTodo := c.MapJobs[0]
					c.MapJobs = c.MapJobs[1:]
					jobTodo.jobType = "map"
					jobTodo.startTime = time.Now()
					c.InProgressJobs[jobTodo.id]=jobTodo
					c.MapAssignChannel <- jobTodo
				// }else if len(c.MapJobs)==0 && len(c.ReduceJobs)!=0{
				}else if c.phase=="reduce" && len(c.ReduceJobs)!=0 {
					// Assign a reduce job
					jobTodo := c.ReduceJobs[0]
					c.ReduceJobs = c.ReduceJobs[1:]
					jobTodo.jobType = "reduce"
					jobTodo.startTime = time.Now()
					c.InProgressJobs[jobTodo.id]=jobTodo
					c.MapAssignChannel <- jobTodo
				} else {
					jobTodo := Job{}
					jobTodo.jobType = "none" 
					c.MapAssignChannel <- jobTodo
				}
			case jobId := <- c.JobDoneChannel:
				jobDone,ok := c.InProgressJobs[jobId]
				if ok {
					// log.Printf("%v Job %v complete notice.", jobDone.jobType,jobId)
					delete(c.InProgressJobs, jobId)
					c.CompletedJobs = append(c.CompletedJobs, jobDone)
					c.JobDoneChannel <- 0
					if len(c.MapJobs)==0 && len(c.InProgressJobs)==0 {
						c.phase="reduce"
					}
					if len(c.MapJobs) ==0 && len(c.ReduceJobs)==0 && len(c.InProgressJobs)==0{
						break;
					}
				}else{
					// log.Printf("Job %v has been timeout.", jobId)
					c.JobDoneChannel <- -1
				}
			case <- c.TimeoutChecker.C :
				checktime := time.Now()
				for jobId, jobInfo := range(c.InProgressJobs){
					if jobInfo.startTime.Add(10*time.Second).Before(checktime) {
						delete(c.InProgressJobs, jobId)
						// log.Printf("%v job %d timeout.",jobInfo.jobType,jobId)
						if jobInfo.jobType=="map" {
							c.MapJobs = append(c.MapJobs, jobInfo)
						} else if jobInfo.jobType=="reduce" {
							c.ReduceJobs = append(c.ReduceJobs, jobInfo)
						}
					}
				}
		}
	}
}

func (c *Coordinator) JobRequest(args *JobRequestArgs, reply *JobAssignReply) error{
	c.MapAssignChannel <- Job{}
	jobTodo := <- c.MapAssignChannel
	reply.JobType = jobTodo.jobType
	reply.FileName = jobTodo.filename
	reply.TaskId = jobTodo.id
	reply.MapNum = c.mapNum
	reply.NReduce= c.reduceNum
	return nil
}

func (c * Coordinator) JobCompleteNotice(args *CompleteNoticeArgs, reply *CompleteNoticeReply) error{
	c.JobDoneChannel <- args.JobId
	code := <- c.JobDoneChannel
	if code==0 {
		reply.Succeed=true
	}else {
		reply.Succeed=false
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if len(c.MapJobs) ==0 && len(c.ReduceJobs)==0 && len(c.InProgressJobs)==0{
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// log.Println("Coordinator initial begin...")
	// TimeChecker = time.NewTicker(time.second)
	// Your code here.
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: inputfiles...\n")
		os.Exit(1)
	}
	c.InProgressJobs = make(map[int]Job)
	c.MapAssignChannel = make(chan Job)
	c.JobDoneChannel = make(chan int)
	c.TimeoutChecker = time.NewTicker(time.Millisecond*500)
	c.reduceNum = nReduce
	jobId := 0
	for _, filename := range os.Args[1:] {
		c.MapJobs = append(c.MapJobs, Job{filename, jobId, "map", time.Now()})
		jobId++
	}
	c.mapNum = jobId
	c.phase = "map"

	// init reduce jobs
	reduceId := 0
	for reduceId < nReduce {
		c.ReduceJobs = append(c.ReduceJobs, Job{"", reduceId, "reduce", time.Now()})
		reduceId++
	}
	// log.Printf("Coordinator init success with %d map jobs and %d reduce jobs.\n", c.mapNum, nReduce)
	c.server()
	go c.ChannelOp()
	return &c
}
