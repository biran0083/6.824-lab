package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	filenames           []string
	mapperStartingTime  map[string]int64
	mapperOutputs       map[string][]string
	reducerStartingTime map[int]int64
	reducerOutputs      map[int]string
	NReduce             int
	mutex               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Coordinator) ShouldScheduleMapWork(filename string) bool {
	_, finished := m.mapperOutputs[filename]
	startingTime, scheduled := m.mapperStartingTime[filename]
	if !scheduled {
		return true
	}
	if finished {
		return false
	}
	now := time.Now()
	return now.Unix()-startingTime > 10
}

func (m *Coordinator) ShouldScheduleReducerWork(i int) bool {
	_, finished := m.reducerOutputs[i]
	startingTime, scheduled := m.reducerStartingTime[i]
	if !scheduled {
		return true
	}
	if finished {
		return false
	}
	now := time.Now()
	return now.Unix()-startingTime > 10
}

func (m *Coordinator) GetWork(req *GetWorkReq, resp *Work) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	resp.Type = NONE
	resp.HasMore = true
	if len(m.reducerOutputs) == m.NReduce {
		resp.HasMore = false
		return nil
	}
	for _, filename := range m.filenames {
		if m.ShouldScheduleMapWork(filename) {
			resp.Type = MAP
			resp.MapperInputFileName = filename
			resp.NReduce = m.NReduce
			m.mapperStartingTime[filename] = time.Now().Unix()
			break
		}
	}
	if resp.Type == NONE && len(m.mapperOutputs) == len(m.filenames) {
		for i := 0; i < m.NReduce; i++ {
			if m.ShouldScheduleReducerWork(i) {
				resp.Type = REDUCE
				resp.ReducerId = i
				for _, outputFile := range m.mapperOutputs {
					resp.ReducerInputFileNames = append(resp.ReducerInputFileNames, outputFile[i])
				}
				m.reducerStartingTime[i] = time.Now().Unix()
				break
			}
		}
	}
	return nil
}

func (m *Coordinator) WorkDone(req *WorkDoneReq, resp *WorkDoneResp) error {
	m.mutex.Lock()
	if req.Type == MAP {
		m.mapperOutputs[req.MapperFilename] = req.MapperOutput
	} else {
		m.reducerOutputs[req.ReducerId] = req.ReducerOutput
	}
	m.mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Coordinator) server() {
	rpc.Register(m)
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
func (m *Coordinator) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.reducerOutputs) == m.NReduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, NReduce int) *Coordinator {
	m := Coordinator{}
	m.filenames = os.Args[1:]
	m.NReduce = 3
	m.mapperOutputs = make(map[string][]string)
	m.mapperStartingTime = make(map[string]int64)
	m.reducerStartingTime = make(map[int]int64)
	m.reducerOutputs = make(map[int]string)

	m.server()
	return &m
}
