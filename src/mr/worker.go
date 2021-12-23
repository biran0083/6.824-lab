package mr

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ReducerInput struct {
	ReducerId int
	Kv        KeyValue
}
type ByHash []ReducerInput

func (a ByHash) Len() int      { return len(a) }
func (a ByHash) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByHash) Less(i, j int) bool {
	if a[i].ReducerId == a[j].ReducerId {
		return a[i].Kv.Key < a[j].Kv.Key
	}
	return a[i].ReducerId < a[j].ReducerId
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

func process(work Work) {}

func GetWork() Work {
	req := GetWorkReq{}
	resp := Work{}
	ok := call("Coordinator.GetWork", &req, &resp)
	if !ok {
		log.Fatal("Something is wrong when calling Coordinator.GetWork")
	}
	return resp
}

func (work Work) doMap(
	mapf func(string, string) []KeyValue,
	filename string,
	NReduce int) {
	res := []string{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	ofiles := []*os.File{}
	encoders := []*json.Encoder{}

	for i := 0; i < NReduce; i++ {
		ofile, err := ioutil.TempFile(".", fmt.Sprintf("mr-tmp-%v", i))
		if err != nil {
			log.Fatal("Failed to create tmp file")
		}
		res = append(res, ofile.Name())
		ofiles = append(ofiles, ofile)
		encoders = append(encoders, json.NewEncoder(ofile))
	}

	kvs := mapf(filename, string(content))
	inputs := []ReducerInput{}
	for _, kv := range kvs {
		input := ReducerInput{ihash(kv.Key) % NReduce, kv}
		inputs = append(inputs, input)
	}
	sort.Sort(ByHash(inputs))
	i := 0
	for i < len(inputs) {
		j := i + 1
		for j < len(inputs) && inputs[j].ReducerId == inputs[i].ReducerId {
			j++
		}
		for k := i; k < j; k++ {
			encoders[inputs[i].ReducerId].Encode(inputs[k].Kv)
		}
		i = j
	}
	for _, ofile := range ofiles {
		ofile.Close()
	}

	req := WorkDoneReq{}
	req.Type = MAP
	req.MapperFilename = filename
	req.MapperOutput = res
	resp := WorkDoneResp{}
	ok := call("Coordinator.WorkDone", &req, &resp)
	if !ok {
		log.Fatal("Something is wrong when calling Coordinator.WorkDone")
	}
}

type HeapEntry struct {
	Kv      KeyValue
	Decoder *json.Decoder
}
type KVHeap []HeapEntry

func (h KVHeap) Len() int           { return len(h) }
func (h KVHeap) Less(i, j int) bool { return h[i].Kv.Key < h[j].Kv.Key }
func (h KVHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *KVHeap) Push(x interface{}) {
	*h = append(*h, x.(HeapEntry))
}

func (h *KVHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (work Work) doReduce(
	reducef func(string, []string) string,
	reducerId int,
	reducerInputFileNames []string) {
	oname := fmt.Sprintf("mr-out-%v", reducerId)
	ofile, _ := os.Create(oname)
	pq := KVHeap{}
	for _, fname := range reducerInputFileNames {
		ifile, err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		decoder := json.NewDecoder(ifile)
		if decoder.More() {
			var kv KeyValue
			decoder.Decode(&kv)
			pq = append(pq, HeapEntry{kv, decoder})
		}
	}
	heap.Init(&pq)
	lastKey := ""
	values := []string{}
	for pq.Len() > 0 {
		e := heap.Pop(&pq).(HeapEntry)
		if e.Decoder.More() {
			var newKv KeyValue
			e.Decoder.Decode(&newKv)
			heap.Push(&pq, HeapEntry{newKv, e.Decoder})

		}
		if lastKey != e.Kv.Key {
			if len(values) > 0 {
				fmt.Fprintf(ofile, "%v %v\n", lastKey, reducef(lastKey, values))
				values = []string{}
			}
			lastKey = e.Kv.Key
		}
		values = append(values, e.Kv.Value)
	}
	if len(values) > 0 {
		fmt.Fprintf(ofile, "%v %v\n", lastKey, reducef(lastKey, values))
	}
	ofile.Close()

	req := WorkDoneReq{}
	req.Type = REDUCE
	req.ReducerId = reducerId
	req.ReducerOutput = oname
	resp := WorkDoneResp{}
	ok := call("Coordinator.WorkDone", &req, &resp)
	if !ok {
		log.Fatal("Something is wrong when calling Coordinator.WorkDone")
	}
	for _, fname := range reducerInputFileNames {
		os.Remove(fname)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		work := GetWork()
		if work.Type == NONE {
			if work.HasMore {
				time.Sleep(time.Second)
				continue
			}
			break
		} else if work.Type == MAP {
			work.doMap(mapf, work.MapperInputFileName, work.NReduce)
		} else {
			work.doReduce(reducef, work.ReducerId, work.ReducerInputFileNames)
		}
	}
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
