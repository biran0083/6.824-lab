package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

// Debugging
const Debug = 0

var file *os.File
var fm = sync.Mutex{}

func CreateNewLogFile() {
	if Debug != 2 {
		return
	}
	fm.Lock()
	var err error
	file, err = ioutil.TempFile("/tmp", "raft_log")
	if err != nil {
		log.Fatal("failed to open log file")
	}
	fmt.Println("opening log file ", file.Name())
	fm.Unlock()
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	} else if Debug == 2 {
		fm.Lock()
		fmt.Fprintf(file, format, a...)
		fm.Unlock()
	}
	return
}
