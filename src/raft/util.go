package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Debugging
const Debug = 0

var file *os.File

func CreateNewLogFile() {
	if Debug != 2 {
		return
	}
	var err error
	file, err = ioutil.TempFile("/tmp", "raft_log")
	if err != nil {
		log.Fatal("failed to open log file")
	}
	fmt.Println("opening log file ", file.Name())
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	} else if Debug == 2 {
		fmt.Fprintf(file, format, a...)
	}
	return
}
