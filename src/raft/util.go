package raft

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
)

// Debugging
var (
	Debug = false
	Error = false
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		// Get the caller information
		_, file, line, ok := runtime.Caller(1) // Skip 1 level to get the caller
		if ok {
			// Prefix the log with the caller's file and line number
			format = fmt.Sprintf("%s:%d: %s", filepath.Base(file), line, format)
		}
		log.Printf(format, a...)
	}
	return
}

func EPrintf(format string, a ...interface{}) (n int, err error) {
	if Error {
		_, file, line, ok := runtime.Caller(1) // Skip 1 level to get the caller
		if ok {
			// Prefix the log with the caller's file and line number
			format = fmt.Sprintf("%s:%d: %s", filepath.Base(file), line, format)
		}
		log.Printf(format, a...)
	}
	return
}
