package raft

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
)

var (
	errorLog                = log.New(os.Stdout, "\033[31m[error]\033[0m", log.LstdFlags|log.Lshortfile)
	infoLog                 = log.New(os.Stdout, "\033[34m[info]\033[0m", log.LstdFlags|log.Lshortfile)
	electionAndHeartbeatLog = log.New(os.Stdout, "\033[34m[info]\033[0m", log.LstdFlags|log.Lshortfile)
	loggers                 = []*log.Logger{errorLog, infoLog}
	raftLoggers             = []*log.Logger{electionAndHeartbeatLog}
	mu                      sync.Mutex
)

var (
	Error  = errorLog.Println
	Errorf = errorLog.Printf
	Info   = infoLog.Println
	Infof  = infoLog.Printf

	PartAInfo  = electionAndHeartbeatLog.Println
	PartAInfof = electionAndHeartbeatLog.Println
)

const (
	InfoLevel = iota
	ErrorLevel
	Disabled
)

func SetLevel(level int) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.SetOutput(os.Stdout)
	}
	if level > ErrorLevel {
		errorLog.SetOutput(ioutil.Discard)
	}
	if level > InfoLevel {
		infoLog.SetOutput(ioutil.Discard)
	}
}

func SetLogStage(discardLogs ...*log.Logger) {
	for _, discardLog := range discardLogs {
		discardLog.SetOutput(ioutil.Discard)
	}
}

func init() {
	SetLogStage(electionAndHeartbeatLog)
}
