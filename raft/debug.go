package raft

import "log"

var Debug = false

func debugf(format string, args ...any) {
	if Debug {
		log.Printf(format, args...)
	}
}
