package mr

import "log"

const Debug = false

func Dprintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format+"\n", v...)
	}
}
