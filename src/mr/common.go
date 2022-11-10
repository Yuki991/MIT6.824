package mr

import (
	"fmt"
	"reflect"
	"time"
)

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	Seq      int
	Phase    TaskPhase
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

// 用给定参数args调用f，超过timeout直接返回
// 忽略函数的返回值，返回值通过传入的参数来传递
func CallFunc(timeout time.Duration, f interface{}, args ...interface{}) bool {
	ch := make(chan bool)
	_f := reflect.ValueOf(f)
	_args := make([]reflect.Value, len(args))
	for i := range args {
		_args[i] = reflect.ValueOf(args[i])
	}

	go func() {
		_ = _f.Call(_args)
		ch <- true
	}()
	go func() {
		time.Sleep(timeout)
		ch <- false
	}()

	ok := <-ch
	go func() {
		<-ch
	}()

	return ok
}
