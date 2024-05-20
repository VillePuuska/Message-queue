package queue

import (
	"slices"
	"strconv"
	"sync"
	"testing"
)

const Iterations int = 1000000

func TestQueue(t *testing.T) {
	q := NewQueue()
	var wg sync.WaitGroup
	for i := 0; i < Iterations; i++ {
		wg.Add(1)
		go func(q *Queue, wg *sync.WaitGroup) {
			q.Add(strconv.Itoa(i))
			wg.Done()
		}(q, &wg)
	}
	wg.Wait()
	if q.tail.offset != int64(Iterations) {
		t.Log("After", Iterations, "q.Add() calls, incorrect offset:", q.tail.offset, q.tail.val)
		t.Fail()
	}
	vals := make([]int, Iterations)
	flag := false
	for i := 0; i < Iterations; i++ {
		val, err := q.Read()
		if err != nil {
			t.Log("Could not read all", Iterations, "messages. Stopped after", i)
			flag = true
			break
		}
		vals[i], _ = strconv.Atoi(val)
	}
	if flag {
		t.Fail()
	}
	val, err := q.Read()
	if err == nil {
		t.Log("Had extra value(s) in the queue:", val)
		t.Fail()
	}
	slices.Sort(vals)
	flag = false
	for i, val := range vals {
		if val != i {
			t.Log("Incorrect values in the list of all added values: index", i, "val", val)
			flag = true
			break
		}
	}
	if flag {
		t.Fail()
	}
}
