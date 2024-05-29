package queue

import (
	"slices"
	"strconv"
	"sync"
	"testing"
)

const Iterations int = 1000000 // Specifies how many calls are done in all concurrent tests

func TestQueue(t *testing.T) {
	t.Run("concurrent Add() and Read()", func(t *testing.T) {
		q := NewQueue()
		var wg sync.WaitGroup

		// Test that tail offset is corerct after concurrent Add() calls
		for i := 0; i < Iterations; i++ {
			wg.Add(1)
			go func(q *Queue, wg *sync.WaitGroup) {
				q.Add(strconv.Itoa(i))
				wg.Done()
			}(q, &wg)
		}
		wg.Wait()
		if q.tail.offset != int64(Iterations) {
			t.Errorf("After %d Add() calls, incorrect offset: was %d, tail value was %q", Iterations, q.tail.offset, q.tail.val)
		}

		// Test that we can Read() all values
		vals := make([]int, Iterations)
		for i := 0; i < Iterations; i++ {
			val, err := q.Read()
			if err != nil {
				t.Logf("Could not read all %d messages. Stopped after %d", Iterations, i)
				t.FailNow()
			}
			vals[i], _ = strconv.Atoi(val)
		}

		// Test that we get an error when we Read() after clearing the queue
		val, err := q.Read()
		if err == nil {
			t.Log("Had extra value(s) in the queue:", val)
			t.Fail()
		}

		// Test that the values Read() from the queue are correct, i.e. 0..Iterations-1
		slices.Sort(vals)
		for i, val := range vals {
			if val != i {
				t.Logf("Incorrect values in the list of all added values: index %d, value %d", i, val)
				if Iterations <= int(1e4) {
					t.Logf("Entire sorted list of Read() values: %v", vals)
				}
				t.FailNow()
			}
		}
	})
}
