package queue

import (
	"slices"
	"strconv"
	"sync"
	"testing"
)

const Iterations int = 1000000 // Specifies how many calls are done in all concurrent tests

func TestQueue(t *testing.T) {
	t.Run("test Add() on manually initialized Queue returns correct error", func(t *testing.T) {
		q := Queue{}
		err := q.Add("asd")
		if err != ErrImproperlyInitializedQueue {
			t.Errorf("Add() on a manually created queue returned incorrect error %q, expected %q", err, ErrImproperlyInitializedQueue)
		}
	})

	t.Run("concurrent Add() and Read()", func(t *testing.T) {
		q := NewQueue()
		var wg sync.WaitGroup

		// Test that tail offset is correct after concurrent Add() calls
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

		// Test that we get the correct error when we Read() after clearing the queue
		val, err := q.Read()
		if err == nil {
			t.Errorf("Had extra value(s) in the queue: %q", val)
		}
		if err != ErrQueueIsEmpty {
			t.Errorf("Read() on an empty queue returned incorrect error %q, expected %q", err, ErrQueueIsEmpty)
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

	t.Run("test IsEmpty()", func(t *testing.T) {
		q := NewQueue()

		if !q.IsEmpty() {
			t.Error("freshly initialized queue, but IsEmpty() returned false, should return true")
		}

		q.Add("asd")
		if q.IsEmpty() {
			t.Error("queue has one element, but IsEmpty() returned true, should return false")
		}

		_, err := q.Read()
		if err != nil {
			t.Errorf("queue has an element, but Read() returned an error: %q", err)
		}

		if !q.IsEmpty() {
			t.Error("last element was read from queue, but IsEmpty() returned false, should return true")
		}

		q.Add("")
		if q.IsEmpty() {
			t.Error("queue has one element, but IsEmpty() returned true, should return false")
		}
	})

	t.Run("test PeekNext()", func(t *testing.T) {
		q := NewQueue()

		_, err := q.PeekNext()
		if err == nil {
			t.Error("freshly initialized queue, but PeekNext() did not return an error, expected an error")
		}
		if err != ErrQueueIsEmpty {
			t.Errorf("PeekNext() on an empty queue returned incorrect error %q, expected %q", err, ErrQueueIsEmpty)
		}

		expected := "asd"
		q.Add(expected)
		got, err := q.PeekNext()
		if err != nil {
			t.Error("queue has a message, but PeekNext() returned an error, expected 'nil' error")
		}
		if got != expected {
			t.Errorf("PeekNext() returned %q, expected %q", got, expected)
		}

		secondExpected := "aaaa"
		q.Add(secondExpected)

		got, err = q.PeekNext()
		if err != nil {
			t.Error("queue has a message, but PeekNext() returned an error, expected 'nil' error")
		}
		if got != expected {
			t.Errorf("PeekNext() returned %q, expected %q", got, expected)
		}

		_, _ = q.Read()
		got, err = q.PeekNext()
		if err != nil {
			t.Error("queue has a message, but PeekNext() returned an error, expected 'nil' error")
		}
		if got != secondExpected {
			t.Errorf("PeekNext() returned %q, expected %q", got, secondExpected)
		}

		_, _ = q.Read()
		_, err = q.PeekNext()
		if err == nil {
			t.Error("all messages have been Read() from queue, but PeekNext() did not return an error, expected an error")
		}
	})

	t.Run("test Length()", func(t *testing.T) {
		q := NewQueue()

		got := q.Length()
		if got != 0 {
			t.Errorf("freshly initialized queue, but Length() returned %d, expected 0", got)
		}

		q.Add("asd")
		got = q.Length()
		if got != 1 {
			t.Errorf("Length() returned %d, expected 1", got)
		}

		q.Add("aaaaaa")
		got = q.Length()
		if got != 2 {
			t.Errorf("Length() returned %d, expected 2", got)
		}

		_, _ = q.Read()
		got = q.Length()
		if got != 1 {
			t.Errorf("Length() returned %d, expected 1", got)
		}

		_, _ = q.Read()
		got = q.Length()
		if got != 0 {
			t.Errorf("Length() returned %d, expected 0", got)
		}

		// Test that Length() is correct after concurrent Add() calls
		var wg sync.WaitGroup
		for i := 0; i < Iterations; i++ {
			wg.Add(1)
			go func(q *Queue, wg *sync.WaitGroup) {
				q.Add("asd")
				wg.Done()
			}(q, &wg)
		}
		wg.Wait()
		got = q.Length()
		if got != int64(Iterations) {
			t.Errorf("After %d Add() calls, Length() returned %d, expected %d", Iterations, got, Iterations)
		}
	})
}
