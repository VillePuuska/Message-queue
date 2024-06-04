package queue

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

const Iterations int = int(1e5) // Specifies how many calls are done in all concurrent tests

func assertError(t testing.TB, got, expected error, explanation string, failnow bool) {
	t.Helper()
	if got != expected {
		t.Errorf("%v: got %q, expected %q", explanation, got, expected)
		if failnow {
			t.FailNow()
		}
	}
}

func TestQueue(t *testing.T) {
	t.Run("test Add() on manually initialized Queue returns correct error", func(t *testing.T) {
		q := Queue[string]{}
		err := q.Add("asd")
		assertError(t, err, ErrImproperlyInitializedQueue, "Add() on a manually created queue returned incorrect error", false)
	})

	t.Run("concurrent Add() and Read()", func(t *testing.T) {
		q := NewQueue[int]()
		var wg sync.WaitGroup

		// Test that tail offset is correct after concurrent Add() calls
		for i := 0; i < Iterations; i++ {
			wg.Add(1)
			go func(q *Queue[int], wg *sync.WaitGroup) {
				q.Add(i)
				wg.Done()
			}(q, &wg)
		}
		wg.Wait()
		if q.tail.message.Offset != int64(Iterations) {
			t.Errorf("After %d Add() calls, incorrect offset: was %d, tail value was %q", Iterations, q.tail.message.Offset, q.tail.message.Val)
		}

		// Test that we can concurrently Read() all values
		vals := make([]int, Iterations)
		errs := make([]error, Iterations)
		for i := 0; i < Iterations; i++ {
			wg.Add(1)
			go func(index int, wg *sync.WaitGroup) {
				msg, err := q.Read()
				vals[index] = msg.Val
				errs[index] = err
				wg.Done()
			}(i, &wg)
		}
		wg.Wait()
		for _, err := range errs {
			assertError(t, err, nil, "Read() returned an error while concurrently reading the queue", true)
		}

		// Test that we get the correct error when we Read() after clearing the queue
		_, err := q.Read()
		assertError(t, err, ErrQueueIsEmpty, "Read() on an empty queue returned incorrect error", false)

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

	t.Run("test AddMany and ReadMany", func(t *testing.T) {
		q := NewQueue[string]()

		_, err := q.ReadMany(1)
		assertError(t, err, ErrQueueIsEmpty, "queue is empty and ReadMany(1) returned an incorrect error", false)

		expected := []string{
			"asd",
			"dsa",
		}
		for _, s := range expected {
			q.Add(s)
		}

		_, err = q.ReadMany(-2)
		assertError(t, err, ErrInvalidLimit, "ReadMany(-2) returned an incorrect error", false)

		got, err := q.ReadMany(2)
		gotVals := make([]string, len(got))
		for i, msg := range got {
			gotVals[i] = msg.Val
		}
		assertError(t, err, nil, "queue has 2 messages but ReadMany(2) returned an error", false)
		if !reflect.DeepEqual(gotVals, expected) {
			t.Errorf("ReadMany(2) returned an incorrect result: got %v, expected %v", got, expected)
		}

		expected = make([]string, Iterations)
		for i := 0; i < Iterations; i++ {
			expected[i] = strconv.Itoa(i)
		}
		err = q.AddMany(expected)
		assertError(t, err, nil, "AddMany returned an unexpected error", false)
		got, err = q.ReadMany(Iterations)
		gotVals = make([]string, len(got))
		for i, msg := range got {
			gotVals[i] = msg.Val
		}
		assertError(t, err, nil, fmt.Sprintf("queue has %d messages but ReadMany(%d) returned an error", Iterations, Iterations), false)
		if !reflect.DeepEqual(got, expected) {
			for i := range got {
				if gotVals[i] == expected[i] {
					continue
				}
				t.Errorf("ReadMany(%d) returned incorrect result, first difference at index %d: got %q, expected %q", Iterations, i, got[i], expected[i])
				break
			}
		}
	})

	t.Run("test IsEmpty()", func(t *testing.T) {
		q := NewQueue[string]()

		if !q.IsEmpty() {
			t.Error("freshly initialized queue, but IsEmpty() returned false, should return true")
		}

		q.Add("asd")
		if q.IsEmpty() {
			t.Error("queue has one element, but IsEmpty() returned true, should return false")
		}

		_, err := q.Read()
		assertError(t, err, nil, "queue has an element, but Read() returned an error", false)

		if !q.IsEmpty() {
			t.Error("last element was read from queue, but IsEmpty() returned false, should return true")
		}

		q.Add("")
		if q.IsEmpty() {
			t.Error("queue has one element, but IsEmpty() returned true, should return false")
		}
	})

	t.Run("test PeekNext()", func(t *testing.T) {
		q := NewQueue[string]()

		_, err := q.PeekNext()
		assertError(t, err, ErrQueueIsEmpty, "PeekNext() on an empty queue returned incorrect error", false)

		expected := "asd"
		q.Add(expected)
		got, err := q.PeekNext()
		assertError(t, err, nil, "queue has a message, but PeekNext() returned an error", false)
		if got.Val != expected {
			t.Errorf("PeekNext() returned %q, expected %q", got, expected)
		}

		secondExpected := "aaaa"
		q.Add(secondExpected)

		got, err = q.PeekNext()
		assertError(t, err, nil, "queue has a message, but PeekNext() returned an error", false)
		if got.Val != expected {
			t.Errorf("PeekNext() returned %q, expected %q", got, expected)
		}

		_, _ = q.Read()
		got, err = q.PeekNext()
		assertError(t, err, nil, "queue has a message, but PeekNext() returned an error", false)
		if got.Val != secondExpected {
			t.Errorf("PeekNext() returned %q, expected %q", got, secondExpected)
		}

		_, _ = q.Read()
		_, err = q.PeekNext()
		assertError(t, err, ErrQueueIsEmpty, "all messages have been Read() from queue, but PeekNext() did not return an error", false)
	})

	t.Run("test Length()", func(t *testing.T) {
		q := NewQueue[string]()

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
			go func(q *Queue[string], wg *sync.WaitGroup) {
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

func TestQueueConfig(t *testing.T) {
	t.Run("test QueueConfig parameter validations", func(t *testing.T) {
		config := DefaultConfig()
		_, err := config.WithRetentionCount(0)
		assertError(t, err, ErrInvalidConfig, "config.WithRetentionCount(0) returned an incorrect error", false)
		_, err = config.WithRetentionCount(-1)
		assertError(t, err, ErrInvalidConfig, "config.WithRetentionCount(-1) returned an incorrect error", false)
		_, err = config.WithRetentionTime(time.Second * 0)
		assertError(t, err, ErrInvalidConfig, "config.WithRetentionTime(time.Second * 0) returned an incorrect error", false)
		_, err = config.WithRetentionTime(-time.Second)
		assertError(t, err, ErrInvalidConfig, "config.WithRetentionTime(-time.Second) returned an incorrect error", false)
	})

	t.Run("test Queue cleanups with QueueConfig parameters", func(t *testing.T) {
		queueDefaultConfig := NewQueue[string]()
		configLowRetentionCount, _ := queueDefaultConfig.config.WithRetentionCount(1)
		queueLowRetentionCount := NewQueueWithConfig[string](configLowRetentionCount)
		configLowRetentionTime, _ := queueDefaultConfig.config.WithRetentionTime(time.Nanosecond)
		queueLowRetentionTime := NewQueueWithConfig[string](configLowRetentionTime)
		configLowRetentionCountAutoCleanup, _ := configLowRetentionCount.WithAutoCleanup(true)
		queueLowRetentionCountAutoCleanup := NewQueueWithConfig[string](configLowRetentionCountAutoCleanup)

		vals := []string{
			"asd",
			"ddd",
		}
		_ = queueDefaultConfig.AddMany(vals)
		_ = queueLowRetentionCount.AddMany(vals)
		_ = queueLowRetentionTime.AddMany(vals)
		_ = queueLowRetentionCountAutoCleanup.AddMany(vals)
		time.Sleep(time.Nanosecond * 5)

		testTable := []struct {
			name          string
			queue         *Queue[string]
			initialLength int64
			cleanupAmount int64
			finalLength   int64
		}{
			{"default config", queueDefaultConfig, 2, 0, 2},
			{"low retention count", queueLowRetentionCount, 2, 1, 1},
			{"low retention time", queueLowRetentionTime, 2, 2, 0},
			{"low retention count and auto cleanup", queueLowRetentionCountAutoCleanup, 1, 0, 1},
		}

		for _, test := range testTable {
			got := test.queue.Length()
			if got != test.initialLength {
				t.Errorf("test %q has incorrect initial length: got %d, expected %d", test.name, got, test.initialLength)
			}
			got = test.queue.Cleanup()
			if got != test.cleanupAmount {
				t.Errorf("test %q Cleanup() deleted incorrect amount: got %d, expected %d", test.name, got, test.cleanupAmount)
			}
			got = test.queue.Length()
			if got != test.finalLength {
				t.Errorf("test %q has incorrect final length: got %d, expected %d", test.name, got, test.finalLength)
			}
		}

	})
}
