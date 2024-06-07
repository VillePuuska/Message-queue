package queue

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/VillePuuska/Message-queue/internal/testutil"
)

const Iterations int = int(1e5) // Specifies how many calls are done in all concurrent tests

func TestQueue(t *testing.T) {
	t.Run("test calling exported methods on a manually initialized Queue return correct errors", func(t *testing.T) {
		q := Queue[string]{}

		_, err := q.IsEmpty()
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "IsEmpty() on a manually created queue returned incorrect error", false)

		_, err = q.Length()
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "Length() on a manually created queue returned incorrect error", false)

		err = q.Add("asd")
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "Add() on a manually created queue returned incorrect error", false)

		err = q.AddMany([]string{"asd"})
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "AddMany() on a manually created queue returned incorrect error", false)

		_, err = q.Read()
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "Read() on a manually created queue returned incorrect error", false)

		_, err = q.ReadMany(10)
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "ReadMany() on a manually created queue returned incorrect error", false)

		_, err = q.PeekNext()
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "PeekNext() on a manually created queue returned incorrect error", false)

		_, err = q.Cleanup()
		testutil.AssertEqual(t, err, ErrImproperlyInitializedQueue, "Cleanup() on a manually created queue returned incorrect error", false)
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
		testutil.AssertEqual(t, q.tail.message.Offset, uint64(Iterations), fmt.Sprintf("After %d Add() calls, incorrect offset", Iterations), false)

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
			testutil.AssertEqual(t, err, nil, "Read() returned an error while concurrently reading the queue", true)
		}

		// Test that we get the correct error when we Read() after clearing the queue
		_, err := q.Read()
		testutil.AssertEqual(t, err, ErrQueueIsEmpty, "Read() on an empty queue returned incorrect error", false)

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
		testutil.AssertEqual(t, err, ErrQueueIsEmpty, "queue is empty and ReadMany(1) returned an incorrect error", false)

		expected := []string{
			"asd",
			"dsa",
		}
		for _, s := range expected {
			q.Add(s)
		}

		_, err = q.ReadMany(-2)
		testutil.AssertEqual(t, err, ErrInvalidLimit, "ReadMany(-2) returned an incorrect error", false)

		got, err := q.ReadMany(2)
		gotVals := make([]string, len(got))
		for i, msg := range got {
			gotVals[i] = msg.Val
		}
		testutil.AssertEqual(t, err, nil, "queue has 2 messages but ReadMany(2) returned an error", false)
		testutil.AssertDeepEqual(t, gotVals, expected, "ReadMany(2) returned an incorrect result", false)

		expected = make([]string, Iterations)
		for i := 0; i < Iterations; i++ {
			expected[i] = strconv.Itoa(i)
		}
		err = q.AddMany(expected)
		testutil.AssertEqual(t, err, nil, "AddMany returned an unexpected error", false)
		got, err = q.ReadMany(Iterations)
		gotVals = make([]string, len(got))
		for i, msg := range got {
			gotVals[i] = msg.Val
		}
		testutil.AssertEqual(t, err, nil, fmt.Sprintf("queue has %d messages but ReadMany(%d) returned an error", Iterations, Iterations), false)
		testutil.AssertDeepEqual(t, gotVals, expected, fmt.Sprintf("ReadMany(%d) returned incorrect result", Iterations), false)
	})

	t.Run("test IsEmpty()", func(t *testing.T) {
		q := NewQueue[string]()

		if flag, _ := q.IsEmpty(); !flag {
			t.Error("freshly initialized queue, but IsEmpty() returned false, should return true")
		}

		q.Add("asd")
		if flag, _ := q.IsEmpty(); flag {
			t.Error("queue has one element, but IsEmpty() returned true, should return false")
		}

		_, err := q.Read()
		testutil.AssertEqual(t, err, nil, "queue has an element, but Read() returned an error", false)

		if flag, _ := q.IsEmpty(); !flag {
			t.Error("last element was read from queue, but IsEmpty() returned false, should return true")
		}

		q.Add("")
		if flag, _ := q.IsEmpty(); flag {
			t.Error("queue has one element, but IsEmpty() returned true, should return false")
		}
	})

	t.Run("test PeekNext()", func(t *testing.T) {
		q := NewQueue[string]()

		_, err := q.PeekNext()
		testutil.AssertEqual(t, err, ErrQueueIsEmpty, "PeekNext() on an empty queue returned incorrect error", false)

		expected := "asd"
		q.Add(expected)
		got, err := q.PeekNext()
		testutil.AssertEqual(t, err, nil, "queue has a message, but PeekNext() returned an error", false)
		testutil.AssertEqual(t, got.Val, expected, "PeekNext() incorrect result", false)

		secondExpected := "aaaa"
		q.Add(secondExpected)

		got, err = q.PeekNext()
		testutil.AssertEqual(t, err, nil, "queue has a message, but PeekNext() returned an error", false)
		testutil.AssertEqual(t, got.Val, expected, "PeekNext() incorrect result", false)

		_, _ = q.Read()
		got, err = q.PeekNext()
		testutil.AssertEqual(t, err, nil, "queue has a message, but PeekNext() returned an error", false)
		testutil.AssertEqual(t, got.Val, secondExpected, "PeekNext() incorrect result", false)

		_, _ = q.Read()
		_, err = q.PeekNext()
		testutil.AssertEqual(t, err, ErrQueueIsEmpty, "all messages have been Read() from queue, but PeekNext() did not return an error", false)
	})

	t.Run("test Length()", func(t *testing.T) {
		q := NewQueue[string]()

		got, _ := q.Length()
		testutil.AssertEqual(t, got, 0, "freshly initialized queue, incorrect Length()", false)

		q.Add("asd")
		got, _ = q.Length()
		testutil.AssertEqual(t, got, 1, "1 message in Queue, incorrect Length()", false)

		q.Add("aaaaaa")
		got, _ = q.Length()
		testutil.AssertEqual(t, got, 2, "2 messages in Queue, incorrect Length()", false)

		_, _ = q.Read()
		got, _ = q.Length()
		testutil.AssertEqual(t, got, 1, "1 message in Queue, incorrect Length()", false)

		_, _ = q.Read()
		got, _ = q.Length()
		testutil.AssertEqual(t, got, 0, "empty Queue, incorrect Length()", false)

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
		got, _ = q.Length()
		testutil.AssertEqual(t, got, uint64(Iterations), fmt.Sprintf("After %d Add() calls, incorrect Length()", Iterations), false)
	})

	t.Run("test overflowing offsets", func(t *testing.T) {
		q := NewQueue[string]()

		expected := "asd"
		q.Add(expected)
		q.head.message.Offset = math.MaxUint64 - 1
		q.tail.message.Offset = math.MaxUint64

		got, _ := q.Length()
		testutil.AssertEqual(t, got, 1, "1 message in Queue, offset overflowing, incorrect Length()", false)

		flag, err := q.IsEmpty()
		testutil.AssertEqual(t, err, nil, "IsEmpty() returned an unexpected error", false)
		testutil.AssertEqual(t, flag, false, "IsEmpty() returned an incorrect value after overflowing offset", false)

		msg, err := q.Read()
		testutil.AssertEqual(t, err, nil, "Read() returned an error when there was a message", false)
		testutil.AssertEqual(t, msg.Val, expected, "Read() returned a message with incorrect value", false)

		flag, err = q.IsEmpty()
		testutil.AssertEqual(t, err, nil, "IsEmpty() returned an unexpected error", false)
		testutil.AssertEqual(t, flag, true, "IsEmpty() returned an incorrect value after overflowing offset", false)

		q.Add(expected)

		testutil.AssertEqual(t, q.tail.message.Offset, 0, "q.tail.message.Offset is incorrect after overflowing", false)

		got, _ = q.Length()
		testutil.AssertEqual(t, got, 1, "1 message in Queue, offset overflowing, incorrect Length()", false)

		flag, err = q.IsEmpty()
		testutil.AssertEqual(t, err, nil, "IsEmpty() returned an unexpected error", false)
		testutil.AssertEqual(t, flag, false, "IsEmpty() returned an incorrect value after overflowing offset", false)

		msg, err = q.Read()
		testutil.AssertEqual(t, err, nil, "Read() returned an error when there was a message", false)
		testutil.AssertEqual(t, msg.Val, expected, "Read() returned a message with incorrect value", false)

		flag, err = q.IsEmpty()
		testutil.AssertEqual(t, err, nil, "IsEmpty() returned an unexpected error", false)
		testutil.AssertEqual(t, flag, true, "IsEmpty() returned an incorrect value after overflowing offset", false)

		q.Add(expected)

		testutil.AssertEqual(t, q.tail.message.Offset, 1, "q.tail.message.Offset is incorrect after overflowing", false)

		got, _ = q.Length()
		testutil.AssertEqual(t, got, 1, "1 message in Queue, offset overflowing, incorrect Length()", false)

		flag, err = q.IsEmpty()
		testutil.AssertEqual(t, err, nil, "IsEmpty() returned an unexpected error", false)
		testutil.AssertEqual(t, flag, false, "IsEmpty() returned an incorrect value after overflowing offset", false)

		msg, err = q.Read()
		testutil.AssertEqual(t, err, nil, "Read() returned an error when there was a message", false)
		testutil.AssertEqual(t, msg.Val, expected, "Read() returned a message with incorrect value", false)

		flag, err = q.IsEmpty()
		testutil.AssertEqual(t, err, nil, "IsEmpty() returned an unexpected error", false)
		testutil.AssertEqual(t, flag, true, "IsEmpty() returned an incorrect value after overflowing offset", false)
	})
}

func TestQueueConfig(t *testing.T) {
	t.Run("test QueueConfig parameter validations", func(t *testing.T) {
		config := DefaultConfig()
		_, err := config.WithRetentionCount(0)
		testutil.AssertEqual(t, err, ErrInvalidConfig, "config.WithRetentionCount(0) returned an incorrect error", false)
		_, err = config.WithRetentionTime(time.Second * 0)
		testutil.AssertEqual(t, err, ErrInvalidConfig, "config.WithRetentionTime(time.Second * 0) returned an incorrect error", false)
		_, err = config.WithRetentionTime(-time.Second)
		testutil.AssertEqual(t, err, ErrInvalidConfig, "config.WithRetentionTime(-time.Second) returned an incorrect error", false)
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
			initialLength uint64
			cleanupAmount uint64
			finalLength   uint64
		}{
			{"default config", queueDefaultConfig, 2, 0, 2},
			{"low retention count", queueLowRetentionCount, 2, 1, 1},
			{"low retention time", queueLowRetentionTime, 2, 2, 0},
			{"low retention count and auto cleanup", queueLowRetentionCountAutoCleanup, 1, 0, 1},
		}

		for _, test := range testTable {
			got, _ := test.queue.Length()
			testutil.AssertEqual(t, got, test.initialLength, fmt.Sprintf("test %q has incorrect initial length", test.name), false)

			got, _ = test.queue.Cleanup()
			testutil.AssertEqual(t, got, test.cleanupAmount, fmt.Sprintf("test %q Cleanup() deleted incorrect amount", test.name), false)

			got, _ = test.queue.Length()
			testutil.AssertEqual(t, got, test.finalLength, fmt.Sprintf("test %q has incorrect final length", test.name), false)
		}

	})
}
