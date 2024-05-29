package queue

import (
	"errors"
	"sync"
)

var (
	ErrQueueIsEmpty               = errors.New("queue is empty")
	ErrImproperlyInitializedQueue = errors.New("improperly initialized queue, tail is nil")
	ErrUnimplementedMethod        = errors.New("unimplemented")
)

type node struct {
	val    string
	offset int64
	next   *node
}

type Queue struct {
	head *node
	tail *node
	mu   sync.Mutex
}

func NewQueue() *Queue {
	n := node{}
	res := Queue{
		head: &n,
		tail: &n,
	}
	return &res
}

func (q *Queue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.head.offset == q.tail.offset
}

func (q *Queue) Read() (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head.offset == q.tail.offset {
		return "", ErrQueueIsEmpty
	}
	res := q.head.val
	q.head = q.head.next
	return res, nil
}

func (q *Queue) Add(val string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.tail == nil {
		return ErrImproperlyInitializedQueue
	}
	q.tail.val = val
	n := node{
		offset: q.tail.offset + 1,
	}
	q.tail.next = &n
	q.tail = &n
	return nil
}

func (q *Queue) PeekNext() (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head.offset == q.tail.offset {
		return "", ErrQueueIsEmpty
	}
	return q.head.val, nil
}

// TODO
func (q *Queue) PeekLast() (string, error) {
	return "", ErrUnimplementedMethod
}

func (q *Queue) Length() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.tail.offset - q.head.offset
}
