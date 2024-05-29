package queue

import (
	"errors"
	"sync"
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

func (q *Queue) Read() (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head.offset == q.tail.offset {
		return "", errors.New("no new messages")
	}
	res := q.head.val
	q.head = q.head.next
	return res, nil
}

func (q *Queue) Add(val string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.tail == nil {
		return errors.New("improperly initialized queue, tail is nil")
	}
	q.tail.val = val
	n := node{
		offset: q.tail.offset + 1,
	}
	q.tail.next = &n
	q.tail = &n
	return nil
}
