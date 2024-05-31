package queue

import (
	"errors"
	"math"
	"sync"
)

var (
	ErrQueueIsEmpty               = errors.New("queue is empty")
	ErrImproperlyInitializedQueue = errors.New("improperly initialized queue, tail is nil")
	ErrUnimplementedMethod        = errors.New("unimplemented")
	ErrInvalidLimit               = errors.New("limit must be positive")
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

func (q *Queue) Read() (string, error) {
	res, err := q.ReadMany(1)
	if err != nil {
		return "", err
	}
	return res[0], nil
}

func (q *Queue) ReadMany(limit int) ([]string, error) {
	if limit <= 0 {
		return []string{}, ErrInvalidLimit
	}
	length := q.Length()
	if length <= math.MaxInt {
		limit = min(limit, int(length))
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	res := make([]string, limit)
	if q.head.offset == q.tail.offset {
		return []string{}, ErrQueueIsEmpty
	}
	node := q.head
	for i := 0; i < limit; i++ {
		res[i] = node.val
		node = node.next
	}
	q.head = node
	return res, nil
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
