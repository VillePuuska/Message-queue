// Package queue implements a simple in-memory message queue.
//
// The package can be imported to a project and used with the provided API.
// TODO: Alternatively, a REST API over HTTP is provided to use Queues as a
// separate service.
//
// The queue is implemented as the type Queue. A Queue should never
// be initialized directly; always use the function NewQueue.
package queue

import (
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrQueueIsEmpty               = errors.New("queue is empty")
	ErrImproperlyInitializedQueue = errors.New("improperly initialized queue, tail is nil")
	ErrUnimplementedMethod        = errors.New("unimplemented")
	ErrInvalidLimit               = errors.New("limit must be positive")
	ErrInvalidConfig              = errors.New("invalid configuration parameter")
)

// Message type contains the actual message/string stored in a Queue
// and related metadata (offset, logAppendTime).
type Message struct {
	Val           string
	Offset        int64
	LogAppendTime time.Time
}

// QueueConfig type contains all the configuration options
// for a Queue.
type QueueConfig struct {
	name           string
	retentionCount int64
	retentionTime  time.Duration
	autoCleanup    bool
}

// Linked list node. Used for Queue internals.
type node struct {
	message *Message
	next    *node
}

// Queue is a message queue.
// Queue methods are safe to use concurrently in multiple goroutines.
//
// When messages are Read() from a Queue, they are discarded. There is no
// retention after a message has been read. It is possible to get a single
// message without discarding/consuming it with the method PeekNext().
//
// NOTE: never create a Queue directly; use NewQueue instead.
type Queue struct {
	head   *node
	tail   *node
	config QueueConfig
	mu     sync.Mutex
}

// Function to create a default QueueConfig.
// To change the configuration, use the WithFoo methods.
// To create a Queue with a specific QueueConfig, use the NewQueueWithConfig function.
func DefaultConfig() QueueConfig {
	config := QueueConfig{
		name:           "",
		retentionCount: int64(1e9),
		retentionTime:  time.Hour * 24,
		autoCleanup:    false,
	}
	return config
}

// Returns a new QueueConfig with the name changed and other parameters kept the same.
func (config QueueConfig) WithName(name string) (QueueConfig, error) {
	config.name = name
	return config, nil
}

// Returns a new QueueConfig with the retentionCount changed and other parameters kept the same.
func (config QueueConfig) WithRetentionCount(retentionCount int64) (QueueConfig, error) {
	if retentionCount <= 0 {
		return config, ErrInvalidConfig
	}
	config.retentionCount = retentionCount
	return config, nil
}

// Returns a new QueueConfig with the retentionTime changed and other parameters kept the same.
func (config QueueConfig) WithRetentionTime(retentionTime time.Duration) (QueueConfig, error) {
	if retentionTime <= 0 {
		return config, ErrInvalidConfig
	}
	config.retentionTime = retentionTime
	return config, nil
}

// Returns a new QueueConfig with the name changed and other parameters kept the same.
func (config QueueConfig) WithAutoCleanup(autoCleanup bool) (QueueConfig, error) {
	config.autoCleanup = autoCleanup
	return config, nil
}

// Function to initialize a new empty Queue with the default config.
func NewQueue() *Queue {
	config := DefaultConfig()
	msg := Message{}
	n := node{
		message: &msg,
	}
	res := Queue{
		head:   &n,
		tail:   &n,
		config: config,
	}
	return &res
}

// Function to initialize a new empty Queue with the default config.
func NewQueueWithConfig(config QueueConfig) *Queue {
	msg := Message{}
	n := node{
		message: &msg,
	}
	res := Queue{
		head:   &n,
		tail:   &n,
		config: config,
	}
	return &res
}

func (q *Queue) GetConfig() QueueConfig {
	return q.config
}

// Checks if the Queue is empty.
func (q *Queue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.config.autoCleanup {
		q.cleanup()
	}

	return q.isEmptyNoLock()
}

// Internal method to check if the Queue is empty.
// Does not lock the Queue; assumes that the Queue is already
// locked when this function is called.
func (q *Queue) isEmptyNoLock() bool {
	return q.head.message.Offset == q.tail.message.Offset
}

// Returns the length of the Queue.
func (q *Queue) Length() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.config.autoCleanup {
		q.cleanup()
	}

	return q.lengthNoLock()
}

// Internal method to get the length of the Queue.
// Does not lock the Queue; assumes that the Queue is already
// locked when this function is called.
func (q *Queue) lengthNoLock() int64 {
	return q.tail.message.Offset - q.head.message.Offset
}

// Method to add a single message to the Queue.
func (q *Queue) Add(val string) error {
	return q.AddMany([]string{val})
}

// Method to add multiple messages to the Queue.
//
// If the Queue has been improperly initialized, i.e. created manually,
// returns the error ErrImproperlyInitializedQueue.
func (q *Queue) AddMany(vals []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tail == nil {
		return ErrImproperlyInitializedQueue
	}

	appendTime := time.Now()
	for _, val := range vals {
		q.tail.message.Val = val
		q.tail.message.LogAppendTime = appendTime
		msg := Message{
			Offset: q.tail.message.Offset + 1,
		}
		n := node{
			message: &msg,
		}
		q.tail.next = &n
		q.tail = &n
	}

	if q.config.autoCleanup {
		q.cleanup()
	}

	return nil
}

// Method to read a single message from the Queue.
func (q *Queue) Read() (Message, error) {
	res, err := q.ReadMany(1)
	if err != nil {
		return Message{}, err
	}
	return res[0], nil
}

// Method to read multiple messages from the Queue.
// Reads at most `limit` messages.
//
// If `limit` is non-positive, returns the error ErrInvalidLimit.
// If the Queue is empty, returns the error ErrQueueIsEmpty.
func (q *Queue) ReadMany(limit int) ([]Message, error) {
	if limit <= 0 {
		return []Message{}, ErrInvalidLimit
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.config.autoCleanup {
		q.cleanup()
	}

	length := q.lengthNoLock()
	if length <= math.MaxInt {
		limit = min(limit, int(length))
	}
	if q.isEmptyNoLock() {
		return []Message{}, ErrQueueIsEmpty
	}
	res := make([]Message, limit)
	node := q.head
	for i := 0; i < limit; i++ {
		res[i] = *node.message
		node = node.next
	}
	q.head = node
	return res, nil
}

// Method to get the next message without consuming it like Read does.
//
// If the Queue is empty, returns the error ErrQueueIsEmpty.
func (q *Queue) PeekNext() (Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.config.autoCleanup {
		q.cleanup()
	}

	if q.isEmptyNoLock() {
		return Message{}, ErrQueueIsEmpty
	}
	return *q.head.message, nil
}

// TODO
// Not yet implemented.
// Returns the error ErrUnimplementedMethod.
func (q *Queue) PeekLast() (Message, error) {
	return Message{}, ErrUnimplementedMethod
}

// Remove messages until there are at most retentionCount messages
// and remove messages that are older than retentionTime.
// Returns the count of deleted messages.
func (q *Queue) Cleanup() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.cleanup()
}

// Internal method to run cleanup on the Queue.
// Does not lock the Queue; assumes that the Queue is already
// locked when this function is called.
// Returns the count of deleted messages.
func (q *Queue) cleanup() int64 {
	removed := int64(0)

	length := q.lengthNoLock()
	retentionCount := q.config.retentionCount
	toRemove := max(0, length-retentionCount)
	removed += toRemove
	node := q.head
	for i := int64(0); i < toRemove; i++ {
		node = node.next
	}
	q.head = node

	currTime := time.Now()
	retentionTime := q.config.retentionTime
	tailOffset := q.tail.message.Offset
	for q.head.message.Offset < tailOffset && currTime.Sub(q.head.message.LogAppendTime) > retentionTime {
		removed++
		q.head = q.head.next
	}

	return removed
}
