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

// Message type contains the actual message stored in a Queue
// and related metadata (offset, logAppendTime).
type Message[T any] struct {
	Val           T
	Offset        uint64
	LogAppendTime time.Time
}

// QueueConfig type contains all the configuration options
// for a Queue.
type QueueConfig struct {
	name           string
	retentionCount uint64
	retentionTime  time.Duration
	autoCleanup    bool
}

// Linked list node. Used for Queue internals.
type node[T any] struct {
	message *Message[T]
	next    *node[T]
}

// Queue[T] is a message queue that stores messages of type T (any).
// Queue methods are safe to use concurrently in multiple goroutines.
//
// When messages are Read() from a Queue, they are discarded. There is no
// retention after a message has been read. It is possible to get a single
// message without discarding/consuming it with the method PeekNext().
//
// NOTE: never create a Queue directly; use NewQueue[T]() instead
// to construct a Queue[T].
type Queue[T any] struct {
	head   *node[T]
	tail   *node[T]
	config QueueConfig
	mu     sync.Mutex
}

// Function to create a default QueueConfig.
// To change the configuration, use the WithFoo methods.
// To create a Queue with a specific QueueConfig, use the NewQueueWithConfig function.
func DefaultConfig() QueueConfig {
	config := QueueConfig{
		name:           "",
		retentionCount: uint64(1e9),
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
func (config QueueConfig) WithRetentionCount(retentionCount uint64) (QueueConfig, error) {
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
// To create a Queue for messages of type T, call NewQueue[T]().
func NewQueue[T any]() *Queue[T] {
	config := DefaultConfig()
	msg := Message[T]{}
	n := node[T]{
		message: &msg,
	}
	res := Queue[T]{
		head:   &n,
		tail:   &n,
		config: config,
	}
	return &res
}

// Function to initialize a new empty Queue with the default config.
// To create a Queue for messages of type T, call NewQueueWithConfig[T]().
func NewQueueWithConfig[T any](config QueueConfig) *Queue[T] {
	msg := Message[T]{}
	n := node[T]{
		message: &msg,
	}
	res := Queue[T]{
		head:   &n,
		tail:   &n,
		config: config,
	}
	return &res
}

func (q *Queue[T]) isProperlyInitialized() bool {
	return q.tail != nil
}

func (q *Queue[T]) GetConfig() QueueConfig {
	return q.config
}

// Checks if the Queue is empty.
func (q *Queue[T]) IsEmpty() (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isProperlyInitialized() {
		return false, ErrImproperlyInitializedQueue
	}

	if q.config.autoCleanup {
		q.cleanup()
	}

	return q.isEmptyNoLock(), nil
}

// Internal method to check if the Queue is empty.
// Does not lock the Queue; assumes that the Queue is already
// locked when this function is called.
func (q *Queue[T]) isEmptyNoLock() bool {
	return q.head.message.Offset == q.tail.message.Offset
}

// Returns the length of the Queue.
func (q *Queue[T]) Length() (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isProperlyInitialized() {
		return 0, ErrImproperlyInitializedQueue
	}

	if q.config.autoCleanup {
		q.cleanup()
	}

	return q.lengthNoLock(), nil
}

// Internal method to get the length of the Queue.
// Does not lock the Queue; assumes that the Queue is already
// locked when this function is called.
func (q *Queue[T]) lengthNoLock() uint64 {
	return q.tail.message.Offset - q.head.message.Offset
}

// Method to add a single message to the Queue.
func (q *Queue[T]) Add(val T) error {
	return q.AddMany([]T{val})
}

// Method to add multiple messages to the Queue.
//
// If the Queue has been improperly initialized, i.e. created manually,
// returns the error ErrImproperlyInitializedQueue.
func (q *Queue[T]) AddMany(vals []T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isProperlyInitialized() {
		return ErrImproperlyInitializedQueue
	}

	appendTime := time.Now()
	for _, val := range vals {
		q.tail.message.Val = val
		q.tail.message.LogAppendTime = appendTime
		msg := Message[T]{
			Offset: q.tail.message.Offset + 1,
		}
		n := node[T]{
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
func (q *Queue[T]) Read() (Message[T], error) {
	res, err := q.ReadMany(1)
	if err != nil {
		return Message[T]{}, err
	}
	return res[0], nil
}

// Method to read multiple messages from the Queue.
// Reads at most `limit` messages.
//
// If `limit` is non-positive, returns the error ErrInvalidLimit.
// If the Queue is empty, returns the error ErrQueueIsEmpty.
func (q *Queue[T]) ReadMany(limit int) ([]Message[T], error) {
	if limit <= 0 {
		return []Message[T]{}, ErrInvalidLimit
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isProperlyInitialized() {
		return []Message[T]{}, ErrImproperlyInitializedQueue
	}

	if q.isEmptyNoLock() {
		return []Message[T]{}, ErrQueueIsEmpty
	}

	if q.config.autoCleanup {
		q.cleanup()
	}

	length := q.lengthNoLock()
	if length <= math.MaxInt {
		limit = min(limit, int(length))
	}
	res := make([]Message[T], limit)
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
func (q *Queue[T]) PeekNext() (Message[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isProperlyInitialized() {
		return Message[T]{}, ErrImproperlyInitializedQueue
	}

	if q.isEmptyNoLock() {
		return Message[T]{}, ErrQueueIsEmpty
	}

	if q.config.autoCleanup {
		q.cleanup()
	}

	return *q.head.message, nil
}

// TODO
// Not yet implemented.
// Returns the error ErrUnimplementedMethod.
func (q *Queue[T]) PeekLast() (Message[T], error) {
	return Message[T]{}, ErrUnimplementedMethod
}

// Remove messages until there are at most retentionCount messages
// and remove messages that are older than retentionTime.
// Returns the count of deleted messages.
func (q *Queue[T]) Cleanup() (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isProperlyInitialized() {
		return 0, ErrImproperlyInitializedQueue
	}

	return q.cleanup(), nil
}

// Internal method to run cleanup on the Queue.
// Does not lock the Queue; assumes that the Queue is already
// locked when this function is called.
// Returns the count of deleted messages.
func (q *Queue[T]) cleanup() uint64 {
	removed := uint64(0)

	length := q.lengthNoLock()
	retentionCount := q.config.retentionCount
	var toRemove uint64
	if length > retentionCount {
		toRemove = length - retentionCount
	}
	removed += toRemove
	node := q.head
	for i := uint64(0); i < toRemove; i++ {
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
