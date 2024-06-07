# In-memory message queue in Go

An in-memory message queue in Go and a simple REST API to use it as a service. Not intended for real use, just an exercise in Go (concurrency and mutexes, testing and some TDD, REST APIs, ...). Mainly using this as a project to practice what I'm learning while reading the _excellent_ book [Learn Go with tests](https://quii.gitbook.io/learn-go-with-tests).

## Initial planned outline
- an in-memory message queue:
    - thread safe,
    - configurable amount of retention,
    - (maybe) option to flush old messages to disk instead of deleting entirely,
    - (maybe) rewind possibility (limited, likely not very performant).
- REST (and maybe a gRPC) API and server to deploy a message queue service,
- (maybe) replication with multiple servers.

---

## Example: How to use the message queue implemented in package `pkg/queue`

The message queue is implemented as the type `Queue[T]` in the package `pkg/queue`. To create a new `Queue`, use the constructor function `NewQueue`. A `Queue[T]` contains messages of any fixed type `T`, and this type needs to be specified when calling the constructor.
```
intQueue := queue.NewQueue[int]()
```
All methods of `Queue` check that the `Queue` was created using the constructor and return the error `ErrImproperlyInitializedQueue` otherwise.
```
invalidQueue := new(queue.Queue[int])
_, err := invalidQueue.IsEmpty()
fmt.Println(err.Error()) // improperly initialized queue, tail is nil
```
The basic methods of `Queue` are: `IsEmpty`, `Length`, `Add`, `AddMany`, `Read`, `ReadMany`, `PeekNext`, and `Cleanup`.
```
empty, _ := intQueue.IsEmpty()
fmt.Println(empty) // true
length, _ := intQueue.Length()
fmt.Println(length) // 0

_ = intQueue.Add(123)                 // Add a single value with Add()
_ = intQueue.AddMany([]int{123, 321}) // Add multiple values with AddMany()

empty, _ = intQueue.IsEmpty()
fmt.Println(empty) // false
length, _ = intQueue.Length()
fmt.Println(length) // 3
```
When you call `Read` to read a message from a `Queue`, the message is consumed/removed.
```
msg, _ := intQueue.Read()
length, _ = intQueue.Length()
fmt.Println(length) // 2
```
The `Message[T]` object you read from the `Queue[T]`, contains the actual value in the `Val` attribute.
```
fmt.Println(msg.Val) // 123
```
The `Message[T]` object also contains the following metadata fields:
  - `Offset` to keep track of how many messages have been added to the `Queue` in total; loops to 0 after `MaxUint64` messages.
  - `LogAppendTime` to store a timestamp for the time the message was added to the Queue.
```
fmt.Println(msg.Offset) // 0
```
When you call `ReadMany(n)`, you get a slice of messages with at most n messages.
```
msgs, _ := intQueue.ReadMany(10)
fmt.Println(len(msgs))   // 2
fmt.Println(msgs[1].Val) // 321
```
When the `Queue` is empty, if you call `Read`, `ReadMany`, or `PeekNext`, you get the error `ErrQueueIsEmpty` back.
```
_, err = intQueue.Read()
fmt.Println(err.Error()) // queue is empty
```
If you want to peek, i.e. read a message without consuming it, you can use the method `PeekNext`.
```
_ = intQueue.Add(222)
msg, _ = intQueue.PeekNext()
fmt.Println(msg.Val) // 222
empty, _ = intQueue.IsEmpty()
fmt.Println(empty) // false
```
To configure
  - the name of a `Queue`,
  - the maximum amount of messages to retain,
  - the maximum duration to retain messages,
  - and whether to automatically clean up old messages

you use the QueueConfig type.

You should create a `QueueConfig` with the constructor `DefaultConfig` and then modify it with the methods
  - `WithName`,
  - `WithRetentionCount`,
  - `WithRetentionTime`,
  - `WithAutoCleanup`.

Alternatively, you can get a copy of the config of an existing `Queue` with the method `GetConfig` and then modify it if needed.

To actually use a `QueueConfig` in a `Queue`, you create a `Queue` with a specific config with the constructor `NewQueueWithConfig`.

For example, you can create a `Queue` for string messages which retains only 2 messages and automatically cleans old messages as follows:
```
config := queue.DefaultConfig()
config, _ = config.WithRetentionCount(2)
config, _ = config.WithAutoCleanup(true)
stringQueue := queue.NewQueueWithConfig[string](config)
```
Now, if we add 3 messages to `stringQueue` and read from it, we get the second message since the first one gets cleaned up.
```
_ = stringQueue.AddMany([]string{"a", "b", "c"})
msgString, _ := stringQueue.Read()
fmt.Println(msgString.Val) // b
```
If we don't set automatic cleanup to be true, then we can manually trigger cleanup with the `Cleanup` method.
```
config2 := queue.DefaultConfig()
config2, _ = config2.WithRetentionCount(2)
stringQueue2 := queue.NewQueueWithConfig[string](config2)
_ = stringQueue2.AddMany([]string{"a", "b", "c"})
```
`stringQueue2` still has 3 messages.
```
length, _ = stringQueue2.Length()
fmt.Println(length) // 3
```
After `Cleanup`, `stringQueue2` has 2 messages.
```
_, _ = stringQueue2.Cleanup()
length, _ = stringQueue2.Length()
fmt.Println(length) // 2
msgString, _ = stringQueue2.Read()
fmt.Println(msgString.Val) // b
```
