# In-memory message queue in Go

An in-memory message queue in Go and a simple REST API to use it as a service. Not intended for real use, just an exercise in Go (concurrency and mutexes, testing, REST APIs, ...).

Initial planned outline:
- an in-memory queue:
    - thread safe,
    - configurable amount of retention,
    - rewind possibility (limited, likely not very performant),
    - option to flush old messages to disk instead of deleting entirely
- REST API to use all methods of the queue,
- possibly a gRPC API as well,
- replication with multiple servers.
