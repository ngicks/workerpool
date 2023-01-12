# workerpool

workerpool implements the fan-out pattern nicely.

```go
// Task Executor
// This function will be called with each task T.
// ctx is cancelled only if worker.Kill() is called,
// So func must respect ctx if work take long time to complete.
exec := workerpool.WorkFn[int](
	func(ctx context.Context, task int) error {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil
		}
		fmt.Printf("%d,", task)
		return nil
	},
)
// Worker pool can be created with New.
pool := workerpool.New[int](exec)

// Add workers to the pool.
// This creates additional n goroutines by return of Add.
pool.Add(32)

for i := 0; i < 32; i++ {
	// Sender() returns chan<- T
	// You can send tasks through this channel.
	pool.Sender() <- i
}

// This is merely fan-out pattern.
// If all workers are busy, this send will block until at least 1 tasks is done.
pool.Sender() <- 32

// Remove removes workers from the pool.
pool.Remove(16)
// Len returns number of workers
fmt.Println(pool.Len())
// This should print like, 16, 16, 32
// These values report number of alive worker, sleeping worker (busy but removed worker),
// and active worker (currently is busy).
// sleeping workers may eventually become 0, since it is not receiving new tasks.

// You can use WaitUntil to observe and wait for state change.
pool.WaitUntil(func(alive, sleeping, active int) bool {
	return alive == 16 && active == 0
})

// You can use the Manager[T] as well.
// This gradually increases pool's workers to max worker,
// or decreases when manager is idle.
manager := workerpool.NewManager(
	/* pool = */		pool,
	/* max worker = */	31,
	workerpool.SetMaxWaiting[int](5),
	workerpool.SetRemovalBatchSize[int](7),
	workerpool.SetRemovalInterval[int](500*time.Millisecond),
)

ctx, cancel := context.WithCancel(context.Background())
go manager.Run(ctx)

// ...later...
cancel()


pool.Remove(math.MaxInt64)
// or
manager.Kill() // pool.Kill() is also ok.
// Wait-s until all worker stop its goroutine.
manager.Wait() // pool.Wait() is also ok.
```

## Features

- Workers can be added / removed dynamically.
- Hookable: task receive / done events can be hooked by setting SetHook Option to New.
- Immune to panicking
  - Task abnormal returns are recovered and hooked.
  - You can observe abnormal returns by setting SetAbnormalReturnCb Option to New.
- Type param enabled.
- Split command and client
  - With generics, task can be any arbitrary type T. Now you can send whatever you need to.
    - Typical implementations used `func()`.
  - This brings opportunity of split the task executor and the task itself.
  - To identify tasks, let T have Id.
- Shut down gracefully.
  - Instruct pool to stop all its goroutines.
    - Wait for all current tasks done: remove all workers by calling Remove with math.MaxInt64.
    - Cancel all contexts for tasks: call Kill. WorkExecutor must respect `context.Context` passed to Exec and return on `<-ctx.Done()` receive.
  - Call `Wait` to wait until all goroutines return.

## example

see and run [example](./example/main.go)
