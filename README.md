# workerpool

workerpool implements fan-out pattern nicely.

## example

```go
package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"time"

	"github.com/ngicks/workerpool"
)

func main() {
	exec := workerpool.WorkFn[int](
		func(ctx context.Context, num int) error {
			time.Sleep((time.Duration(rand.Int31n(1000)) + 1000) * time.Millisecond)
			fmt.Printf("%d,", num)
			return nil
		},
	)
	pool := workerpool.New[int](exec)

	fmt.Println("Adding 1 goroutine; for sending tasks.")
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			pool.SenderChan() <- i
		}
		fmt.Println("\nEnding 1 goroutine; task sending done.")
		close(done)
	}()

	fmt.Println("Adding 1 goroutine; for logging.")
	stopLog := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopLog:
				return
			default:
			}

			activeWorker := pool.ActiveWorkerNum()
			goroutines := runtime.NumGoroutine()
			active, sleeping := pool.Len()
			fmt.Printf(
				"\nCurrent active worker: %d\n"+
					"Current goroutine num: %d\n"+
					"Alive worker = %d, Sleeping worker = %d.\n",
				activeWorker,
				goroutines,
				active, sleeping,
			)

			time.Sleep(500 * time.Millisecond)
		}
	}()

	fmt.Printf("Current goroutine num: %d\n", runtime.NumGoroutine())
	fmt.Println("Adding 32 workers")
	pool.Add(32)

	time.Sleep(500 * time.Millisecond)

	pool.Remove(16)
	fmt.Println("Requested to remove 16 workers.")

	<-done

	time.Sleep(time.Second)
	pool.Remove(math.MaxUint32)
	pool.Wait()
	<-time.After(time.Second)
	close(stopLog)
	fmt.Println("\nDone.")
}

```

```
go run example/main.go
Adding 1 goroutine; for sending tasks.
Adding 1 goroutine; for logging.
Current goroutine num: 3
Adding 32 workers

Current active worker: 1
Current goroutine num: 35
Alive worker = 32, Sleeping worker = 0.
Requested to remove 16 workers.

Current active worker: 32
Current goroutine num: 35
Alive worker = 16, Sleeping worker = 16.

Current active worker: 32
Current goroutine num: 35
Alive worker = 16, Sleeping worker = 16.
22,23,21,0,8,14,9,13,3,2,26,11,25,17,6,12,20,10,16,30,4,
Current active worker: 19
Current goroutine num: 22
Alive worker = 16, Sleeping worker = 3.
19,31,7,29,18,15,27,5,1,28,24,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
32,39,35,34,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
41,42,38,33,37,40,45,36,46,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
44,43,47,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
48,54,49,52,53,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
51,59,60,50,63,57,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
55,58,61,56,64,62,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
67,66,72,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
65,75,68,71,70,74,69,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
77,73,76,79,80,82,
Current active worker: 16
Current goroutine num: 19
Alive worker = 16, Sleeping worker = 0.
83,85,86,
Ending 1 goroutine; task sending done.
78,84,89,81,
Current active worker: 12
Current goroutine num: 18
Alive worker = 16, Sleeping worker = 0.
93,87,91,
Current active worker: 9
Current goroutine num: 18
Alive worker = 16, Sleeping worker = 0.
92,88,90,98,95,
Current active worker: 4
Current goroutine num: 6
Alive worker = 0, Sleeping worker = 4.
96,94,99,97,
Current active worker: 0
Current goroutine num: 2
Alive worker = 0, Sleeping worker = 0.

Current active worker: 0
Current goroutine num: 2
Alive worker = 0, Sleeping worker = 0.

Done.
```
