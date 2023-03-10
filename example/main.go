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
	exec := workerpool.WorkFn[string, int](
		func(ctx context.Context, id string, num int) error {
			time.Sleep((time.Duration(rand.Int31n(1000)) + 1000) * time.Millisecond)
			fmt.Printf("%d,", num)
			return nil
		},
	)
	pool := workerpool.New[string, int](exec, workerpool.NewUuidPool())

	fmt.Println("Adding 1 goroutine; for sending tasks.")
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			pool.Sender() <- i
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

			goroutines := runtime.NumGoroutine()
			active, sleeping, activeWorker := pool.Len()
			fmt.Printf(
				"\nCurrent goroutine num: %2.2d\n"+
					"Alive worker = %2.2d, Sleeping worker = %2.2d, Active worker = %2.2d\n",
				goroutines,
				active, sleeping, activeWorker,
			)

			time.Sleep(500 * time.Millisecond)
		}
	}()

	fmt.Printf("Current goroutine num: %d\n", runtime.NumGoroutine())
	fmt.Println("Adding 32 workers")
	pool.Add(32)

	time.Sleep(time.Second)

	pool.Remove(16)
	fmt.Println("Requested to remove 16 workers.")

	<-done

	time.Sleep(time.Second)
	pool.Remove(math.MaxInt64)
	pool.Wait()
	<-time.After(time.Second)
	close(stopLog)
	fmt.Println("\nDone.")
}
