package workerpool

import (
	"context"
	"time"

	"github.com/ngicks/gommon/pkg/common"
)

const (
	defaultMaxWaiting       = 5
	defaultRemovalBatchSize = 5
	defaultRemovalInterval  = 2 * time.Second
)

func timerFactory() common.Timer { return common.NewTimerReal() }

type onlyExposable[T any] interface {
	Sender() chan<- T
	Len() (alive, sleeping int)
	Kill()
	Pause(ctx context.Context, fn func(ctx context.Context)) (err error)
	Wait()
	WaitUntil(condition func(alive, sleeping int) bool, action ...func())
}

type Controller[K comparable, T any] struct {
	Container onlyExposable[T]
	container *Container[K, T]

	taskCh           chan T
	maxWorker        int
	maxWaiting       int
	removalBatchSize int
	removalInterval  time.Duration

	timerFactory func() common.Timer
}

func NewController[K comparable, T any](container *Container[K, T], maxWorker int, opts ...ControllerOption[K, T]) *Controller[K, T] {
	c := &Controller[K, T]{
		Container:        container,
		container:        container,
		taskCh:           make(chan T),
		maxWorker:        maxWorker,
		maxWaiting:       defaultMaxWaiting,
		removalBatchSize: defaultRemovalBatchSize,
		removalInterval:  defaultRemovalInterval,
		timerFactory:     timerFactory,
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Controller[K, T]) MaxWorker() int {
	return c.maxWorker
}

func (c *Controller[K, T]) Run(ctx context.Context) (task T, hadPending bool, err error) {
	timer := c.timerFactory()
	resetTimer := func() {
		timer.Reset(c.removalInterval)
	}
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C():
			default:
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			var zero T
			return zero, false, nil
		case <-timer.C():
			alive, sleeping := c.container.Len()
			if alive == 0 {
				// does not need to reset timer here...
				// until worker is added.
				// But does it make tests difficult?
				continue
			}
			waiting := alive + sleeping

			if waiting > c.maxWaiting {
				delta := waiting - c.maxWaiting
				if delta > c.removalBatchSize {
					delta = c.removalBatchSize
				}
				c.container.Remove(delta)
			}
			resetTimer()
		case task, ok := <-c.taskCh:
			if !ok {
				var zero T
				return zero, false, ErrInputChanClosed
			}

			timer.Stop()
			select {
			case c.container.Sender() <- task:
			default:
				alive, sleeping := c.container.Len()
				delta := c.maxWorker - (alive + sleeping)
				if delta > 0 {
					if delta > (c.maxWaiting + 1) {
						delta = c.maxWaiting + 1
					}
					c.container.Add(delta)
				}
				select {
				case c.container.Sender() <- task:
				case <-ctx.Done():
					return task, true, nil
				}
			}
			resetTimer()
		}
	}
}
