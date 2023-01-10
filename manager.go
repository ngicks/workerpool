package workerpool

import (
	"context"
	"time"

	"github.com/ngicks/gommon/pkg/common"
)

var (
	defaultMaxWaiting       = 5
	defaultRemovalBatchSize = 5
	defaultRemovalInterval  = 2 * time.Second
)

type Manager[T any] struct {
	pool *Pool[T]

	taskCh           chan T
	maxWorker        int
	maxWaiting       int
	removalBatchSize int
	removalInterval  time.Duration

	timerFactory func() common.ITimer
}

func NewManager[T any](pool *Pool[T], maxWorker int, options ...ManagerOption[T]) *Manager[T] {
	m := &Manager[T]{
		pool:             pool,
		taskCh:           make(chan T),
		maxWorker:        maxWorker,
		maxWaiting:       defaultMaxWaiting,
		removalBatchSize: defaultRemovalBatchSize,
		removalInterval:  defaultRemovalInterval,
		timerFactory:     timerFactory,
	}

	for _, opt := range options {
		opt(m)
	}

	return m
}

func (m *Manager[T]) Run(ctx context.Context) (task T, hadPending bool, err error) {
	timer := m.timerFactory()
	resetTimer := func() {
		timer.Reset(m.removalInterval)
	}
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			var zero T
			return zero, false, nil
		case <-timer.Channel():
			alive, sleeping := m.pool.Len()
			if alive == 0 {
				// does not need to reset timer here...
				// until worker is added.
				// But is it hard to test out?
				continue
			}
			activeWorkerNum := m.pool.ActiveWorkerNum()
			waiting := int64(alive+sleeping) - activeWorkerNum

			if waiting > int64(m.maxWaiting) {
				delta := int(waiting - int64(m.maxWaiting))
				if delta > m.removalBatchSize {
					delta = m.removalBatchSize
				}
				m.pool.Remove(delta)
			}
			resetTimer()
		case task, ok := <-m.taskCh:
			if !ok {
				var zero T
				return zero, false, ErrInputChanClosed
			}

			timer.Stop()
			select {
			case m.pool.Sender() <- task:
			default:
				alive, sleeping := m.pool.Len()
				delta := m.maxWorker - (alive + sleeping)
				if delta > 0 {
					if delta > (m.maxWaiting + 1) {
						delta = m.maxWaiting + 1
					}
					m.pool.Add(delta)
				}
				select {
				case m.pool.Sender() <- task:
				case <-ctx.Done():
					return task, true, nil
				}
			}
			resetTimer()
		}
	}
}

// Sender is getter of a sender side of the task channel,
// where you can send tasks to workers.
func (m *Manager[T]) Sender() chan<- T {
	return m.taskCh
}

// Len returns number of workers.
// alive is running workers. sleeping is workers removed by Remove but still working on its task.
func (m *Manager[T]) Len() (alive, sleeping int) {
	return m.pool.Len()
}

// ActiveWorkerNum returns number of actively working worker.
func (m *Manager[T]) ActiveWorkerNum() int64 {
	return m.pool.ActiveWorkerNum()
}

// Kill kills all workers.
func (m *Manager[T]) Kill() {
	m.pool.Kill()
}

func (m *Manager[T]) Pause(
	ctx context.Context,
	timeout time.Duration,
) (continueWorkers func() (cancelled bool), err error) {
	return m.pool.Pause(ctx, timeout)
}

// Wait waits for all workers to stop.
// Calling this without Kill and/or Remove all workers may block forever.
func (m *Manager[T]) Wait() {
	m.pool.Wait()
}
