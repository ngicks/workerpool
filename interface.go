package workerpool

import (
	"context"
	"errors"
)

type WorkerPool[K comparable, T any] interface {
	Get() (w Worker[K, T], ok bool)
	Put(w Worker[K, T])
}

var (
	ErrAlreadyRunning  = errors.New("already running")
	ErrKilled          = errors.New("worker killed")
	ErrInputChanClosed = errors.New("input chan closed")
	ErrNotRunning      = errors.New("not running")
)

type Worker[K comparable, T any] interface {
	Id() K
	Run(ctx context.Context, taskCh <-chan T) error
	Pause(ctx context.Context, fn func(ctx context.Context)) (err error)
	Kill()
	State() WorkingState
	WaitUntil(condition func(state WorkingState) bool, actions ...func())
}

// WorkerInitiator is an interface for additional methods.
type WorkerInitiator interface {
	Start() error
	Stop() error
}
