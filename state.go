package workerpool

type WorkingState int32

const (
	Stopped WorkingState = iota
	Idle
	Active
	Paused
)
