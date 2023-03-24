package workerpool

type WorkingState int32

func (ws WorkingState) IsActive() bool {
	return ws == Active
}

const (
	Stopped WorkingState = iota
	Idle
	Active
	Paused
)
