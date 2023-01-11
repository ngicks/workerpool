package workerpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestState(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		label                        string
		isEnded, isRunning, IsPaused bool
		s, target                    WorkerState
	}

	for _, testCase := range []testCase{
		{"stopped", false, false, false, Stopped, Stopped},
		{"running", false, true, false, Running, Running},
		{"paused", false, false, true, Paused, Paused},
		{"ended", true, false, false, Stopped, Stopped.setEnded()},
		{"ended", true, false, false, Stopped, EndedMask},
		{"ended but still running", true, true, false, Running, Running.setEnded()},
		{"ended but still paused", true, false, true, Paused, Paused.setEnded()},
	} {
		assert.Equal(testCase.isEnded, testCase.target.IsEnded(), testCase.label)
		assert.Equal(testCase.isRunning, testCase.target.IsRunning(), testCase.label)
		assert.Equal(testCase.IsPaused, testCase.target.IsPaused(), testCase.label)
		s, isEnded := testCase.target.State()
		assert.Equal(testCase.s, s, testCase.label)
		assert.Equal(testCase.isEnded, isEnded, testCase.label)
	}

	for _, input := range []WorkerState{Stopped, EndedMask} {
		isEnded := input.IsEnded()

		running := input.set(Running)
		s, _ := running.State()
		assert.Equal(Running, s)
		assert.Equal(isEnded, running.IsEnded())

		paused := running.set(Paused)
		s, _ = paused.State()
		assert.Equal(Paused, s)
		assert.Equal(isEnded, paused.IsEnded())

		stopped := paused.set(Stopped)
		s, _ = stopped.State()
		assert.Equal(Stopped, s)
		assert.Equal(isEnded, stopped.IsEnded())
	}
}
