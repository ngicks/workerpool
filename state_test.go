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

	for _, input := range []WorkerState{Stopped, EndedMask, ActiveMask, Running.setActive()} {
		name := input.name()
		isEnded := input.IsEnded()
		isActive := input.IsActive()

		running := input.set(Running)
		s, _ := running.State()
		assert.Equal(Running, s, name)
		assert.Equal(isEnded, running.IsEnded(), name)
		assert.Equal(isActive, running.IsActive(), name)

		ended := running.setEnded()
		s, _ = running.State()
		assert.Equal(Running, s, name)
		assert.Equal(true, ended.IsEnded(), name)
		assert.Equal(isActive, ended.IsActive(), name)

		paused := running.set(Paused)
		s, _ = paused.State()
		assert.Equal(Paused, s, name)
		assert.Equal(isEnded, paused.IsEnded(), name)
		assert.Equal(isActive, running.IsActive(), name)

		stopped := paused.set(Stopped)
		s, _ = stopped.State()
		assert.Equal(Stopped, s, name)
		assert.Equal(isEnded, stopped.IsEnded(), name)
		assert.Equal(isActive, running.IsActive(), name)
	}
}
