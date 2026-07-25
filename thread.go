package eg

import (
	"sync/atomic"
)

type Thread struct {
	previous, next *Thread
	routine        func()
	started        atomic.Bool
}

func NewThread(routine func()) *Thread {
	return &Thread{
		routine: routine,
	}
}

func (t *Thread) Then(next *Thread) *Thread {
	if t == nil || next == nil {
		return t
	}

	if t.next != nil {
		return t
	}

	t.next = next
	t.next.After(t)

	return t.next
}

func (t *Thread) After(previous *Thread) *Thread {
	if t == nil || previous == nil {
		return t
	}

	if t.previous != nil {
		return t
	}

	t.previous = previous
	t.previous.Then(t)

	return t.previous
}

func (t *Thread) Start() {
	if t == nil {
		return
	}

	go func() {
		t.previous.MaybeStart()
		t.previous = nil

		t.started.Store(true)
		t.routine()

		t.next.MaybeStart()
		t.next = nil
	}()
}

func (t *Thread) Started() bool {
	if t == nil {
		return true
	}

	return t.started.Load()
}

func (t *Thread) MaybeStart() {
	if t.Started() {
		return
	}

	t.Start()
}
