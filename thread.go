package eg

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	egThreadPrefix = "EG_THREAD_"
)

type threader struct {
	mtx     sync.Mutex
	threads []*Thread
	exexMap sync.Map
}

var tr threader

func (tr *threader) CreateNew(routine func()) *Thread {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()

	position := len(tr.threads)

	t := &Thread{
		position: position,
		routine:  routine,
	}

	tr.threads = append(tr.threads, t)
	tr.exexMap.Store(position, false)
	return t
}

func (tr *threader) IsThreadExecuted(position int) bool {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()

	executed, ok := tr.exexMap.Load(position)
	if !ok {
		return true
	}

	return executed.(bool)
}

func (tr *threader) SetThreadExecuted(position int) {
	tr.exexMap.Delete(position)
}

type Thread struct {
	position       int
	previous, next *Thread
	routine        func()
	started        atomic.Bool
}

func NewThread(routine func()) *Thread {
	return tr.CreateNew(routine)
}

func (t *Thread) String() string {
	if t == nil {
		return fmt.Sprint(egThreadPrefix + "NULL")
	}

	return fmt.Sprintf("%s%d", egThreadPrefix, t.position)
}

func (t *Thread) Position() int {
	if t == nil {
		return -1
	}

	return t.position
}

func (t *Thread) SetExecuted() {
	if t == nil {
		return
	}

	t.started.Store(true)
	tr.SetThreadExecuted(t.position)
}

func (t *Thread) logSelf(comment string) {
	fmt.Printf("%s: %s\n", t.String(), comment)
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

func (t *Thread) Start(goInited bool) {
	if t == nil {
		return
	}

	starter := func() {
		t.previous.MaybeStart(true)
		t.previous = nil

		if !tr.IsThreadExecuted(t.Position()) {
			t.SetExecuted()
			t.logSelf("started")
			t.routine()
			t.logSelf("done")
		}

		t.next.MaybeStart(true)
		t.next = nil
	}

	if goInited {
		t.logSelf("started no go")
		starter()
	} else {
		t.logSelf("started with go")
		go starter()
	}
}

func (t *Thread) Started() bool {
	if t == nil {
		return true
	}

	return t.started.Load()
}

func (t *Thread) MaybeStart(goInited bool) {
	if t.Started() {
		return
	}

	if goInited {
		t.logSelf("maybe started no go")
		t.Start(true)
		return
	}

	t.logSelf("maybe started with go")

	go t.Start(true)
}

func (t *Thread) Run() {
	t.MaybeStart(false)
}
