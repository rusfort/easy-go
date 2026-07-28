package eg

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

const (
	egThreadPrefix = "EG_THREAD_"
)

type threadCore struct {
	mtx       sync.Mutex
	threaders map[reflect.Type]any
}

var thc threadCore

func createNewThreadInCore[F EGFunction[T], T any](thc *threadCore, op F) *Thread[F, T] {
	thc.mtx.Lock()
	defer thc.mtx.Unlock()

	if thc.threaders == nil {
		thc.threaders = make(map[reflect.Type]any)
	}

	var (
		thr any
		ok  bool
	)

	thr, ok = thc.threaders[reflect.TypeOf(op)]
	if !ok {
		var newThreader threader[F, T]
		thc.threaders[reflect.TypeOf(op)] = &newThreader
		thr = &newThreader
	}

	thrdr, ok := thr.(*threader[F, T])
	if !ok {
		panic(fmt.Sprintf("unknown type of threader %T", thrdr))
	}

	return thrdr.CreateNew(op)
}

func getThreaderFromCore[F EGFunction[T], T any](thc *threadCore) *threader[F, T] {
	thc.mtx.Lock()
	defer thc.mtx.Unlock()

	thr, ok := thc.threaders[reflect.TypeFor[F]()]
	if !ok {
		var newThreader threader[F, T]
		thc.threaders[reflect.TypeFor[F]()] = &newThreader
		thr = &newThreader
	}

	thrdr, ok := thr.(*threader[F, T])
	if !ok {
		panic(fmt.Sprintf("unknown type of threader %T", thrdr))
	}

	return thrdr
}

//-----

type EGFunction[T any] interface {
    ~func() | ~func() T | ~func() (T, error)
}

type threader[F EGFunction[T], T any] struct {
	mtx     sync.Mutex
	threads []*Thread[F, T]
	exexMap sync.Map
}

func (tr *threader[F, T]) CreateNew(routine F) *Thread[F, T] {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()

	position := len(tr.threads)

	t := &Thread[F, T]{
		position: position,
		routine:  routine,
	}

	tr.threads = append(tr.threads, t)
	tr.exexMap.Store(position, false)
	return t
}

func (tr *threader[F, T]) IsThreadExecuted(position int) bool {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()

	executed, ok := tr.exexMap.Load(position)
	if !ok {
		return true
	}

	return executed.(bool)
}

func (tr *threader[F, T]) SetThreadExecuted(position int) {
	tr.exexMap.Delete(position)
}

//-----

type Thread[F EGFunction[T], T any] struct {
	position       int
	previous, next *Thread[F, T]
	routine        F
	started        atomic.Bool
}

func NewThread[F EGFunction[T], T any](routine F) *Thread[F, T] {
	return createNewThreadInCore[F, T](&thc, routine)
}

func (t *Thread[F, T]) String() string {
	if t == nil {
		return fmt.Sprint(egThreadPrefix + "NULL")
	}

	return fmt.Sprintf("%s%d", egThreadPrefix, t.position)
}

func (t *Thread[F, T]) Position() int {
	if t == nil {
		return -1
	}

	return t.position
}

func (t *Thread[F, T]) SetExecuted() {
	if t == nil {
		return
	}

	t.started.Store(true)
	getThreaderFromCore[F, T](&thc).SetThreadExecuted(t.position)
}

func (t *Thread[F, T]) logSelf(comment string) {
	fmt.Printf("%s: %s\n", t.String(), comment)
}

func (t *Thread[F, T]) Then(next *Thread[F, T]) *Thread[F, T] {
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

func (t *Thread[F, T]) After(previous *Thread[F, T]) *Thread[F, T] {
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

func (t *Thread[F, T]) start(goInited bool) {
	if t == nil {
		return
	}

	starter := func() {
		t.previous.MaybeStart(true)
		t.previous = nil

		if !getThreaderFromCore[F, T](&thc).IsThreadExecuted(t.Position()) {
			t.SetExecuted()
			t.logSelf("started")
			t.execute()
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

func (t *Thread[F, T]) execute() {
	switch v := any(t.routine).(type) {
	case func():
		v()
	case func() T:
		v()
	case func() (T, error):
		v()
	}
}

func (t *Thread[F, T]) Started() bool {
	if t == nil {
		return true
	}

	return t.started.Load()
}

func (t *Thread[F, T]) MaybeStart(goInited bool) {
	if t.Started() {
		return
	}

	if goInited {
		t.logSelf("maybe started no go")
		t.start(true)
		return
	}

	t.logSelf("maybe started with go")

	go t.start(true)
}

func (t *Thread[F, T]) Run() {
	t.MaybeStart(false)
}

//-----

func WaitConcurrentExec[F EGFunction[T], T any](threads ...*Thread[F, T]) {
	wg := sync.WaitGroup{}
	wg.Add(len(threads))

	for _, t := range threads {
		go func() {
			defer wg.Done()

			t.MaybeStart(true)
		}()
	}

	wg.Wait()
}

// func WorkThreads[T any](threads ...*Thread) []T{
// 	mtx := sync.Mutex{}

// }
