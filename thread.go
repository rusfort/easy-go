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

type threadResult[T any] struct {
	result *T
	err    *error
}

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

const (
	EGFunctionTypeUnknown = iota
	EGFunctionTypeDefault
	EGFunctionTypeValue
	EGFunctionTypeResult
)

type EGFunction[T any] interface {
	~func() | ~func() T | ~func() (T, error)
}

type threader[F EGFunction[T], T any] struct {
	mtx     sync.Mutex
	threads []*Thread[F, T]
	exexMap sync.Map
}

func GetFunctionType[T any](f any) int {
	switch f.(type) {
	case func():
		return EGFunctionTypeDefault
	case func() T:
		return EGFunctionTypeValue
	case func() (T, error):
		return EGFunctionTypeResult
	default:
		return EGFunctionTypeUnknown
	}
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
	result         *threadResult[T]
}

func NewThread[F EGFunction[T], T any](routine F) *Thread[F, T] {
	return newThread[F, T](routine)
}

func newThread[F EGFunction[T], T any](routine F) *Thread[F, T] {
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

func (t *Thread[F, T]) Function() F {
	if t == nil {
		var f F
		return f
	}

	return t.routine
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

func (t *Thread[F, T]) start(goInited bool) threadResult[T] {
	var zero threadResult[T]
	if t == nil {
		return zero
	}

	starter := func() threadResult[T] {
		var res threadResult[T]
		t.previous.MaybeStart(true)
		t.previous = nil

		if !getThreaderFromCore[F, T](&thc).IsThreadExecuted(t.Position()) {
			t.SetExecuted()
			t.logSelf("started")
			res = t.execute()
			t.result = &res
			t.logSelf("done")
		}

		t.next.MaybeStart(true)
		t.next = nil

		return res
	}

	if goInited {
		t.logSelf("started no go")
		return starter()
	} else {
		t.logSelf("started with go")
		go starter()
	}

	return zero
}

func (t *Thread[F, T]) execute() threadResult[T] {
	switch v := any(t.routine).(type) {
	case func():
		v()
		return threadResult[T]{}
	case func() T:
		r := v()
		return threadResult[T]{
			result: &r,
		}
	case func() (T, error):
		r, err := v()
		return threadResult[T]{
			result: &r,
			err:    &err,
		}
	default:
		return threadResult[T]{}
	}
}

func (t *Thread[F, T]) Started() bool {
	if t == nil {
		return true
	}

	return t.started.Load()
}

func (t *Thread[F, T]) MaybeStart(goInited bool) threadResult[T] {
	var zero threadResult[T]
	if t.Started() {
		return zero
	}

	if goInited {
		t.logSelf("maybe started no go")
		return t.start(true)
	}

	t.logSelf("maybe started with go")

	go t.start(true)

	return zero
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

func WorkThreads[F EGFunction[T], T any](threads ...*Thread[F, T]) *Slice[T] {
	result := NewSlice[T]()
	if len(threads) == 0 {
		return result
	}

	if GetFunctionType[T](threads[0].Function()) == EGFunctionTypeDefault {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(len(threads))

	for _, t := range threads {
		go func() {
			defer wg.Done()

			r := t.MaybeStart(true)
			if r.result == nil {
				return
			}

			result.Append(*r.result)
		}()
	}

	wg.Wait()

	return result
}
