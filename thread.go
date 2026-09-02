package eg

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	egThreadPrefix = "EG_THREAD_"
)

// threadResult - an abstract object which stands for return parameters of any function
type threadResult[T any] struct {
	result *T
	err    *error
}

// threadCore is a singleton like GO engine which controls all threaders
type threadCore struct {
	mtx       sync.Mutex
	threaders map[reflect.Type]any
}

var thc threadCore

func createNewThreadInCore[F egFunction[T], T any](thc *threadCore, op F) *Thread[F, T] {
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

func getThreaderFromCore[F egFunction[T], T any](thc *threadCore) *threader[F, T] {
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

// egFunction stands for an abstraction of any golang func possible
type egFunction[T any] interface {
	~func() | ~func() T | ~func() (T, error)
}

// threader is a controller of all threads of a specific type
type threader[F egFunction[T], T any] struct {
	mtx     sync.Mutex
	threads []*Thread[F, T]
	exexMap sync.Map
}

// GetFunctionType is a way to recognise a type of egFunction
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

// CreateNew creates a new thread in threader
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

// IsThreadExecuted returns an execution status of a thread inside a threader
func (tr *threader[F, T]) IsThreadExecuted(position int) bool {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()

	executed, ok := tr.exexMap.Load(position)
	if !ok {
		return true
	}

	return executed.(bool)
}

// SetThreadExecuted tells threader about a successful execution
func (tr *threader[F, T]) SetThreadExecuted(position int) {
	tr.exexMap.Delete(position)
}

//-----

// Thread is a base entity of easy-go lib - it works the similar way as threads in other languages
type Thread[F egFunction[T], T any] struct {
	position       int
	previous, next *Thread[F, T]
	routine        F
	started        atomic.Bool
	result         *threadResult[T]
}

// NewThread creates new Thread for one of the egFunction[T]
func NewThread[F egFunction[T], T any](routine F) *Thread[F, T] {
	return newThread[F, T](routine)
}

// NewBasicThread creates new Thread exactly for 'func()' with no return type
func NewBasicThread(routine func()) *Thread[func() any, any] {
	return NewThread[func() any, any](func() any {
		routine()
		return nil
	})
}

func newThread[F egFunction[T], T any](routine F) *Thread[F, T] {
	return createNewThreadInCore[F, T](&thc, routine)
}

// String describes a Thread
func (t *Thread[F, T]) String() string {
	if t == nil {
		return fmt.Sprint(egThreadPrefix + "NULL")
	}

	trType := strings.ReplaceAll(fmt.Sprintf("%T", t.routine), " ", "")

	return fmt.Sprintf("%s%s_%d", egThreadPrefix, trType, t.position)
}

// Position returns a position of the Thread inside a threader of this type
func (t *Thread[F, T]) Position() int {
	if t == nil {
		return -1
	}

	return t.position
}

// Function returns an executable routine of the Thread itself
func (t *Thread[F, T]) Function() F {
	if t == nil {
		var f F
		return f
	}

	return t.routine
}

// SetExecuted prevents the Thread to be executed twice
func (t *Thread[F, T]) SetExecuted() {
	if t == nil {
		return
	}

	t.started.Store(true)
	getThreaderFromCore[F, T](&thc).SetThreadExecuted(t.position)
}

// Then records a strict order of execution: next is always executed after t
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

// After records a strict order of execution: previous is always executed before t
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

// start is a main execution engine of a Thread - it's panic free and never creates excess goroutine
func (t *Thread[F, T]) start(goInited bool) threadResult[T] {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("EG: THREAD (in start) PANICED: %v", err)
		}
	}()

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
			res = t.execute()
			t.result = &res
		}

		t.next.MaybeStart(true)
		t.next = nil

		return res
	}

	if goInited {
		return starter()
	} else {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("EG: THREAD (in starter) PANICED: %v", err)
				}
			}()

			starter()
		}()
	}

	return zero
}

// execute executes the core routine of a Thread and returns a threadResult
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

// Started returns a fact of the Thread execution
func (t *Thread[F, T]) Started() bool {
	if t == nil {
		return true
	}

	return t.started.Load()
}

// MaybeStart is a double-start-safe and excess-goroutine-safe execution
func (t *Thread[F, T]) MaybeStart(goInited bool) threadResult[T] {
	var zero threadResult[T]
	if t.Started() {
		return zero
	}

	if goInited {
		return t.start(true)
	}

	go t.start(true)

	return zero
}

// Run runs a Thread concurrently
func (t *Thread[F, T]) Run() {
	t.MaybeStart(false)
}

//-----

// WaitConcurrentExec runs all of the given threads concurrently and waits till all of them finish
func WaitConcurrentExec[F egFunction[T], T any](threads ...*Thread[F, T]) {
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

// WorkThreads is the same as WaitConcurrentExec but returns a Slice of results (if a Thread type provides a threadResult)
func WorkThreads[F egFunction[T], T any](threads ...*Thread[F, T]) *Slice[T] {
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
