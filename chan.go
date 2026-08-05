package eg

import (
	"fmt"
	"reflect"
	"sync/atomic"
)

// Chan - user-friendly chan without "<-" and "->"
type Chan[T any] struct {
	closed atomic.Bool
	ch     chan T
}

type IChan interface {
	GetType() reflect.Type
	AbstractOriginal() reflect.Value
}

// NewChan - create a new Chan with data transfer type T
func NewChan[T any]() *Chan[T] {
	return &Chan[T]{}
}

// Original - get a go chan (not sure you really want it)
func (c *Chan[T]) Original() chan T {
	return c.ch
}

// AbstractOriginal - just don't use it, trust me
func (c *Chan[T]) AbstractOriginal() reflect.Value {
	var abstractChan any = c.ch
	return abstractChan.(reflect.Value)
}

// Write - panic-free putting a value in the Chan (blocking call until any eg.Thread or goroutine reads from it, does nothing if the Chan is closed)
func (c *Chan[T]) Write(v T) {
	if c.closed.Load() {
		return
	}

	c.ch <- v
}

// Read - read a value from the Chan (blocking call until any eg.Thread or goroutine writes anything in it)
func (c *Chan[T]) Read() T {
	if c.closed.Load() {
		var zero T
		return zero
	}

	return <-c.ch
}

// Close - closes the Chan (below this point Write will be unavailable)
func (c *Chan[T]) Close() {
	c.closed.Store(true)
	close(c.ch)
}

// GetType - returns reflect Type of T
func (c *Chan[T]) GetType() reflect.Type {
	return reflect.TypeFor[T]()
}

type Selector map[IChan]func(any)

const maxChansInSelector = 3

func (s Selector) GetStructure() ([]IChan, IChan) {
	var (
		chanKeys   []IChan
		defaultKey IChan = nil
	)

	for ch := range s {
		if _, ok := ch.(selectorDefault); ok {
			if defaultKey != nil {
				panic("found more than one default in select")
			}

			defaultKey = ch

			continue
		}

		if len(chanKeys) == maxChansInSelector {
			panic(fmt.Sprintf("found more than %d chans in select", maxChansInSelector))
		}

		chanKeys = append(chanKeys, ch)
	}

	if len(chanKeys) == 0 {
		panic("nothing to select from")
	}

	return chanKeys, defaultKey
}

func Returner(any) {}

type selectorDefault any

var SelectorDefault *Chan[selectorDefault]

// ChanSelect - like go select (NOTE: blocking call, size of selector must be min 1 and max 3 Chan + min 0 and max 1 SelectorDefault, otherwise panic) - returns true once the Returner is selected
func ChanSelect(selector Selector) bool {
	if selector == nil {
		return false
	}

	_, _ = selector.GetStructure()

	rSelector := make([]reflect.SelectCase, 0, len(selector))
	funcs := make([]func(any), 0, len(selector))
	for ch, fun := range selector {
		if _, ok := ch.(selectorDefault); ok {
			rSelector = append(rSelector, reflect.SelectCase{Dir: reflect.SelectDefault})
		} else {
			rSelector = append(rSelector, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: ch.AbstractOriginal()})
		}
		funcs = append(funcs, fun)
	}

	chosenIndex, value, _ := reflect.Select(rSelector)

	chosenFun := funcs[chosenIndex]

	if isReturner(chosenFun) {
		return true
	}

	chosenFun(value)

	return false
}

func isReturner(f func(any)) bool {
	return FuncEqual(f, Returner)
}

func createInstance(t reflect.Type) any {
	ptrValue := reflect.New(t)

	return ptrValue.Interface()
}
