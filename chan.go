package eg

import "sync/atomic"

// Chan - user-friendly chan without "<-" and "->"
type Chan[T any] struct {
	closed atomic.Bool
	ch     chan T
}

// NewChan - create a new Chan with data transfer type T
func NewChan[T any]() *Chan[T] {
	return &Chan[T]{}
}

// Original - get a go chan (not sure you really want it)
func (c *Chan[T]) Original() chan T {
	return c.ch
}

// Write - panic-free putting a value in the Chan (blocking call until any eg.Thread or goroutine reads from it, does nothing if the Chan is closed)
func (c *Chan[T]) Write(v T) {
	if c.closed.Load(){
		return
	}

	c.ch <- v
}

// Read - read a value from the Chan (blocking call until any eg.Thread or goroutine writes anything in it)
func (c *Chan[T]) Read() T {
	if c.closed.Load(){
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
