package eg

// Chan - user-friendly chan without "<-" and "->"
type Chan[T any] struct {
	ch chan T
}

// NewChan - create a new Chan with data transfer type T
func NewChan[T any]() *Chan[T] {
	return &Chan[T]{}
}

// Original - get a go chan (not sure you really want it)
func (c *Chan[T]) Original() chan T {
	return c.ch
}

// Write - put a value in the Chan (blocking call until any eg.Thread or goroutine reads from it)
func (c *Chan[T]) Write(v T) {
	c.ch <- v
}

// Read - read a value from the Chan (blocking call until any eg.Thread or goroutine writes anything in it)
func (c *Chan[T]) Read() T {
	return <-c.ch
}
