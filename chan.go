package eg

type Chan[T any] struct {
	ch chan T
}

func NewChan[T any]() *Chan[T] {
	return &Chan[T]{}
}

func (c *Chan[T]) Original() chan T {
	return c.ch
}

func (c *Chan[T]) Write(v T) {
	c.ch <- v
}

func (c *Chan[T]) Read() T {
	return <-c.ch
}
