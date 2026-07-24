package eg

type Slice[T any] struct {
	sl []T
}

func NewSlice[T any](elems ...T) *Slice[T] {
	return &Slice[T]{
		sl: elems,
	}
}

func (s *Slice[T]) Original() []T {
	return s.sl
}

func (s *Slice[T]) Size() int64 {
	return int64(len(s.sl))
}

func (s *Slice[T]) Empty() bool {
	return len(s.sl) == 0
}

func (s *Slice[T]) Erase() {
	s.sl = []T{}
}

func (s *Slice[T]) Get(idx int64) T {
	if idx >= s.Size() || idx < 0 {
		var t T
		return t
	}

	return s.sl[idx]
}

func (s *Slice[T]) Set(idx int64, value T) {
	if idx < 0 {
		return
	}

	if idx >= s.Size() {
		zeros := make([]T, idx-s.Size()+1)
		s.sl = append(s.sl, zeros...)
	}

	s.sl[idx] = value
}

func (s *Slice[T]) Append(elems ...T) *Slice[T] {
	s.sl = append(s.sl, elems...)
	return s
}

func (s *Slice[T]) Prepend(elems ...T) *Slice[T] {
	s.sl = append(elems, s.sl...)
	return s
}

func (s *Slice[T]) Extend(another *Slice[T]) *Slice[T] {
	s.sl = append(s.sl, another.Original()...)
	return s
}
