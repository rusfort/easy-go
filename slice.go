package eg

type Slice[T any] struct {
	sl []T
}

func NewSlice[T any](elems ...T) *Slice[T] {
	return &Slice[T]{
		sl: elems,
	}
}

func NewSliceOfSize[T any](size int64) *Slice[T] {
	return &Slice[T]{
		sl: make([]T, 0, size),
	}
}

func (s *Slice[T]) Original() []T {
	return s.sl
}

func (s *Slice[T]) Size() int64 {
	return int64(len(s.sl))
}

func (s *Slice[T]) IsEmpty() bool {
	return len(s.sl) == 0
}

func (s *Slice[T]) Vanish() {
	s.sl = []T{}
}

func (s *Slice[T]) ResetToDefault() {
	s.sl = make([]T, s.Size())
}

func (s *Slice[T]) Erase() {
	s.sl = make([]T, 0, s.Size())
}

func (s *Slice[T]) Get(idx int64) T {
	if idx >= s.Size() || idx < 0 {
		var t T
		return t
	}

	return s.sl[idx]
}

func (s *Slice[T]) Enlarge(newSize int64) {
	if newSize <= s.Size() {
		return
	}

	zeros := make([]T, newSize-s.Size())
	s.sl = append(s.sl, zeros...)
}

func (s *Slice[T]) Set(idx int64, value T) {
	if idx < 0 {
		return
	}

	if idx >= s.Size() {
		s.Enlarge(idx + 1)
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

func (s *Slice[T]) Cut(start, end int64) *Slice[T] {
	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	if start >= end {
		return NewSlice[T]()
	}

	if end > s.Size() {
		end = s.Size()
	}

	sl := s.sl[start:end]

	return NewSlice(sl...)
}
