package eg

import "sync"

// Slice - user-friendly and concurrent-friendly slice []T without go-style crap like a = append(a, b)
type Slice[T any] struct {
	mtx sync.Mutex
	sl  []T
}

// NewSlice - create new Slice of type T - elems may be empty (NOTE: returns a pointer)
func NewSlice[T any](elems ...T) *Slice[T] {
	return &Slice[T]{
		sl: elems,
	}
}

// NewSlice - create new Slice of type with given go-slice capacity (NOTE: returns a pointer)
func NewSliceOfSize[T any](size int64) *Slice[T] {
	return &Slice[T]{
		sl: make([]T, 0, size),
	}
}

// Original - returns go slice from the inside (just in case you need it)
func (s *Slice[T]) Original() []T {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.sl
}

// Size - size of go slice from the inside (not capacity)
func (s *Slice[T]) Size() int64 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return int64(len(s.sl))
}

// IsEmpty - true if there are no values in it (capacity may not be 0)
func (s *Slice[T]) IsEmpty() bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return len(s.sl) == 0
}

// Vanish - completely vanishes the Slice - 0 len and 0 cap left
func (s *Slice[T]) Vanish() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.sl = []T{}
}

// ResetToDefault - replaces all the values with default of type T (NOTE: not recommended, better use Erase())
func (s *Slice[T]) ResetToDefault() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.sl = make([]T, s.Size())
}

// Erase - drops all the values (size will be 0) but keeps capacity the same as size was
func (s *Slice[T]) Erase() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.sl = make([]T, 0, s.Size())
}

// Get - panic-free "a = b[idx]" operation - returns value if idx is in bounds and default of type T otherwise
func (s *Slice[T]) Get(idx int64) T {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if idx >= s.Size() || idx < 0 {
		var t T
		return t
	}

	return s.sl[idx]
}

// Enlarge - adds some default values of T to the end, does nothing if newSize <= Size()
func (s *Slice[T]) Enlarge(newSize int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if newSize <= s.Size() {
		return
	}

	zeros := make([]T, newSize-s.Size())
	s.sl = append(s.sl, zeros...)
}

// Set - panic-free "b[idx] = a" operation - sets value if idx >= 0 and does nothing otherwise (NOTE: if idx >= Size(), it enlarges the Slice - see Enlarge())
func (s *Slice[T]) Set(idx int64, value T) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if idx < 0 {
		return
	}

	if idx >= s.Size() {
		s.Enlarge(idx + 1)
	}

	s.sl[idx] = value
}

// Append - Python-styled "b = append(b, a)" operation - appends elems to the end of the inner go slice
func (s *Slice[T]) Append(elems ...T) *Slice[T] {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.sl = append(s.sl, elems...)
	return s
}

// Prepend - Python-styled "b = append(a, b)" operation - appends elems before the inner go slice
func (s *Slice[T]) Prepend(elems ...T) *Slice[T] {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.sl = append(elems, s.sl...)
	return s
}

// Extend - the same as Append but joins two objects of eg.Slice - returns the first of them and keeps another unchanged
func (s *Slice[T]) Extend(another *Slice[T]) *Slice[T] {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.sl = append(s.sl, another.Original()...)
	return s
}

// Cut - panic-free slicing of the Slice - returns new Slice with the inner slice as example[start:end] (NOTE: start < end, start >= 0 and end < Size - otherwise it edits start and end to fit in bounds)
func (s *Slice[T]) Cut(start, end int64) *Slice[T] {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if start < 0 {
		start = 0
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

// Copy - safe copy of the Slice object (NOTE: lock is not copied, so the return Slice is completely independent)
func (s *Slice[T]) Copy() *Slice[T] {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return NewSlice(s.sl...)
}
