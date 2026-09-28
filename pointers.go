package eg

func ToPtr[T comparable](t T) *T {
	ptr := new(T)

	if t == *ptr {
		return nil
	}

	*ptr = t
	return ptr
}

func FromPtr[T comparable](ptr *T) T {
	if ptr == nil {
		var t T
		return t
	}

	return *ptr
}
