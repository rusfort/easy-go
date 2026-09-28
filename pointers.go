package eg

func ToPtr[T comparable](t T) *T {
	ptr := new(T)
	if t == *ptr {
		return nil
	}
	return ptr
}
