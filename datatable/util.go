package datatable

// Insert a value into a slice.
func insert[T any](dst []T, index int, value T) []T {
	var zero T
	dst = append(dst, zero)
	copy(dst[index+1:], dst[index:])
	dst[index] = value
	return dst
}
