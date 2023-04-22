package datacolimpl

func dummyCastElem[T any](v interface{}) (r T, ok bool) {
	r, ok = v.(T)
	return
}

func dummyCastArray[T any](v interface{}) (r []T, ok bool) {
	r, ok = v.([]T)
	return
}

func dummyCheckNull[T any](v T) bool {
	return false
}
