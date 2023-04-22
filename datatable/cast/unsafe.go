package cast

// Internal memory layout of `interface{}`
type rawInterface struct {
	Typ uintptr
	Ptr uintptr //unsafe.Pointer
}

type rawSliceHeader struct {
	Data uintptr //unsafe.Pointer
	Len  int
	Cap  int
}
