package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func XEquals[T any](dc *datacolimpl.DataColumnImpl[T], c T, cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if cmpEq(v, c) {
				return True3VL
			} else {
				return False3VL
			}
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

func XNullableEquals2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if cmpEq(v.Value, c.Value) {
					return True3VL
				} else {
					return False3VL
				}
			} else if v.Valid == c.Valid {
				return True3VL
			} else {
				return False3VL
			}
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

func XNotEquals[T any](dc *datacolimpl.DataColumnImpl[T], c T, cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if !cmpEq(v, c) {
				return True3VL
			} else {
				return False3VL
			}
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

func XNullableNotEquals2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if !cmpEq(v.Value, c.Value) {
					return True3VL
				} else {
					return False3VL
				}
			} else if v.Valid != c.Valid {
				return True3VL
			} else {
				return False3VL
			}
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

// True if \exists c \in cs : c == v (2VL)
// -> c_0 == v or ... or c_n == v (2VL)
func XIn[T any](dc *datacolimpl.DataColumnImpl[T], cs []T, cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			for _, c := range cs {
				if cmpEq(v, c) {
					return True3VL
				}
			}
			return False3VL
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

// True if \exists c \in cs : c == v (2VL)
// -> c_0 == v or ... or c_n == v (2VL)
func XNullableIn2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			for _, c := range cs {
				if v.Valid && c.Valid {
					if cmpEq(v.Value, c.Value) {
						return True3VL
					}
				} else if v.Valid == c.Valid {
					return True3VL
				}
			}
			return False3VL
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

// True if \forall c \in cs : c != v (2VL)
// -> c_0 != v and ... and c_n != v (2VL)
func XNotIn[T any](dc *datacolimpl.DataColumnImpl[T], cs []T, cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			for _, c := range cs {
				if cmpEq(v, c) {
					return False3VL
				}
			}
			return True3VL
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

// True if \forall c \in cs : c != v (2VL)
// -> c_0 != v and ... and c_n != v (2VL)
func XNullableNotIn2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			for _, c := range cs {
				if v.Valid && c.Valid {
					if cmpEq(v.Value, c.Value) {
						return False3VL
					}
				} else if v.Valid == c.Valid {
					return False3VL
				}
			}
			return True3VL
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}
