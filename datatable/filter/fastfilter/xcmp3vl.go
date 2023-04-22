package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func XNullableEquals3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if cmpEq(v.Value, c.Value) {
					return True3VL
				} else {
					return False3VL
				}
			}
			return Unknown3VL
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

func XNullableNotEquals3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if !cmpEq(v.Value, c.Value) {
					return True3VL
				} else {
					return False3VL
				}
			}
			return Unknown3VL
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

// True if \exists c \in cs : c == v (3VL)
// -> c_0 == v or ... or c_n == v (3VL)
func XNullableIn3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			ret := False3VL
			for _, c := range cs {
				if v.Valid && c.Valid {
					if cmpEq(v.Value, c.Value) {
						return True3VL
					}
				} else {
					ret = Unknown3VL
				}
			}
			return ret
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

// True if \forall c \in cs : c != v (3VL)
// -> c_0 != v and ... and c_n != v (3VL)
func XNullableNotIn3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T], cmpEq func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			ret := True3VL
			for _, c := range cs {
				if v.Valid && c.Valid {
					if cmpEq(v.Value, c.Value) {
						return False3VL
					}
				} else {
					ret = Unknown3VL
				}
			}
			return ret
		})

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}
