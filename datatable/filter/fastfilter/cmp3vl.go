package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func NullableEquals3VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value == c.Value {
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

func NullableIsNull[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid {
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

func NullableNotEquals3VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value != c.Value {
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

func NullableIsNotNull[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if !v.Valid {
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

// True if \exists c \in cs : c == v (3VL)
// -> c_0 == v or ... or c_n == v (3VL)
func NullableIn3VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			ret := False3VL
			for _, c := range cs {
				if v.Valid && c.Valid {
					if v.Value == c.Value {
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
func NullableNotIn3VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			ret := True3VL
			for _, c := range cs {
				if v.Valid && c.Valid {
					if v.Value == c.Value {
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
