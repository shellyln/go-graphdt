package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func XNullableLessThan3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLt func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if cmpLt(v.Value, c.Value) {
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

func XNullableLessThanOrEquals3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLe func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if cmpLe(v.Value, c.Value) {
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

func XNullableGreaterThan3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLt func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if cmpLt(c.Value, v.Value) {
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

func XNullableGreaterThanOrEquals3VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLe func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if cmpLe(c.Value, v.Value) {
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
