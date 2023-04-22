package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func NullableLessThan3VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value < c.Value {
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

func NullableLessThanOrEquals3VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value <= c.Value {
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

func NullableGreaterThan3VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value > c.Value {
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

func NullableGreaterThanOrEquals3VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value >= c.Value {
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
