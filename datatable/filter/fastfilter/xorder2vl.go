package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func XLessThan[T any](dc *datacolimpl.DataColumnImpl[T], c T, cmpLt func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if cmpLt(v, c) {
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

func XNullableLessThan2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLt func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && cmpLt(v.Value, c.Value) {
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

func XLessThanOrEquals[T any](dc *datacolimpl.DataColumnImpl[T], c T, cmpLe func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if cmpLe(v, c) {
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

func XNullableLessThanOrEquals2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLe func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && cmpLe(v.Value, c.Value) {
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

func XGreaterThan[T any](dc *datacolimpl.DataColumnImpl[T], c T, cmpLt func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if cmpLt(c, v) {
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

func XNullableGreaterThan2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLt func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && cmpLt(c.Value, v.Value) {
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

func XGreaterThanOrEquals[T any](dc *datacolimpl.DataColumnImpl[T], c T, cmpLe func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if cmpLe(c, v) {
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

func XNullableGreaterThanOrEquals2VL[T any](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T], cmpLe func(p, q T) bool) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && cmpLe(c.Value, v.Value) {
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
