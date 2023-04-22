package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func LessThan[T Ordered](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if v < c {
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

func NullableLessThan2VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && v.Value < c.Value {
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

func LessThanOrEquals[T Ordered](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if v <= c {
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

func NullableLessThanOrEquals2VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && v.Value <= c.Value {
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

func GreaterThan[T Ordered](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if v > c {
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

func NullableGreaterThan2VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && v.Value > c.Value {
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

func GreaterThanOrEquals[T Ordered](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if v >= c {
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

func NullableGreaterThanOrEquals2VL[T Ordered](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid && v.Value >= c.Value {
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
