package fastfilter

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Equals[T comparable](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if v == c {
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

func NullableEquals2VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value == c.Value {
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

func NotEquals[T comparable](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if v != c {
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

func NullableNotEquals2VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if v.Value != c.Value {
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
func In[T comparable](dc *datacolimpl.DataColumnImpl[T], cs []T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			for _, c := range cs {
				if v == c {
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
func NullableIn2VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			for _, c := range cs {
				if v.Valid && c.Valid {
					if v.Value == c.Value {
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
func NotIn[T comparable](dc *datacolimpl.DataColumnImpl[T], cs []T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			for _, c := range cs {
				if v == c {
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
func NullableNotIn2VL[T comparable](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			for _, c := range cs {
				if v.Valid && c.Valid {
					if v.Value == c.Value {
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
