package fastfilter

import (
	"strings"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// True if \exists c \in cs : c == v (2VL)
// -> c_0 == v or ... or c_n == v (2VL)
func Includes[T ~string](dc *datacolimpl.DataColumnImpl[T], cs []T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		cs2 := make([][]string, len(cs))
		for i, p := range cs {
			w := strings.Split(string(p), ";")
			for j, q := range w {
				w[j] = ";" + q + ";"
			}
			cs2[i] = w
		}

		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
		OUTER:
			for _, c := range cs2 {
				s := ";" + string(v) + ";"
				for _, needle := range c {
					if !strings.Contains(s, needle) {
						continue OUTER
					}
				}
				return True3VL
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
func NullableIncludes2VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		cs2 := make([]Nullable[[]string], len(cs))
		for i, p := range cs {
			if p.Valid {
				w := strings.Split(string(p.Value), ";")
				for j, q := range w {
					w[j] = ";" + q + ";"
				}
				cs2[i].Valid = true
				cs2[i].Value = w
			}
		}

		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
		OUTER:
			for _, c := range cs2 {
				if v.Valid && c.Valid {
					s := ";" + string(v.Value) + ";"
					for _, needle := range c.Value {
						if !strings.Contains(s, needle) {
							continue OUTER
						}
					}
					return True3VL
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
func Excludes[T ~string](dc *datacolimpl.DataColumnImpl[T], cs []T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		cs2 := make([][]string, len(cs))
		for i, p := range cs {
			w := strings.Split(string(p), ";")
			for j, q := range w {
				w[j] = ";" + q + ";"
			}
			cs2[i] = w
		}

		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
		OUTER:
			for _, c := range cs2 {
				s := ";" + string(v) + ";"
				for _, needle := range c {
					if !strings.Contains(s, needle) {
						continue OUTER
					}
				}
				return False3VL
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
func NullableExcludes2VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		cs2 := make([]Nullable[[]string], len(cs))
		for i, p := range cs {
			if p.Valid {
				w := strings.Split(string(p.Value), ";")
				for j, q := range w {
					w[j] = ";" + q + ";"
				}
				cs2[i].Valid = true
				cs2[i].Value = w
			}
		}

		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
		OUTER:
			for _, c := range cs2 {
				if v.Valid && c.Valid {
					s := ";" + string(v.Value) + ";"
					for _, needle := range c.Value {
						if !strings.Contains(s, needle) {
							continue OUTER
						}
					}
					return False3VL
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
