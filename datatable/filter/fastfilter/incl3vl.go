package fastfilter

import (
	"strings"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// True if \exists c \in cs : c == v (3VL)
// -> c_0 == v or ... or c_n == v (3VL)
func NullableIncludes3VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
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
			ret := False3VL
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
func NullableExcludes3VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], cs []Nullable[T]) FilterGenFunc {
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
			ret := True3VL
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
