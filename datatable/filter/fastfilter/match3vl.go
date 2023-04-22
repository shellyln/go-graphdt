package fastfilter

import (
	"regexp"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func NullableMatch3VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		var re *regexp.Regexp

		if c.Valid {
			var err error
			re, err = regexp.Compile(string(c.Value))
			if err != nil {
				return err
			}
		}

		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if re.Match([]byte(v.Value)) {
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

func NullableNotMatch3VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		var re *regexp.Regexp

		if c.Valid {
			var err error
			re, err = regexp.Compile(string(c.Value))
			if err != nil {
				return err
			}
		}

		// TODO: return another function when c.Valid == false
		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if !re.Match([]byte(v.Value)) {
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
