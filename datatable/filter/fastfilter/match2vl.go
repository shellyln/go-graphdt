package fastfilter

import (
	"regexp"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Match[T ~string](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		re, err := regexp.Compile(string(c))
		if err != nil {
			return err
		}

		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if re.Match([]byte(v)) {
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

func NullableMatch2VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		var re *regexp.Regexp

		if c.Valid {
			var err error
			re, err = regexp.Compile(string(c.Value))
			if err != nil {
				return err
			}
		}

		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if re.Match([]byte(v.Value)) {
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

func NotMatch[T ~string](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		re, err := regexp.Compile(string(c))
		if err != nil {
			return err
		}

		fn := dc.ApplyFilterFunc(func(v T) Bool3VL {
			if !re.Match([]byte(v)) {
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

func NullableNotMatch2VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		var re *regexp.Regexp

		if c.Valid {
			var err error
			re, err = regexp.Compile(string(c.Value))
			if err != nil {
				return err
			}
		}

		fn := dc.ApplyFilterFunc(func(v Nullable[T]) Bool3VL {
			if v.Valid && c.Valid {
				if !re.Match([]byte(v.Value)) {
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
