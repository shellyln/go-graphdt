package fastfilter

import (
	"regexp"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
	"github.com/shellyln/go-sql-like-expr/likeexpr"
)

func Like[T ~string](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		re, err := regexp.Compile(likeexpr.ToRegexp(string(c), '\\', true))
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

func NullableLike2VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		var re *regexp.Regexp

		if c.Valid {
			var err error
			re, err = regexp.Compile(likeexpr.ToRegexp(string(c.Value), '\\', true))
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

func NotLike[T ~string](dc *datacolimpl.DataColumnImpl[T], c T) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		re, err := regexp.Compile(likeexpr.ToRegexp(string(c), '\\', true))
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

func NullableNotLike2VL[T ~string](dc *datacolimpl.DataColumnImpl[Nullable[T]], c Nullable[T]) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		var re *regexp.Regexp

		if c.Valid {
			var err error
			re, err = regexp.Compile(likeexpr.ToRegexp(string(c.Value), '\\', true))
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
