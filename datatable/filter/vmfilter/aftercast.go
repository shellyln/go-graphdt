package vmfilter

import (
	"regexp"
	"strings"

	. "github.com/shellyln/go-graphdt/datatable/types"
	"github.com/shellyln/go-sql-like-expr/likeexpr"
)

// []string -> [][]string
func CastIncludesOp2(castFn CastFunc) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		cv, ok := castFn(v)
		if !ok {
			return nil, false
		}

		tv, ok := cv.([]string)
		if !ok {
			return nil, false
		}

		length := len(tv)
		cs := make([][]string, length)

		for i := 0; i < length; i++ {
			p := tv[i]
			w := strings.Split(string(p), ";")
			for j, q := range w {
				w[j] = ";" + q + ";"
			}
			cs[i] = w
		}
		return cs, true
	}
}

// []Nullable[string] -> []Nullable[[]string]
func CastIncludesNullableOp2(castFn CastFunc) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		cv, ok := castFn(v)
		if !ok {
			return nil, false
		}

		tv, ok := cv.([]Nullable[string])
		if !ok {
			return nil, false
		}

		length := len(tv)
		cs := make([]Nullable[[]string], length)

		for i := 0; i < length; i++ {
			p := tv[i]
			if p.Valid {
				w := strings.Split(string(p.Value), ";")
				for j, q := range w {
					w[j] = ";" + q + ";"
				}
				cs[i].Valid = true
				cs[i].Value = w
			}
		}
		return cs, true
	}
}

// string -> *regexp.Regexp
func CastLikeOp2(castFn CastFunc) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		cv, ok := castFn(v)
		if !ok {
			return nil, false
		}

		tv, ok := cv.(string)
		if !ok {
			return nil, false
		}

		re, err := regexp.Compile(likeexpr.ToRegexp(tv, '\\', true))
		if err != nil {
			return nil, false
		}
		return re, true
	}
}

// Nullable[string] -> Nullable[*regexp.Regexp]
func CastLikeNullableOp2(castFn CastFunc) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		cv, ok := castFn(v)
		if !ok {
			return nil, false
		}

		tv, ok := cv.(Nullable[string])
		if !ok {
			return nil, false
		}

		var ret Nullable[*regexp.Regexp]
		if tv.Valid {
			re, err := regexp.Compile(likeexpr.ToRegexp(tv.Value, '\\', true))
			if err != nil {
				return nil, false
			}
			ret.Valid = true
			ret.Value = re
		}
		return ret, true
	}
}

// string -> *regexp.Regexp
func CastMatchOp2(castFn CastFunc) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		cv, ok := castFn(v)
		if !ok {
			return nil, false
		}

		tv, ok := cv.(string)
		if !ok {
			return nil, false
		}

		re, err := regexp.Compile(tv)
		if err != nil {
			return nil, false
		}
		return re, true
	}
}

// Nullable[string] -> Nullable[*regexp.Regexp]
func CastMatchNullableOp2(castFn CastFunc) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		cv, ok := castFn(v)
		if !ok {
			return nil, false
		}

		tv, ok := cv.(Nullable[string])
		if !ok {
			return nil, false
		}

		var ret Nullable[*regexp.Regexp]
		if tv.Valid {
			re, err := regexp.Compile(tv.Value)
			if err != nil {
				return nil, false
			}
			ret.Valid = true
			ret.Value = re
		}
		return ret, true
	}
}
