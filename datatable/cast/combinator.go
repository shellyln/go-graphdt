package cast

import (
	"reflect"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func CastFuncOf[R any](fn func(v interface{}) (R, bool)) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		return fn(v)
	}
}

func WrapNullable[T any, R any](fn func(v T) (R, bool)) func(v T) (Nullable[R], bool) {
	return func(v T) (ret Nullable[R], ok bool) {
		var z R
		z, ok = fn(v)
		if ok {
			ret.Valid = true
			ret.Value = z
		}
		return
	}
}

func UnwrapNullable[T any, R any](fn func(v T) (R, bool)) func(v Nullable[T]) (R, bool) {
	return func(v Nullable[T]) (ret R, ok bool) {
		if v.Valid {
			var z R
			z, ok = fn(v.Value)
			if ok {
				ret = z
			}
		} else {
			ok = true
		}
		return
	}
}

func AsNullable[T any, R any](fn func(v T) (R, bool)) func(v Nullable[T]) (Nullable[R], bool) {
	return func(v Nullable[T]) (ret Nullable[R], ok bool) {
		if v.Valid {
			var z R
			z, ok = fn(v.Value)
			if ok {
				ret.Valid = true
				ret.Value = z
			}
		} else {
			ok = true
		}
		return
	}
}

func ArrayOf[T any, R any](fn func(v T) (R, bool)) func(v interface{}) ([]R, bool) {
	if areSameType[T, R]() {
		// R -> R
		return RawCastToItself[[]R]
	} else {
		return func(v interface{}) ([]R, bool) {
			if v != nil {
				// T -> R
				if src, ok := v.([]T); ok {
					length := len(src)
					dst := make([]R, length)
					for i := 0; i < length; i++ {
						if z, ok := fn(src[i]); ok {
							dst[i] = z
						} else {
							return nil, false
						}
					}
					return dst, true
				}
				// any -> R
				if areSameType[T, interface{}]() {
					rv := reflect.ValueOf(v)
					rt := rv.Type()
					switch rt.Kind() {
					case reflect.Array, reflect.Slice:
						// do nothing
					default:
						return nil, false
					}
					length := rv.Len()
					dst := make([]R, length)
					for i := 0; i < length; i++ {
						if z, ok := fn(rv.Index(i).Interface().(T)); ok {
							dst[i] = z
						} else {
							return nil, false
						}
					}
					return dst, true
				}
			}
			return nil, false
		}
	}
}

func Combine(funcs ...CastFunc) CastFunc {
	return func(v interface{}) (interface{}, bool) {
		for _, fn := range funcs {
			if z, ok := fn(v); ok {
				return z, true
			}
		}
		return nil, false
	}
}
