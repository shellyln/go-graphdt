package cast

import (
	"reflect"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func areSameType[T any, R any]() bool {
	var t T
	var r R
	return reflect.TypeOf(t) == reflect.TypeOf(r)
}

func Fail(v interface{}) (ret interface{}, ok bool) {
	return
}

func FailTR[T, R any](v T) (ret R, ok bool) {
	return
}

func FailR[R any](v interface{}) (ret R, ok bool) {
	return
}

func RawCastToItself[T any](v interface{}) (ret T, ok bool) {
	ret, ok = v.(T)
	return
}

func CastToItself[T any](v interface{}) (ret interface{}, ok bool) {
	ret, ok = v.(T)
	return
}

func NewRawCastFunc[T any, R any](fn func(v T) (R, bool)) func(v interface{}) (R, bool) {
	if areSameType[T, R]() {
		return RawCastToItself[R]
	} else {
		return func(v interface{}) (R, bool) {
			var z T
			var ok bool
			if v != nil {
				z, ok = v.(T)
				if !ok {
					var r R
					return r, ok
				}
			}
			return fn(z)
		}
	}
}

func NewCastFunc[T any, R any](fn func(v T) (R, bool)) CastFunc {
	castFn := NewRawCastFunc(fn)
	return func(v interface{}) (interface{}, bool) {
		return castFn(v)
	}
}

func NewRawArrayCastFunc[T any, R any](fn func(v T) (R, bool)) func(v interface{}) ([]R, bool) {
	if areSameType[T, R]() {
		return RawCastToItself[[]R]
	} else {
		return func(v interface{}) ([]R, bool) {
			var src []T
			var ok bool
			if v != nil {
				src, ok = v.([]T)
				if !ok {
					return nil, false
				}
			}
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
	}
}

func NewArrayCastFunc[T any, R any](fn func(v T) (R, bool)) CastFunc {
	castFn := NewRawArrayCastFunc(fn)
	return func(v interface{}) (interface{}, bool) {
		return castFn(v)
	}
}

func GenerateCastFunc[T any, R any](from, to DataColumnType, fn func(v T) (R, bool)) CastFunc {
	if from&Type_Flag_Array != to&Type_Flag_Array {
		return Fail
	}
	if from&Type_Flag_Array == 0 {
		if (from & Type_Flag_Nullable & to & Type_Flag_Nullable) != 0 {
			// nullable -> nullable
			return NewCastFunc(AsNullable(fn))
		} else if (from&Type_Flag_Nullable | to&Type_Flag_Nullable) == 0 {
			// non null -> non null
			return NewCastFunc(fn)
		} else if from&Type_Flag_Nullable == 0 {
			// non null -> nullable
			return NewCastFunc(WrapNullable(fn))
		} else {
			// nullable -> non null
			return NewCastFunc(UnwrapNullable(fn))
		}
	} else {
		if (from & Type_Flag_Nullable & to & Type_Flag_Nullable) != 0 {
			// nullable -> nullable
			return NewArrayCastFunc(AsNullable(fn))
		} else if (from&Type_Flag_Nullable | to&Type_Flag_Nullable) == 0 {
			// non null -> non null
			return NewArrayCastFunc(fn)
		} else if from&Type_Flag_Nullable == 0 {
			// non null -> nullable
			return NewArrayCastFunc(WrapNullable(fn))
		} else {
			// nullable -> non null
			return NewArrayCastFunc(UnwrapNullable(fn))
		}
	}
}
