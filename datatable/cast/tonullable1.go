package cast

import (
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func ToNullableAny(v interface{}) (Nullable[interface{}], bool) {
	if v != nil {
		if r, ok := v.(Nullable[interface{}]); ok {
			return r, true
		} else {
			return Nullable[interface{}]{
				Valid: true,
				Value: v,
			}, true
		}
	} else {
		return Nullable[interface{}]{}, true
	}
}

func ToNullableInt[R Int](v interface{}) (Nullable[R], bool) {
	ret, ok := v.(Nullable[R])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastNumberToNumber[int8, R])(v2)
	case int16:
		return WrapNullable(CastNumberToNumber[int16, R])(v2)
	case int32:
		return WrapNullable(CastNumberToNumber[int32, R])(v2)
	case int64:
		return WrapNullable(CastNumberToNumber[int64, R])(v2)
	case int:
		return WrapNullable(CastNumberToNumber[int, R])(v2)
	case uint8:
		return WrapNullable(CastNumberToNumber[uint8, R])(v2)
	case uint16:
		return WrapNullable(CastNumberToNumber[uint16, R])(v2)
	case uint32:
		return WrapNullable(CastNumberToNumber[uint32, R])(v2)
	case uint64:
		return WrapNullable(CastNumberToNumber[uint64, R])(v2)
	case uint:
		return WrapNullable(CastNumberToNumber[uint, R])(v2)
	case uintptr:
		return WrapNullable(CastNumberToNumber[uintptr, R])(v2)
	case float32:
		return WrapNullable(CastNumberToNumber[float32, R])(v2)
	case float64:
		return WrapNullable(CastNumberToNumber[float64, R])(v2)
	case complex64:
		return WrapNullable(CastComplexToNumber[complex64, R])(v2)
	case complex128:
		return WrapNullable(CastComplexToNumber[complex128, R])(v2)
	case bool:
		return WrapNullable(CastBoolToNumber[bool, R])(v2)
	case string:
		return WrapNullable(CastStringToInt[string, R])(v2)
	case time.Time:
		return WrapNullable(CastTimeToNumber[R])(v2)
	case TimeRange:
		return WrapNullable(CastTimeRangeToNumber[R])(v2)

	case Nullable[int8]:
		return AsNullable(CastNumberToNumber[int8, R])(v2)
	case Nullable[int16]:
		return AsNullable(CastNumberToNumber[int16, R])(v2)
	case Nullable[int32]:
		return AsNullable(CastNumberToNumber[int32, R])(v2)
	case Nullable[int64]:
		return AsNullable(CastNumberToNumber[int64, R])(v2)
	case Nullable[int]:
		return AsNullable(CastNumberToNumber[int, R])(v2)
	case Nullable[uint8]:
		return AsNullable(CastNumberToNumber[uint8, R])(v2)
	case Nullable[uint16]:
		return AsNullable(CastNumberToNumber[uint16, R])(v2)
	case Nullable[uint32]:
		return AsNullable(CastNumberToNumber[uint32, R])(v2)
	case Nullable[uint64]:
		return AsNullable(CastNumberToNumber[uint64, R])(v2)
	case Nullable[uint]:
		return AsNullable(CastNumberToNumber[uint, R])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastNumberToNumber[uintptr, R])(v2)
	case Nullable[float32]:
		return AsNullable(CastNumberToNumber[float32, R])(v2)
	case Nullable[float64]:
		return AsNullable(CastNumberToNumber[float64, R])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToNumber[complex64, R])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToNumber[complex128, R])(v2)
	case Nullable[bool]:
		return AsNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToInt[string, R])(v2)
	case Nullable[time.Time]:
		return AsNullable(CastTimeToNumber[R])(v2)
	case Nullable[TimeRange]:
		return AsNullable(CastTimeRangeToNumber[R])(v2)

	default:
		return ret, false
	}
}

func ToNullableUint[R Uint](v interface{}) (Nullable[R], bool) {
	ret, ok := v.(Nullable[R])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastNumberToNumber[int8, R])(v2)
	case int16:
		return WrapNullable(CastNumberToNumber[int16, R])(v2)
	case int32:
		return WrapNullable(CastNumberToNumber[int32, R])(v2)
	case int64:
		return WrapNullable(CastNumberToNumber[int64, R])(v2)
	case int:
		return WrapNullable(CastNumberToNumber[int, R])(v2)
	case uint8:
		return WrapNullable(CastNumberToNumber[uint8, R])(v2)
	case uint16:
		return WrapNullable(CastNumberToNumber[uint16, R])(v2)
	case uint32:
		return WrapNullable(CastNumberToNumber[uint32, R])(v2)
	case uint64:
		return WrapNullable(CastNumberToNumber[uint64, R])(v2)
	case uint:
		return WrapNullable(CastNumberToNumber[uint, R])(v2)
	case uintptr:
		return WrapNullable(CastNumberToNumber[uintptr, R])(v2)
	case float32:
		return WrapNullable(CastNumberToNumber[float32, R])(v2)
	case float64:
		return WrapNullable(CastNumberToNumber[float64, R])(v2)
	case complex64:
		return WrapNullable(CastComplexToNumber[complex64, R])(v2)
	case complex128:
		return WrapNullable(CastComplexToNumber[complex128, R])(v2)
	case bool:
		return WrapNullable(CastBoolToNumber[bool, R])(v2)
	case string:
		return WrapNullable(CastStringToUint[string, R])(v2)
	case time.Time:
		return WrapNullable(CastTimeToNumber[R])(v2)
	case TimeRange:
		return WrapNullable(CastTimeRangeToNumber[R])(v2)

	case Nullable[int8]:
		return AsNullable(CastNumberToNumber[int8, R])(v2)
	case Nullable[int16]:
		return AsNullable(CastNumberToNumber[int16, R])(v2)
	case Nullable[int32]:
		return AsNullable(CastNumberToNumber[int32, R])(v2)
	case Nullable[int64]:
		return AsNullable(CastNumberToNumber[int64, R])(v2)
	case Nullable[int]:
		return AsNullable(CastNumberToNumber[int, R])(v2)
	case Nullable[uint8]:
		return AsNullable(CastNumberToNumber[uint8, R])(v2)
	case Nullable[uint16]:
		return AsNullable(CastNumberToNumber[uint16, R])(v2)
	case Nullable[uint32]:
		return AsNullable(CastNumberToNumber[uint32, R])(v2)
	case Nullable[uint64]:
		return AsNullable(CastNumberToNumber[uint64, R])(v2)
	case Nullable[uint]:
		return AsNullable(CastNumberToNumber[uint, R])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastNumberToNumber[uintptr, R])(v2)
	case Nullable[float32]:
		return AsNullable(CastNumberToNumber[float32, R])(v2)
	case Nullable[float64]:
		return AsNullable(CastNumberToNumber[float64, R])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToNumber[complex64, R])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToNumber[complex128, R])(v2)
	case Nullable[bool]:
		return AsNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToUint[string, R])(v2)
	case Nullable[time.Time]:
		return AsNullable(CastTimeToNumber[R])(v2)
	case Nullable[TimeRange]:
		return AsNullable(CastTimeRangeToNumber[R])(v2)

	default:
		return ret, false
	}
}

func ToNullableFloat[R Float](v interface{}) (Nullable[R], bool) {
	ret, ok := v.(Nullable[R])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastNumberToNumber[int8, R])(v2)
	case int16:
		return WrapNullable(CastNumberToNumber[int16, R])(v2)
	case int32:
		return WrapNullable(CastNumberToNumber[int32, R])(v2)
	case int64:
		return WrapNullable(CastNumberToNumber[int64, R])(v2)
	case int:
		return WrapNullable(CastNumberToNumber[int, R])(v2)
	case uint8:
		return WrapNullable(CastNumberToNumber[uint8, R])(v2)
	case uint16:
		return WrapNullable(CastNumberToNumber[uint16, R])(v2)
	case uint32:
		return WrapNullable(CastNumberToNumber[uint32, R])(v2)
	case uint64:
		return WrapNullable(CastNumberToNumber[uint64, R])(v2)
	case uint:
		return WrapNullable(CastNumberToNumber[uint, R])(v2)
	case uintptr:
		return WrapNullable(CastNumberToNumber[uintptr, R])(v2)
	case float32:
		return WrapNullable(CastNumberToNumber[float32, R])(v2)
	case float64:
		return WrapNullable(CastNumberToNumber[float64, R])(v2)
	case complex64:
		return WrapNullable(CastComplexToNumber[complex64, R])(v2)
	case complex128:
		return WrapNullable(CastComplexToNumber[complex128, R])(v2)
	case bool:
		return WrapNullable(CastBoolToNumber[bool, R])(v2)
	case string:
		return WrapNullable(CastStringToFloat[string, R])(v2)
	case time.Time:
		return WrapNullable(CastTimeToNumber[R])(v2)
	case TimeRange:
		return WrapNullable(CastTimeRangeToNumber[R])(v2)

	case Nullable[int8]:
		return AsNullable(CastNumberToNumber[int8, R])(v2)
	case Nullable[int16]:
		return AsNullable(CastNumberToNumber[int16, R])(v2)
	case Nullable[int32]:
		return AsNullable(CastNumberToNumber[int32, R])(v2)
	case Nullable[int64]:
		return AsNullable(CastNumberToNumber[int64, R])(v2)
	case Nullable[int]:
		return AsNullable(CastNumberToNumber[int, R])(v2)
	case Nullable[uint8]:
		return AsNullable(CastNumberToNumber[uint8, R])(v2)
	case Nullable[uint16]:
		return AsNullable(CastNumberToNumber[uint16, R])(v2)
	case Nullable[uint32]:
		return AsNullable(CastNumberToNumber[uint32, R])(v2)
	case Nullable[uint64]:
		return AsNullable(CastNumberToNumber[uint64, R])(v2)
	case Nullable[uint]:
		return AsNullable(CastNumberToNumber[uint, R])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastNumberToNumber[uintptr, R])(v2)
	case Nullable[float32]:
		return AsNullable(CastNumberToNumber[float32, R])(v2)
	case Nullable[float64]:
		return AsNullable(CastNumberToNumber[float64, R])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToNumber[complex64, R])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToNumber[complex128, R])(v2)
	case Nullable[bool]:
		return AsNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToFloat[string, R])(v2)
	case Nullable[time.Time]:
		return AsNullable(CastTimeToNumber[R])(v2)
	case Nullable[TimeRange]:
		return AsNullable(CastTimeRangeToNumber[R])(v2)

	default:
		return ret, false
	}
}

func ToNullableComplex[R Complex](v interface{}) (Nullable[R], bool) {
	ret, ok := v.(Nullable[R])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastNumberToComplex[int8, R])(v2)
	case int16:
		return WrapNullable(CastNumberToComplex[int16, R])(v2)
	case int32:
		return WrapNullable(CastNumberToComplex[int32, R])(v2)
	case int64:
		return WrapNullable(CastNumberToComplex[int64, R])(v2)
	case int:
		return WrapNullable(CastNumberToComplex[int, R])(v2)
	case uint8:
		return WrapNullable(CastNumberToComplex[uint8, R])(v2)
	case uint16:
		return WrapNullable(CastNumberToComplex[uint16, R])(v2)
	case uint32:
		return WrapNullable(CastNumberToComplex[uint32, R])(v2)
	case uint64:
		return WrapNullable(CastNumberToComplex[uint64, R])(v2)
	case uint:
		return WrapNullable(CastNumberToComplex[uint, R])(v2)
	case uintptr:
		return WrapNullable(CastNumberToComplex[uintptr, R])(v2)
	case float32:
		return WrapNullable(CastNumberToComplex[float32, R])(v2)
	case float64:
		return WrapNullable(CastNumberToComplex[float64, R])(v2)
	case complex64:
		return WrapNullable(CastComplexToComplex[complex64, R])(v2)
	case complex128:
		return WrapNullable(CastComplexToComplex[complex128, R])(v2)
	case bool:
		return WrapNullable(CastBoolToNumber[bool, R])(v2)
	case string:
		return WrapNullable(CastStringToComplex[string, R])(v2)
	case time.Time:
		return WrapNullable(CastTimeToComplex[R])(v2)
	case TimeRange:
		return WrapNullable(CastTimeRangeToComplex[R])(v2)

	case Nullable[int8]:
		return AsNullable(CastNumberToComplex[int8, R])(v2)
	case Nullable[int16]:
		return AsNullable(CastNumberToComplex[int16, R])(v2)
	case Nullable[int32]:
		return AsNullable(CastNumberToComplex[int32, R])(v2)
	case Nullable[int64]:
		return AsNullable(CastNumberToComplex[int64, R])(v2)
	case Nullable[int]:
		return AsNullable(CastNumberToComplex[int, R])(v2)
	case Nullable[uint8]:
		return AsNullable(CastNumberToComplex[uint8, R])(v2)
	case Nullable[uint16]:
		return AsNullable(CastNumberToComplex[uint16, R])(v2)
	case Nullable[uint32]:
		return AsNullable(CastNumberToComplex[uint32, R])(v2)
	case Nullable[uint64]:
		return AsNullable(CastNumberToComplex[uint64, R])(v2)
	case Nullable[uint]:
		return AsNullable(CastNumberToComplex[uint, R])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastNumberToComplex[uintptr, R])(v2)
	case Nullable[float32]:
		return AsNullable(CastNumberToComplex[float32, R])(v2)
	case Nullable[float64]:
		return AsNullable(CastNumberToComplex[float64, R])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToComplex[complex64, R])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToComplex[complex128, R])(v2)
	case Nullable[bool]:
		return AsNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToComplex[string, R])(v2)
	case Nullable[time.Time]:
		return AsNullable(CastTimeToComplex[R])(v2)
	case Nullable[TimeRange]:
		return AsNullable(CastTimeRangeToComplex[R])(v2)

	default:
		return ret, false
	}
}
