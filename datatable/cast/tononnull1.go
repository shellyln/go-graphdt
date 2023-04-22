package cast

import (
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

// func foo[R Int]() {
// 	NewRawArrayCastFunc(ToInt[R])
// 	NewRawArrayCastFunc(ToNullableInt[R])
// }

func ToAny(v interface{}) (interface{}, bool) {
	return v, true
}

func ToInt[R Int](v interface{}) (R, bool) {
	ret, ok := v.(R)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastNumberToNumber[int8, R](v2)
	case int16:
		return CastNumberToNumber[int16, R](v2)
	case int32:
		return CastNumberToNumber[int32, R](v2)
	case int64:
		return CastNumberToNumber[int64, R](v2)
	case int:
		return CastNumberToNumber[int, R](v2)
	case uint8:
		return CastNumberToNumber[uint8, R](v2)
	case uint16:
		return CastNumberToNumber[uint16, R](v2)
	case uint32:
		return CastNumberToNumber[uint32, R](v2)
	case uint64:
		return CastNumberToNumber[uint64, R](v2)
	case uint:
		return CastNumberToNumber[uint, R](v2)
	case uintptr:
		return CastNumberToNumber[uintptr, R](v2)
	case float32:
		return CastNumberToNumber[float32, R](v2)
	case float64:
		return CastNumberToNumber[float64, R](v2)
	case complex64:
		return CastComplexToNumber[complex64, R](v2)
	case complex128:
		return CastComplexToNumber[complex128, R](v2)
	case bool:
		return CastBoolToNumber[bool, R](v2)
	case string:
		return CastStringToInt[string, R](v2)
	case time.Time:
		return CastTimeToNumber[R](v2)
	case TimeRange:
		return CastTimeRangeToNumber[R](v2)

	case Nullable[int8]:
		return UnwrapNullable(CastNumberToNumber[int8, R])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastNumberToNumber[int16, R])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastNumberToNumber[int32, R])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastNumberToNumber[int64, R])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastNumberToNumber[int, R])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastNumberToNumber[uint8, R])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastNumberToNumber[uint16, R])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastNumberToNumber[uint32, R])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastNumberToNumber[uint64, R])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastNumberToNumber[uint, R])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastNumberToNumber[uintptr, R])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastNumberToNumber[float32, R])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastNumberToNumber[float64, R])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToNumber[complex64, R])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToNumber[complex128, R])(v2)
	case Nullable[bool]:
		return UnwrapNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToInt[string, R])(v2)
	case Nullable[time.Time]:
		return UnwrapNullable(CastTimeToNumber[R])(v2)
	case Nullable[TimeRange]:
		return UnwrapNullable(CastTimeRangeToNumber[R])(v2)

	default:
		return ret, false
	}
}

func ToUint[R Uint](v interface{}) (R, bool) {
	ret, ok := v.(R)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastNumberToNumber[int8, R](v2)
	case int16:
		return CastNumberToNumber[int16, R](v2)
	case int32:
		return CastNumberToNumber[int32, R](v2)
	case int64:
		return CastNumberToNumber[int64, R](v2)
	case int:
		return CastNumberToNumber[int, R](v2)
	case uint8:
		return CastNumberToNumber[uint8, R](v2)
	case uint16:
		return CastNumberToNumber[uint16, R](v2)
	case uint32:
		return CastNumberToNumber[uint32, R](v2)
	case uint64:
		return CastNumberToNumber[uint64, R](v2)
	case uint:
		return CastNumberToNumber[uint, R](v2)
	case uintptr:
		return CastNumberToNumber[uintptr, R](v2)
	case float32:
		return CastNumberToNumber[float32, R](v2)
	case float64:
		return CastNumberToNumber[float64, R](v2)
	case complex64:
		return CastComplexToNumber[complex64, R](v2)
	case complex128:
		return CastComplexToNumber[complex128, R](v2)
	case bool:
		return CastBoolToNumber[bool, R](v2)
	case string:
		return CastStringToUint[string, R](v2)
	case time.Time:
		return CastTimeToNumber[R](v2)
	case TimeRange:
		return CastTimeRangeToNumber[R](v2)

	case Nullable[int8]:
		return UnwrapNullable(CastNumberToNumber[int8, R])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastNumberToNumber[int16, R])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastNumberToNumber[int32, R])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastNumberToNumber[int64, R])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastNumberToNumber[int, R])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastNumberToNumber[uint8, R])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastNumberToNumber[uint16, R])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastNumberToNumber[uint32, R])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastNumberToNumber[uint64, R])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastNumberToNumber[uint, R])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastNumberToNumber[uintptr, R])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastNumberToNumber[float32, R])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastNumberToNumber[float64, R])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToNumber[complex64, R])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToNumber[complex128, R])(v2)
	case Nullable[bool]:
		return UnwrapNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToUint[string, R])(v2)
	case Nullable[time.Time]:
		return UnwrapNullable(CastTimeToNumber[R])(v2)
	case Nullable[TimeRange]:
		return UnwrapNullable(CastTimeRangeToNumber[R])(v2)

	default:
		return ret, false
	}
}

func ToFloat[R Float](v interface{}) (R, bool) {
	ret, ok := v.(R)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastNumberToNumber[int8, R](v2)
	case int16:
		return CastNumberToNumber[int16, R](v2)
	case int32:
		return CastNumberToNumber[int32, R](v2)
	case int64:
		return CastNumberToNumber[int64, R](v2)
	case int:
		return CastNumberToNumber[int, R](v2)
	case uint8:
		return CastNumberToNumber[uint8, R](v2)
	case uint16:
		return CastNumberToNumber[uint16, R](v2)
	case uint32:
		return CastNumberToNumber[uint32, R](v2)
	case uint64:
		return CastNumberToNumber[uint64, R](v2)
	case uint:
		return CastNumberToNumber[uint, R](v2)
	case uintptr:
		return CastNumberToNumber[uintptr, R](v2)
	case float32:
		return CastNumberToNumber[float32, R](v2)
	case float64:
		return CastNumberToNumber[float64, R](v2)
	case complex64:
		return CastComplexToNumber[complex64, R](v2)
	case complex128:
		return CastComplexToNumber[complex128, R](v2)
	case bool:
		return CastBoolToNumber[bool, R](v2)
	case string:
		return CastStringToFloat[string, R](v2)
	case time.Time:
		return CastTimeToNumber[R](v2)
	case TimeRange:
		return CastTimeRangeToNumber[R](v2)

	case Nullable[int8]:
		return UnwrapNullable(CastNumberToNumber[int8, R])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastNumberToNumber[int16, R])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastNumberToNumber[int32, R])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastNumberToNumber[int64, R])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastNumberToNumber[int, R])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastNumberToNumber[uint8, R])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastNumberToNumber[uint16, R])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastNumberToNumber[uint32, R])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastNumberToNumber[uint64, R])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastNumberToNumber[uint, R])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastNumberToNumber[uintptr, R])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastNumberToNumber[float32, R])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastNumberToNumber[float64, R])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToNumber[complex64, R])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToNumber[complex128, R])(v2)
	case Nullable[bool]:
		return UnwrapNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToFloat[string, R])(v2)
	case Nullable[time.Time]:
		return UnwrapNullable(CastTimeToNumber[R])(v2)
	case Nullable[TimeRange]:
		return UnwrapNullable(CastTimeRangeToNumber[R])(v2)

	default:
		return ret, false
	}
}

func ToComplex[R Complex](v interface{}) (R, bool) {
	ret, ok := v.(R)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastNumberToComplex[int8, R](v2)
	case int16:
		return CastNumberToComplex[int16, R](v2)
	case int32:
		return CastNumberToComplex[int32, R](v2)
	case int64:
		return CastNumberToComplex[int64, R](v2)
	case int:
		return CastNumberToComplex[int, R](v2)
	case uint8:
		return CastNumberToComplex[uint8, R](v2)
	case uint16:
		return CastNumberToComplex[uint16, R](v2)
	case uint32:
		return CastNumberToComplex[uint32, R](v2)
	case uint64:
		return CastNumberToComplex[uint64, R](v2)
	case uint:
		return CastNumberToComplex[uint, R](v2)
	case uintptr:
		return CastNumberToComplex[uintptr, R](v2)
	case float32:
		return CastNumberToComplex[float32, R](v2)
	case float64:
		return CastNumberToComplex[float64, R](v2)
	case complex64:
		return CastComplexToComplex[complex64, R](v2)
	case complex128:
		return CastComplexToComplex[complex128, R](v2)
	case bool:
		return CastBoolToNumber[bool, R](v2)
	case string:
		return CastStringToComplex[string, R](v2)
	case time.Time:
		return CastTimeToComplex[R](v2)
	case TimeRange:
		return CastTimeRangeToComplex[R](v2)

	case Nullable[int8]:
		return UnwrapNullable(CastNumberToComplex[int8, R])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastNumberToComplex[int16, R])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastNumberToComplex[int32, R])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastNumberToComplex[int64, R])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastNumberToComplex[int, R])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastNumberToComplex[uint8, R])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastNumberToComplex[uint16, R])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastNumberToComplex[uint32, R])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastNumberToComplex[uint64, R])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastNumberToComplex[uint, R])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastNumberToComplex[uintptr, R])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastNumberToComplex[float32, R])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastNumberToComplex[float64, R])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToComplex[complex64, R])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToComplex[complex128, R])(v2)
	case Nullable[bool]:
		return UnwrapNullable(CastBoolToNumber[bool, R])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToComplex[string, R])(v2)
	case Nullable[time.Time]:
		return UnwrapNullable(CastTimeToComplex[R])(v2)
	case Nullable[TimeRange]:
		return UnwrapNullable(CastTimeRangeToComplex[R])(v2)

	default:
		return ret, false
	}
}
