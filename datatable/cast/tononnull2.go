package cast

import (
	"reflect"
	"time"
	"unsafe"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func ToBool[R ~bool](v interface{}) (R, bool) {
	ret, ok := v.(R)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastIntegerToBool[int8, R](v2)
	case int16:
		return CastIntegerToBool[int16, R](v2)
	case int32:
		return CastIntegerToBool[int32, R](v2)
	case int64:
		return CastIntegerToBool[int64, R](v2)
	case int:
		return CastIntegerToBool[int, R](v2)
	case uint8:
		return CastIntegerToBool[uint8, R](v2)
	case uint16:
		return CastIntegerToBool[uint16, R](v2)
	case uint32:
		return CastIntegerToBool[uint32, R](v2)
	case uint64:
		return CastIntegerToBool[uint64, R](v2)
	case uint:
		return CastIntegerToBool[uint, R](v2)
	case uintptr:
		return CastIntegerToBool[uintptr, R](v2)
	case float32:
		return CastFloatToBool[float32, R](v2)
	case float64:
		return CastFloatToBool[float64, R](v2)
	case complex64:
		return CastComplexToBool[complex64, R](v2)
	case complex128:
		return CastComplexToBool[complex128, R](v2)
	case bool:
		return CastBoolToBool[bool, R](v2)
	case string:
		return CastStringToBool[string, R](v2)

	case Nullable[int8]:
		return UnwrapNullable(CastIntegerToBool[int8, R])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastIntegerToBool[int16, R])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastIntegerToBool[int32, R])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastIntegerToBool[int64, R])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastIntegerToBool[int, R])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastIntegerToBool[uint8, R])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastIntegerToBool[uint16, R])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastIntegerToBool[uint32, R])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastIntegerToBool[uint64, R])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastIntegerToBool[uint, R])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastIntegerToBool[uintptr, R])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastFloatToBool[float32, R])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastFloatToBool[float64, R])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToBool[complex64, R])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToBool[complex128, R])(v2)
	case Nullable[bool]:
		return UnwrapNullable(CastBoolToBool[bool, R])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToBool[string, R])(v2)

	default:
		return ret, false
	}
}

func ToString[R ~string](v interface{}) (R, bool) {
	ret, ok := v.(R)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastIntToString[int8, R](v2)
	case int16:
		return CastIntToString[int16, R](v2)
	case int32:
		return CastIntToString[int32, R](v2)
	case int64:
		return CastIntToString[int64, R](v2)
	case int:
		return CastIntToString[int, R](v2)
	case uint8:
		return CastUintToString[uint8, R](v2)
	case uint16:
		return CastUintToString[uint16, R](v2)
	case uint32:
		return CastUintToString[uint32, R](v2)
	case uint64:
		return CastUintToString[uint64, R](v2)
	case uint:
		return CastUintToString[uint, R](v2)
	case uintptr:
		return CastUintToString[uintptr, R](v2)
	case float32:
		return CastFloatToString[float32, R](v2)
	case float64:
		return CastFloatToString[float64, R](v2)
	case complex64:
		return CastComplexToString[complex64, R](v2)
	case complex128:
		return CastComplexToString[complex128, R](v2)
	case bool:
		return CastBoolToString[bool, R](v2)
	case string:
		return CastStringToString[string, R](v2)
	case []byte:
		return CastBlobToString[[]byte, R](v2)
	case time.Time:
		return CastTimeToString[R](v2)
	case TimeRange:
		return CastTimeRangeToString[R](v2)

	case Nullable[int8]:
		return UnwrapNullable(CastIntToString[int8, R])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastIntToString[int16, R])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastIntToString[int32, R])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastIntToString[int64, R])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastIntToString[int, R])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastUintToString[uint8, R])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastUintToString[uint16, R])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastUintToString[uint32, R])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastUintToString[uint64, R])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastUintToString[uint, R])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastUintToString[uintptr, R])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastFloatToString[float32, R])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastFloatToString[float64, R])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToString[complex64, R])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToString[complex128, R])(v2)
	case Nullable[bool]:
		return UnwrapNullable(CastBoolToString[bool, R])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToString[string, R])(v2)
	case Nullable[[]byte]:
		return UnwrapNullable(CastBlobToString[[]byte, R])(v2)
	case Nullable[time.Time]:
		return UnwrapNullable(CastTimeToString[R])(v2)
	case Nullable[TimeRange]:
		return UnwrapNullable(CastTimeRangeToString[R])(v2)

	default:
		return ret, false
	}
}

func ToBlob[R ~[]byte](v interface{}) (R, bool) {
	ret, ok := v.(R)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	rv := reflect.ValueOf(v)
	rt := rv.Type()

	switch rt.Kind() {
	case reflect.String:
		ret = R(rv.String())

	case reflect.Slice:
		elRt := rt.Elem()
		length := rv.Len() * int(elRt.Size())
		ret = make(R, length)
		copy(ret, *(*[]byte)(unsafe.Pointer(&rawSliceHeader{
			Data: rv.Pointer(),
			Len:  int(length),
			Cap:  int(length),
		})))

	default:
		length := rt.Size()
		ret = make(R, length)
		copy(ret, *(*[]byte)(unsafe.Pointer(&rawSliceHeader{
			Data: (*rawInterface)(unsafe.Pointer(&v)).Ptr,
			Len:  int(length),
			Cap:  int(length),
		})))
	}

	return ret, true
}

func ToTime(v interface{}) (time.Time, bool) {
	ret, ok := v.(time.Time)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastIntegerToTime(v2)
	case int16:
		return CastIntegerToTime(v2)
	case int32:
		return CastIntegerToTime(v2)
	case int64:
		return CastIntegerToTime(v2)
	case int:
		return CastIntegerToTime(v2)
	case uint8:
		return CastIntegerToTime(v2)
	case uint16:
		return CastIntegerToTime(v2)
	case uint32:
		return CastIntegerToTime(v2)
	case uint64:
		return CastIntegerToTime(v2)
	case uint:
		return CastIntegerToTime(v2)
	case uintptr:
		return CastIntegerToTime(v2)
	case float32:
		return CastFloatToTime(v2)
	case float64:
		return CastFloatToTime(v2)
	case complex64:
		return CastComplexToTime(v2)
	case complex128:
		return CastComplexToTime(v2)
	case string:
		return CastStringToTime(v2)
	case time.Time:
		return CastTimeToTime(v2)
	case TimeRange:
		return CastTimeRangeToTime(v2)

	case Nullable[int8]:
		return UnwrapNullable(CastIntegerToTime[int8])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastIntegerToTime[int16])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastIntegerToTime[int32])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastIntegerToTime[int64])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastIntegerToTime[int])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastIntegerToTime[uint8])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastIntegerToTime[uint16])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastIntegerToTime[uint32])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastIntegerToTime[uint64])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastIntegerToTime[uint])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastIntegerToTime[uintptr])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastFloatToTime[float32])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastFloatToTime[float64])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToTime[complex64])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToTime[complex128])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToTime[string])(v2)
	case Nullable[time.Time]:
		return UnwrapNullable(CastTimeToTime)(v2)
	case Nullable[TimeRange]:
		return UnwrapNullable(CastTimeRangeToTime)(v2)

	default:
		return ret, false
	}
}

func ToTimeRange(v interface{}) (TimeRange, bool) {
	ret, ok := v.(TimeRange)
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return CastIntegerToTimeRange(v2)
	case int16:
		return CastIntegerToTimeRange(v2)
	case int32:
		return CastIntegerToTimeRange(v2)
	case int64:
		return CastIntegerToTimeRange(v2)
	case int:
		return CastIntegerToTimeRange(v2)
	case uint8:
		return CastIntegerToTimeRange(v2)
	case uint16:
		return CastIntegerToTimeRange(v2)
	case uint32:
		return CastIntegerToTimeRange(v2)
	case uint64:
		return CastIntegerToTimeRange(v2)
	case uint:
		return CastIntegerToTimeRange(v2)
	case uintptr:
		return CastIntegerToTimeRange(v2)
	case float32:
		return CastFloatToTimeRange(v2)
	case float64:
		return CastFloatToTimeRange(v2)
	case complex64:
		return CastComplexToTimeRange(v2)
	case complex128:
		return CastComplexToTimeRange(v2)
	case string:
		return CastStringToTimeRange(v2)
	case time.Time:
		return CastTimeToTimeRange(v2)
	case TimeRange:
		return CastTimeRangeToTimeRange(v2)

	case Nullable[int8]:
		return UnwrapNullable(CastIntegerToTimeRange[int8])(v2)
	case Nullable[int16]:
		return UnwrapNullable(CastIntegerToTimeRange[int16])(v2)
	case Nullable[int32]:
		return UnwrapNullable(CastIntegerToTimeRange[int32])(v2)
	case Nullable[int64]:
		return UnwrapNullable(CastIntegerToTimeRange[int64])(v2)
	case Nullable[int]:
		return UnwrapNullable(CastIntegerToTimeRange[int])(v2)
	case Nullable[uint8]:
		return UnwrapNullable(CastIntegerToTimeRange[uint8])(v2)
	case Nullable[uint16]:
		return UnwrapNullable(CastIntegerToTimeRange[uint16])(v2)
	case Nullable[uint32]:
		return UnwrapNullable(CastIntegerToTimeRange[uint32])(v2)
	case Nullable[uint64]:
		return UnwrapNullable(CastIntegerToTimeRange[uint64])(v2)
	case Nullable[uint]:
		return UnwrapNullable(CastIntegerToTimeRange[uint])(v2)
	case Nullable[uintptr]:
		return UnwrapNullable(CastIntegerToTimeRange[uintptr])(v2)
	case Nullable[float32]:
		return UnwrapNullable(CastFloatToTimeRange[float32])(v2)
	case Nullable[float64]:
		return UnwrapNullable(CastFloatToTimeRange[float64])(v2)
	case Nullable[complex64]:
		return UnwrapNullable(CastComplexToTimeRange[complex64])(v2)
	case Nullable[complex128]:
		return UnwrapNullable(CastComplexToTimeRange[complex128])(v2)
	case Nullable[string]:
		return UnwrapNullable(CastStringToTimeRange[string])(v2)
	case Nullable[time.Time]:
		return UnwrapNullable(CastTimeToTimeRange)(v2)
	case Nullable[TimeRange]:
		return UnwrapNullable(CastTimeRangeToTimeRange)(v2)

	default:
		return ret, false
	}
}
