package cast

import (
	"reflect"
	"time"
	"unsafe"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func ToNullableBool[R ~bool](v interface{}) (Nullable[R], bool) {
	ret, ok := v.(Nullable[R])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastIntegerToBool[int8, R])(v2)
	case int16:
		return WrapNullable(CastIntegerToBool[int16, R])(v2)
	case int32:
		return WrapNullable(CastIntegerToBool[int32, R])(v2)
	case int64:
		return WrapNullable(CastIntegerToBool[int64, R])(v2)
	case int:
		return WrapNullable(CastIntegerToBool[int, R])(v2)
	case uint8:
		return WrapNullable(CastIntegerToBool[uint8, R])(v2)
	case uint16:
		return WrapNullable(CastIntegerToBool[uint16, R])(v2)
	case uint32:
		return WrapNullable(CastIntegerToBool[uint32, R])(v2)
	case uint64:
		return WrapNullable(CastIntegerToBool[uint64, R])(v2)
	case uint:
		return WrapNullable(CastIntegerToBool[uint, R])(v2)
	case uintptr:
		return WrapNullable(CastIntegerToBool[uintptr, R])(v2)
	case float32:
		return WrapNullable(CastFloatToBool[float32, R])(v2)
	case float64:
		return WrapNullable(CastFloatToBool[float64, R])(v2)
	case complex64:
		return WrapNullable(CastComplexToBool[complex64, R])(v2)
	case complex128:
		return WrapNullable(CastComplexToBool[complex128, R])(v2)
	case bool:
		return WrapNullable(CastBoolToBool[bool, R])(v2)
	case string:
		return WrapNullable(CastStringToBool[string, R])(v2)

	case Nullable[int8]:
		return AsNullable(CastIntegerToBool[int8, R])(v2)
	case Nullable[int16]:
		return AsNullable(CastIntegerToBool[int16, R])(v2)
	case Nullable[int32]:
		return AsNullable(CastIntegerToBool[int32, R])(v2)
	case Nullable[int64]:
		return AsNullable(CastIntegerToBool[int64, R])(v2)
	case Nullable[int]:
		return AsNullable(CastIntegerToBool[int, R])(v2)
	case Nullable[uint8]:
		return AsNullable(CastIntegerToBool[uint8, R])(v2)
	case Nullable[uint16]:
		return AsNullable(CastIntegerToBool[uint16, R])(v2)
	case Nullable[uint32]:
		return AsNullable(CastIntegerToBool[uint32, R])(v2)
	case Nullable[uint64]:
		return AsNullable(CastIntegerToBool[uint64, R])(v2)
	case Nullable[uint]:
		return AsNullable(CastIntegerToBool[uint, R])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastIntegerToBool[uintptr, R])(v2)
	case Nullable[float32]:
		return AsNullable(CastFloatToBool[float32, R])(v2)
	case Nullable[float64]:
		return AsNullable(CastFloatToBool[float64, R])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToBool[complex64, R])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToBool[complex128, R])(v2)
	case Nullable[bool]:
		return AsNullable(CastBoolToBool[bool, R])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToBool[string, R])(v2)

	default:
		return ret, false
	}
}

func ToNullableString[R ~string](v interface{}) (Nullable[R], bool) {
	ret, ok := v.(Nullable[R])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastIntToString[int8, R])(v2)
	case int16:
		return WrapNullable(CastIntToString[int16, R])(v2)
	case int32:
		return WrapNullable(CastIntToString[int32, R])(v2)
	case int64:
		return WrapNullable(CastIntToString[int64, R])(v2)
	case int:
		return WrapNullable(CastIntToString[int, R])(v2)
	case uint8:
		return WrapNullable(CastUintToString[uint8, R])(v2)
	case uint16:
		return WrapNullable(CastUintToString[uint16, R])(v2)
	case uint32:
		return WrapNullable(CastUintToString[uint32, R])(v2)
	case uint64:
		return WrapNullable(CastUintToString[uint64, R])(v2)
	case uint:
		return WrapNullable(CastUintToString[uint, R])(v2)
	case uintptr:
		return WrapNullable(CastUintToString[uintptr, R])(v2)
	case float32:
		return WrapNullable(CastFloatToString[float32, R])(v2)
	case float64:
		return WrapNullable(CastFloatToString[float64, R])(v2)
	case complex64:
		return WrapNullable(CastComplexToString[complex64, R])(v2)
	case complex128:
		return WrapNullable(CastComplexToString[complex128, R])(v2)
	case bool:
		return WrapNullable(CastBoolToString[bool, R])(v2)
	case string:
		return WrapNullable(CastStringToString[string, R])(v2)
	case []byte:
		return WrapNullable(CastBlobToString[[]byte, R])(v2)
	case time.Time:
		return WrapNullable(CastTimeToString[R])(v2)
	case TimeRange:
		return WrapNullable(CastTimeRangeToString[R])(v2)

	case Nullable[int8]:
		return AsNullable(CastIntToString[int8, R])(v2)
	case Nullable[int16]:
		return AsNullable(CastIntToString[int16, R])(v2)
	case Nullable[int32]:
		return AsNullable(CastIntToString[int32, R])(v2)
	case Nullable[int64]:
		return AsNullable(CastIntToString[int64, R])(v2)
	case Nullable[int]:
		return AsNullable(CastIntToString[int, R])(v2)
	case Nullable[uint8]:
		return AsNullable(CastUintToString[uint8, R])(v2)
	case Nullable[uint16]:
		return AsNullable(CastUintToString[uint16, R])(v2)
	case Nullable[uint32]:
		return AsNullable(CastUintToString[uint32, R])(v2)
	case Nullable[uint64]:
		return AsNullable(CastUintToString[uint64, R])(v2)
	case Nullable[uint]:
		return AsNullable(CastUintToString[uint, R])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastUintToString[uintptr, R])(v2)
	case Nullable[float32]:
		return AsNullable(CastFloatToString[float32, R])(v2)
	case Nullable[float64]:
		return AsNullable(CastFloatToString[float64, R])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToString[complex64, R])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToString[complex128, R])(v2)
	case Nullable[bool]:
		return AsNullable(CastBoolToString[bool, R])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToString[string, R])(v2)
	case Nullable[[]byte]:
		return AsNullable(CastBlobToString[[]byte, R])(v2)
	case Nullable[time.Time]:
		return AsNullable(CastTimeToString[R])(v2)
	case Nullable[TimeRange]:
		return AsNullable(CastTimeRangeToString[R])(v2)

	default:
		return ret, false
	}
}

func ToNullableBlob[R ~[]byte](v interface{}) (Nullable[R], bool) {
	ret, ok := v.(Nullable[R])
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
		ret.Valid = true
		ret.Value = R(rv.String())

	case reflect.Slice:
		elRt := rt.Elem()
		length := rv.Len() * int(elRt.Size())
		ret.Valid = true
		ret.Value = make(R, length)
		copy(ret.Value, *(*[]byte)(unsafe.Pointer(&rawSliceHeader{
			Data: rv.Pointer(),
			Len:  int(length),
			Cap:  int(length),
		})))

	default:
		length := rt.Size()
		ret.Valid = true
		ret.Value = make(R, length)
		copy(ret.Value, *(*[]byte)(unsafe.Pointer(&rawSliceHeader{
			Data: (*rawInterface)(unsafe.Pointer(&v)).Ptr,
			Len:  int(length),
			Cap:  int(length),
		})))
	}

	return ret, true
}

func ToNullableTime(v interface{}) (Nullable[time.Time], bool) {
	ret, ok := v.(Nullable[time.Time])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastIntegerToTime[int8])(v2)
	case int16:
		return WrapNullable(CastIntegerToTime[int16])(v2)
	case int32:
		return WrapNullable(CastIntegerToTime[int32])(v2)
	case int64:
		return WrapNullable(CastIntegerToTime[int64])(v2)
	case int:
		return WrapNullable(CastIntegerToTime[int])(v2)
	case uint8:
		return WrapNullable(CastIntegerToTime[uint8])(v2)
	case uint16:
		return WrapNullable(CastIntegerToTime[uint16])(v2)
	case uint32:
		return WrapNullable(CastIntegerToTime[uint32])(v2)
	case uint64:
		return WrapNullable(CastIntegerToTime[uint64])(v2)
	case uint:
		return WrapNullable(CastIntegerToTime[uint])(v2)
	case uintptr:
		return WrapNullable(CastIntegerToTime[uintptr])(v2)
	case float32:
		return WrapNullable(CastFloatToTime[float32])(v2)
	case float64:
		return WrapNullable(CastFloatToTime[float64])(v2)
	case complex64:
		return WrapNullable(CastComplexToTime[complex64])(v2)
	case complex128:
		return WrapNullable(CastComplexToTime[complex128])(v2)
	case string:
		return WrapNullable(CastStringToTime[string])(v2)
	case time.Time:
		return WrapNullable(CastTimeToTime)(v2)
	case TimeRange:
		return WrapNullable(CastTimeRangeToTime)(v2)

	case Nullable[int8]:
		return AsNullable(CastIntegerToTime[int8])(v2)
	case Nullable[int16]:
		return AsNullable(CastIntegerToTime[int16])(v2)
	case Nullable[int32]:
		return AsNullable(CastIntegerToTime[int32])(v2)
	case Nullable[int64]:
		return AsNullable(CastIntegerToTime[int64])(v2)
	case Nullable[int]:
		return AsNullable(CastIntegerToTime[int])(v2)
	case Nullable[uint8]:
		return AsNullable(CastIntegerToTime[uint8])(v2)
	case Nullable[uint16]:
		return AsNullable(CastIntegerToTime[uint16])(v2)
	case Nullable[uint32]:
		return AsNullable(CastIntegerToTime[uint32])(v2)
	case Nullable[uint64]:
		return AsNullable(CastIntegerToTime[uint64])(v2)
	case Nullable[uint]:
		return AsNullable(CastIntegerToTime[uint])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastIntegerToTime[uintptr])(v2)
	case Nullable[float32]:
		return AsNullable(CastFloatToTime[float32])(v2)
	case Nullable[float64]:
		return AsNullable(CastFloatToTime[float64])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToTime[complex64])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToTime[complex128])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToTime[string])(v2)
	case Nullable[time.Time]:
		return AsNullable(CastTimeToTime)(v2)
	case Nullable[TimeRange]:
		return AsNullable(CastTimeRangeToTime)(v2)

	default:
		return ret, false
	}
}

func ToNullableTimeRange(v interface{}) (Nullable[TimeRange], bool) {
	ret, ok := v.(Nullable[TimeRange])
	if ok {
		return ret, true
	}
	if v == nil {
		return ret, true
	}

	switch v2 := v.(type) {
	case int8:
		return WrapNullable(CastIntegerToTimeRange[int8])(v2)
	case int16:
		return WrapNullable(CastIntegerToTimeRange[int16])(v2)
	case int32:
		return WrapNullable(CastIntegerToTimeRange[int32])(v2)
	case int64:
		return WrapNullable(CastIntegerToTimeRange[int64])(v2)
	case int:
		return WrapNullable(CastIntegerToTimeRange[int])(v2)
	case uint8:
		return WrapNullable(CastIntegerToTimeRange[uint8])(v2)
	case uint16:
		return WrapNullable(CastIntegerToTimeRange[uint16])(v2)
	case uint32:
		return WrapNullable(CastIntegerToTimeRange[uint32])(v2)
	case uint64:
		return WrapNullable(CastIntegerToTimeRange[uint64])(v2)
	case uint:
		return WrapNullable(CastIntegerToTimeRange[uint])(v2)
	case uintptr:
		return WrapNullable(CastIntegerToTimeRange[uintptr])(v2)
	case float32:
		return WrapNullable(CastFloatToTimeRange[float32])(v2)
	case float64:
		return WrapNullable(CastFloatToTimeRange[float64])(v2)
	case complex64:
		return WrapNullable(CastComplexToTimeRange[complex64])(v2)
	case complex128:
		return WrapNullable(CastComplexToTimeRange[complex128])(v2)
	case string:
		return WrapNullable(CastStringToTimeRange[string])(v2)
	case time.Time:
		return WrapNullable(CastTimeToTimeRange)(v2)
	case TimeRange:
		return WrapNullable(CastTimeRangeToTimeRange)(v2)

	case Nullable[int8]:
		return AsNullable(CastIntegerToTimeRange[int8])(v2)
	case Nullable[int16]:
		return AsNullable(CastIntegerToTimeRange[int16])(v2)
	case Nullable[int32]:
		return AsNullable(CastIntegerToTimeRange[int32])(v2)
	case Nullable[int64]:
		return AsNullable(CastIntegerToTimeRange[int64])(v2)
	case Nullable[int]:
		return AsNullable(CastIntegerToTimeRange[int])(v2)
	case Nullable[uint8]:
		return AsNullable(CastIntegerToTimeRange[uint8])(v2)
	case Nullable[uint16]:
		return AsNullable(CastIntegerToTimeRange[uint16])(v2)
	case Nullable[uint32]:
		return AsNullable(CastIntegerToTimeRange[uint32])(v2)
	case Nullable[uint64]:
		return AsNullable(CastIntegerToTimeRange[uint64])(v2)
	case Nullable[uint]:
		return AsNullable(CastIntegerToTimeRange[uint])(v2)
	case Nullable[uintptr]:
		return AsNullable(CastIntegerToTimeRange[uintptr])(v2)
	case Nullable[float32]:
		return AsNullable(CastFloatToTimeRange[float32])(v2)
	case Nullable[float64]:
		return AsNullable(CastFloatToTimeRange[float64])(v2)
	case Nullable[complex64]:
		return AsNullable(CastComplexToTimeRange[complex64])(v2)
	case Nullable[complex128]:
		return AsNullable(CastComplexToTimeRange[complex128])(v2)
	case Nullable[string]:
		return AsNullable(CastStringToTimeRange[string])(v2)
	case Nullable[time.Time]:
		return AsNullable(CastTimeToTimeRange)(v2)
	case Nullable[TimeRange]:
		return AsNullable(CastTimeRangeToTimeRange)(v2)

	default:
		return ret, false
	}
}
