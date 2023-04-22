package cast

import (
	"math"
	"strconv"
	"time"
	"unsafe"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getIntCastFunc[T Int](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_Bool:
		return GenerateCastFunc(from, to, CastIntegerToBool[T, bool])
	case Type_String:
		return GenerateCastFunc(from, to, CastIntToString[T, string])
	case Type_DateTime:
		return GenerateCastFunc(from, to, CastIntegerToTime[T])
	case Type_DateTimeRange:
		return GenerateCastFunc(from, to, CastIntegerToTimeRange[T])
	default:
		return internalGetNumberCastFunc[T](from, to)
	}
}

func getUintCastFunc[T Uint](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_Bool:
		return GenerateCastFunc(from, to, CastIntegerToBool[T, bool])
	case Type_String:
		return GenerateCastFunc(from, to, CastUintToString[T, string])
	case Type_DateTime:
		return GenerateCastFunc(from, to, CastIntegerToTime[T])
	case Type_DateTimeRange:
		return GenerateCastFunc(from, to, CastIntegerToTimeRange[T])
	default:
		return internalGetNumberCastFunc[T](from, to)
	}
}

func getFloatCastFunc[T Float](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_Bool:
		return GenerateCastFunc(from, to, CastFloatToBool[T, bool])
	case Type_String:
		return GenerateCastFunc(from, to, CastFloatToString[T, string])
	case Type_DateTime:
		return GenerateCastFunc(from, to, CastFloatToTime[T])
	case Type_DateTimeRange:
		return GenerateCastFunc(from, to, CastFloatToTimeRange[T])
	default:
		return internalGetNumberCastFunc[T](from, to)
	}
}

func internalGetNumberCastFunc[T Int | Uint | Float](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_I8:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, int8])
	case Type_I16:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, int16])
	case Type_I32:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, int32])
	case Type_I64:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, int64])
	case Type_Int:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, int])
	case Type_U8:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, uint8])
	case Type_U16:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, uint16])
	case Type_U32:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, uint32])
	case Type_U64:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, uint64])
	case Type_Uint:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, uint])
	case Type_UintPtr:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, uintptr])
	case Type_F32:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, float32])
	case Type_F64:
		return GenerateCastFunc(from, to, CastNumberToNumber[T, float64])
	case Type_Complex64:
		return GenerateCastFunc(from, to, CastNumberToComplex[T, complex64])
	case Type_Complex128:
		return GenerateCastFunc(from, to, CastNumberToComplex[T, complex128])
	case Type_Any:
		return GenerateCastFunc(from, to, CastNumberToAny[T])
	default:
		return Fail
	}
}

func CastNumberToNumber[T Int | Uint | Float, R Int | Uint | Float](v T) (R, bool) {
	return R(v), true
}

func CastNumberToComplex[T Int | Uint | Float, R Complex](v T) (R, bool) {
	return R(complex(float64(v), 0)), true
}

func CastIntegerToBool[T Int | Uint, R ~bool](v T) (R, bool) {
	return v != 0, true
}

func CastFloatToBool[T Float, R ~bool](v T) (R, bool) {
	return R(v != 0 && !math.IsNaN(float64(v))), true
}

func CastIntToString[T Int, R ~string](v T) (R, bool) {
	return R(strconv.FormatInt(int64(v), 10)), true
}

func CastUintToString[T Uint, R ~string](v T) (R, bool) {
	return R(strconv.FormatUint(uint64(v), 10)), true
}

func CastFloatToString[T Float, R ~string](v T) (R, bool) {
	return R(strconv.FormatFloat(float64(v), 'g', -1, int(unsafe.Sizeof(v))*8)), true
}

func CastIntegerToTime[T Int | Uint](v T) (time.Time, bool) {
	return time.Unix(int64(v), 0), true
}

func CastFloatToTime[T Float](v T) (time.Time, bool) {
	z := float64(v)
	a := math.Floor(z)
	b := (z - a) * 1_000_000_000
	return time.Unix(int64(a), int64(b)), true
}

func CastIntegerToTimeRange[T Int | Uint](v T) (TimeRange, bool) {
	t := time.Unix(int64(v), 0)
	return TimeRange{
		Start: t,
		End:   t,
	}, true
}

func CastFloatToTimeRange[T Float](v T) (TimeRange, bool) {
	z := float64(v)
	a := math.Floor(z)
	b := (z - a) * 1_000_000_000
	t := time.Unix(int64(a), int64(b))
	return TimeRange{
		Start: t,
		End:   t,
	}, true
}

func CastNumberToAny[T Int | Uint | Float](v T) (interface{}, bool) {
	return v, true
}
