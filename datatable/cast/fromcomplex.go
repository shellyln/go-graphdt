package cast

import (
	"math"
	"math/cmplx"
	"strconv"
	"time"
	"unsafe"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getComplexCastFunc[T Complex](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_I8:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, int8])
	case Type_I16:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, int16])
	case Type_I32:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, int32])
	case Type_I64:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, int64])
	case Type_Int:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, int])
	case Type_U8:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, uint8])
	case Type_U16:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, uint16])
	case Type_U32:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, uint32])
	case Type_U64:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, uint64])
	case Type_Uint:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, uint])
	case Type_UintPtr:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, uintptr])
	case Type_F32:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, float32])
	case Type_F64:
		return GenerateCastFunc(from, to, CastComplexToNumber[T, float64])
	case Type_Complex64:
		return GenerateCastFunc(from, to, CastComplexToComplex[T, complex64])
	case Type_Complex128:
		return GenerateCastFunc(from, to, CastComplexToComplex[T, complex128])
	case Type_Bool:
		return GenerateCastFunc(from, to, CastComplexToBool[T, bool])
	case Type_String:
		return GenerateCastFunc(from, to, CastComplexToString[T, string])
	case Type_DateTime:
		return GenerateCastFunc(from, to, CastComplexToTime[T])
	case Type_DateTimeRange:
		return GenerateCastFunc(from, to, CastComplexToTimeRange[T])
	case Type_Any:
		return GenerateCastFunc(from, to, CastComplexToAny[T])
	default:
		return Fail
	}
}

func CastComplexToNumber[T Complex, R Int | Uint | Float](v T) (R, bool) {
	return R(real(complex128(v))), true
}

func CastComplexToComplex[T Complex, R Complex](v T) (R, bool) {
	return R(v), true
}

func CastComplexToBool[T Complex, R ~bool](v T) (R, bool) {
	return R(v != 0 && !cmplx.IsNaN(complex128(v))), true
}

func CastComplexToString[T Complex, R ~string](v T) (R, bool) {
	return R(strconv.FormatComplex(complex128(v), 'g', -1, int(unsafe.Sizeof(v))*8)), true
}

func CastComplexToTime[T Complex](v T) (time.Time, bool) {
	z := real(complex128(v))
	a := math.Floor(z)
	b := (z - a) * 1_000_000_000
	return time.Unix(int64(a), int64(b)), true
}

func CastComplexToTimeRange[T Complex](v T) (TimeRange, bool) {
	z := real(complex128(v))
	a := math.Floor(z)
	b := (z - a) * 1_000_000_000
	t := time.Unix(int64(a), int64(b))
	return TimeRange{
		Start: t,
		End:   t,
	}, true
}

func CastComplexToAny[T Complex](v T) (interface{}, bool) {
	return v, true
}
