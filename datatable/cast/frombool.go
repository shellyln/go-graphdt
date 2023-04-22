package cast

import (
	"strconv"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getBoolCastFunc[T ~bool](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_I8:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, int8])
	case Type_I16:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, int16])
	case Type_I32:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, int32])
	case Type_I64:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, int64])
	case Type_Int:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, int])
	case Type_U8:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, uint8])
	case Type_U16:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, uint16])
	case Type_U32:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, uint32])
	case Type_U64:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, uint64])
	case Type_Uint:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, uint])
	case Type_UintPtr:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, uintptr])
	case Type_F32:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, float32])
	case Type_F64:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, float64])
	case Type_Complex64:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, complex64])
	case Type_Complex128:
		return GenerateCastFunc(from, to, CastBoolToNumber[T, complex128])
	case Type_Bool:
		return GenerateCastFunc(from, to, CastBoolToBool[T, bool])
	case Type_String:
		return GenerateCastFunc(from, to, CastBoolToString[T, string])
	case Type_Any:
		return GenerateCastFunc(from, to, CastBoolToAny[T])
	default:
		return Fail
	}
}

func CastBoolToNumber[T ~bool, R Int | Uint | Float | Complex](v T) (R, bool) {
	if v {
		return 1, true
	} else {
		return 0, true
	}
}

func CastBoolToBool[T ~bool, R ~bool](v T) (R, bool) {
	return R(v), true
}

func CastBoolToString[T ~bool, R ~string](v T) (R, bool) {
	return R(strconv.FormatBool(bool(v))), true
}

func CastBoolToAny[T ~bool](v T) (interface{}, bool) {
	return v, true
}
