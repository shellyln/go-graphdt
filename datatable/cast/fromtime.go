package cast

import (
	"encoding/json"
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getTimeCastFunc(from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_I8:
		return GenerateCastFunc(from, to, CastTimeToNumber[int8])
	case Type_I16:
		return GenerateCastFunc(from, to, CastTimeToNumber[int16])
	case Type_I32:
		return GenerateCastFunc(from, to, CastTimeToNumber[int32])
	case Type_I64:
		return GenerateCastFunc(from, to, CastTimeToNumber[int64])
	case Type_Int:
		return GenerateCastFunc(from, to, CastTimeToNumber[int])
	case Type_U8:
		return GenerateCastFunc(from, to, CastTimeToNumber[uint8])
	case Type_U16:
		return GenerateCastFunc(from, to, CastTimeToNumber[uint16])
	case Type_U32:
		return GenerateCastFunc(from, to, CastTimeToNumber[uint32])
	case Type_U64:
		return GenerateCastFunc(from, to, CastTimeToNumber[uint64])
	case Type_Uint:
		return GenerateCastFunc(from, to, CastTimeToNumber[uint])
	case Type_UintPtr:
		return GenerateCastFunc(from, to, CastTimeToNumber[uintptr])
	case Type_F32:
		return GenerateCastFunc(from, to, CastTimeToNumber[float32])
	case Type_F64:
		return GenerateCastFunc(from, to, CastTimeToNumber[float64])
	case Type_Complex64:
		return GenerateCastFunc(from, to, CastTimeToComplex[complex64])
	case Type_Complex128:
		return GenerateCastFunc(from, to, CastTimeToComplex[complex128])
	case Type_String:
		return GenerateCastFunc(from, to, CastTimeToString[string])
	case Type_DateTime:
		return GenerateCastFunc(from, to, CastTimeToTime)
	case Type_DateTimeRange:
		return GenerateCastFunc(from, to, CastTimeToTimeRange)
	case Type_Any:
		return GenerateCastFunc(from, to, CastTimeToAny)
	default:
		return Fail
	}
}

func CastTimeToNumber[R Int | Uint | Float](v time.Time) (R, bool) {
	return R(v.Unix()), true
}

func CastTimeToComplex[R Complex](v time.Time) (R, bool) {
	return R(complex(float64(v.Unix()), 0)), true
}

func CastTimeToString[R ~string](v time.Time) (R, bool) {
	z, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return R(z[1 : len(z)-1]), true
}

func CastTimeToTime(v time.Time) (time.Time, bool) {
	return v, true
}

func CastTimeToTimeRange(v time.Time) (TimeRange, bool) {
	return TimeRange{
		Start: v,
		End:   v,
	}, true
}

func CastTimeToAny(v time.Time) (interface{}, bool) {
	return v, true
}
