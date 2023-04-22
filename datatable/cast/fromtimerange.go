package cast

import (
	"encoding/json"
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getTimeRangeCastFunc(from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_I8:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[int8])
	case Type_I16:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[int16])
	case Type_I32:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[int32])
	case Type_I64:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[int64])
	case Type_Int:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[int])
	case Type_U8:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[uint8])
	case Type_U16:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[uint16])
	case Type_U32:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[uint32])
	case Type_U64:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[uint64])
	case Type_Uint:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[uint])
	case Type_UintPtr:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[uintptr])
	case Type_F32:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[float32])
	case Type_F64:
		return GenerateCastFunc(from, to, CastTimeRangeToNumber[float64])
	case Type_Complex64:
		return GenerateCastFunc(from, to, CastTimeRangeToComplex[complex64])
	case Type_Complex128:
		return GenerateCastFunc(from, to, CastTimeRangeToComplex[complex128])
	case Type_String:
		return GenerateCastFunc(from, to, CastTimeRangeToString[string])
	case Type_DateTime:
		return GenerateCastFunc(from, to, CastTimeRangeToTime)
	case Type_DateTimeRange:
		return GenerateCastFunc(from, to, CastTimeRangeToTimeRange)
	case Type_Any:
		return GenerateCastFunc(from, to, CastTimeRangeToAny)
	default:
		return Fail
	}
}

func CastTimeRangeToNumber[R Int | Uint | Float](v TimeRange) (R, bool) {
	return R(v.Start.Unix()), true
}

func CastTimeRangeToComplex[R Complex](v TimeRange) (R, bool) {
	return R(complex(float64(v.Start.Unix()), 0)), true
}

func CastTimeRangeToString[R ~string](v TimeRange) (R, bool) {
	start, err := json.Marshal(v.Start)
	if err != nil {
		return "", false
	}
	end, err := json.Marshal(v.End)
	if err != nil {
		return "", false
	}
	return R(start[1:len(start)-1]) + "/" + R(end[1:len(end)-1]), true
}

func CastTimeRangeToTime(v TimeRange) (time.Time, bool) {
	return v.Start, true
}

func CastTimeRangeToTimeRange(v TimeRange) (TimeRange, bool) {
	return v, true
}

func CastTimeRangeToAny(v TimeRange) (interface{}, bool) {
	return v, true
}
