package cast

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getStringCastFunc[T ~string](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_I8:
		return GenerateCastFunc(from, to, CastStringToInt[T, int8])
	case Type_I16:
		return GenerateCastFunc(from, to, CastStringToInt[T, int16])
	case Type_I32:
		return GenerateCastFunc(from, to, CastStringToInt[T, int32])
	case Type_I64:
		return GenerateCastFunc(from, to, CastStringToInt[T, int64])
	case Type_Int:
		return GenerateCastFunc(from, to, CastStringToInt[T, int])
	case Type_U8:
		return GenerateCastFunc(from, to, CastStringToUint[T, uint8])
	case Type_U16:
		return GenerateCastFunc(from, to, CastStringToUint[T, uint16])
	case Type_U32:
		return GenerateCastFunc(from, to, CastStringToUint[T, uint32])
	case Type_U64:
		return GenerateCastFunc(from, to, CastStringToUint[T, uint64])
	case Type_Uint:
		return GenerateCastFunc(from, to, CastStringToUint[T, uint])
	case Type_UintPtr:
		return GenerateCastFunc(from, to, CastStringToUint[T, uintptr])
	case Type_F32:
		return GenerateCastFunc(from, to, CastStringToFloat[T, float32])
	case Type_F64:
		return GenerateCastFunc(from, to, CastStringToFloat[T, float64])
	case Type_Complex64:
		return GenerateCastFunc(from, to, CastStringToComplex[T, complex64])
	case Type_Complex128:
		return GenerateCastFunc(from, to, CastStringToComplex[T, complex128])
	case Type_Bool:
		return GenerateCastFunc(from, to, CastStringToBool[T, bool])
	case Type_String:
		return GenerateCastFunc(from, to, CastStringToString[T, string])
	case Type_DateTime:
		return GenerateCastFunc(from, to, CastStringToTime[T])
	case Type_DateTimeRange:
		return GenerateCastFunc(from, to, CastStringToTimeRange[T])
	case Type_Any:
		return GenerateCastFunc(from, to, CastStringToAny[T])
	default:
		return Fail
	}
}

func CastStringToInt[T ~string, R Int](v T) (R, bool) {
	z, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return 0, false
	}
	return R(z), true
}

func CastStringToUint[T ~string, R Uint](v T) (R, bool) {
	z, err := strconv.ParseUint(string(v), 10, 64)
	if err != nil {
		return 0, false
	}
	return R(z), true
}

func CastStringToFloat[T ~string, R Float](v T) (R, bool) {
	z, err := strconv.ParseFloat(string(v), 64)
	if err != nil {
		return 0, false
	}
	return R(z), true
}

func CastStringToComplex[T ~string, R Complex](v T) (R, bool) {
	z, err := strconv.ParseComplex(string(v), 128)
	if err != nil {
		return 0, false
	}
	return R(z), true
}

func CastStringToBool[T ~string, R ~bool](v T) (R, bool) {
	z, err := strconv.ParseBool(string(v))
	if err != nil {
		return false, false
	}
	return R(z), true
}

func CastStringToString[T ~string, R ~string](v T) (R, bool) {
	return R(v), true
}

func CastStringToTime[T ~string](v T) (time.Time, bool) {
	var ret time.Time
	err := json.Unmarshal([]byte("\""+v+"\""), &ret)
	if err != nil {
		return ret, false
	}
	return ret, true
}

func CastStringToTimeRange[T ~string](v T) (TimeRange, bool) {
	var ret TimeRange
	ss := strings.Split(string(v), "/")
	if len(ss) != 2 {
		return ret, false
	}

	err := json.Unmarshal([]byte("\""+ss[0]+"\""), &ret.Start)
	if err != nil {
		return ret, false
	}
	err = json.Unmarshal([]byte("\""+ss[1]+"\""), &ret.End)
	if err != nil {
		return ret, false
	}
	return ret, true
}

func CastStringToAny[T ~string](v T) (interface{}, bool) {
	return v, true
}
