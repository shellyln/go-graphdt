package cast

import (
	"encoding/base64"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getBlobCastFunc[T ~[]byte](from, to DataColumnType) CastFunc {
	switch to & Type_Mask_Element {
	case Type_String:
		return GenerateCastFunc(from, to, CastBlobToString[T, string])
	case Type_Any:
		return GenerateCastFunc(from, to, CastBlobToAny[T])
	default:
		return Fail
	}
}

func CastBlobToString[T ~[]byte, R ~string](v T) (R, bool) {
	return R(base64.StdEncoding.EncodeToString(v)), true
}

func CastBlobToAny[T ~[]byte](v T) (interface{}, bool) {
	return v, true
}
