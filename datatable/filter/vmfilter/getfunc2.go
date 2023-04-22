package vmfilter

import (
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func GetComparatorIsNull(ty DataColumnType, is3VL bool) func(a interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		return UnaryFalse
	} else {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return unaryCmpFuncOf(Nullable3VLIsNull[int8])
		case Type_I16:
			return unaryCmpFuncOf(Nullable3VLIsNull[int16])
		case Type_I32:
			return unaryCmpFuncOf(Nullable3VLIsNull[int32])
		case Type_I64:
			return unaryCmpFuncOf(Nullable3VLIsNull[int64])
		case Type_Int:
			return unaryCmpFuncOf(Nullable3VLIsNull[int])
		case Type_U8:
			return unaryCmpFuncOf(Nullable3VLIsNull[uint8])
		case Type_U16:
			return unaryCmpFuncOf(Nullable3VLIsNull[uint16])
		case Type_U32:
			return unaryCmpFuncOf(Nullable3VLIsNull[uint32])
		case Type_U64:
			return unaryCmpFuncOf(Nullable3VLIsNull[uint64])
		case Type_Uint:
			return unaryCmpFuncOf(Nullable3VLIsNull[uint])
		case Type_UintPtr:
			return unaryCmpFuncOf(Nullable3VLIsNull[uintptr])
		case Type_F32:
			return unaryCmpFuncOf(Nullable3VLIsNull[float32])
		case Type_F64:
			return unaryCmpFuncOf(Nullable3VLIsNull[float64])
		case Type_Complex64:
			return unaryCmpFuncOf(Nullable3VLIsNull[complex64])
		case Type_Complex128:
			return unaryCmpFuncOf(Nullable3VLIsNull[complex128])
		case Type_Bool:
			return unaryCmpFuncOf(Nullable3VLIsNull[bool])
		case Type_String:
			return unaryCmpFuncOf(Nullable3VLIsNull[string])
		case Type_Blob:
			return unaryCmpFuncOf(Nullable3VLIsNull[[]byte])
		case Type_DateTime:
			return unaryCmpFuncOf(Nullable3VLIsNull[time.Time])
		case Type_DateTimeRange:
			return unaryCmpFuncOf(Nullable3VLIsNull[TimeRange])
		default:
			return UnaryFalse
		}
	}
}

func GetComparatorIsNotNull(ty DataColumnType, is3VL bool) func(a interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		return UnaryTrue
	} else {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[int8])
		case Type_I16:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[int16])
		case Type_I32:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[int32])
		case Type_I64:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[int64])
		case Type_Int:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[int])
		case Type_U8:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[uint8])
		case Type_U16:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[uint16])
		case Type_U32:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[uint32])
		case Type_U64:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[uint64])
		case Type_Uint:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[uint])
		case Type_UintPtr:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[uintptr])
		case Type_F32:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[float32])
		case Type_F64:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[float64])
		case Type_Complex64:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[complex64])
		case Type_Complex128:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[complex128])
		case Type_Bool:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[bool])
		case Type_String:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[string])
		case Type_Blob:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[[]byte])
		case Type_DateTime:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[time.Time])
		case Type_DateTimeRange:
			return unaryCmpFuncOf(Nullable3VLIsNotNull[TimeRange])
		default:
			return UnaryFalse
		}
	}
}
