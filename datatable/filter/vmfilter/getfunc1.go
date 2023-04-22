package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/filter/cmpr"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func GetComparatorEq(ty DataColumnType, is3VL bool) func(a, b interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return fromComparator(cmpr.ComparableEq[int8])
		case Type_I16:
			return fromComparator(cmpr.ComparableEq[int16])
		case Type_I32:
			return fromComparator(cmpr.ComparableEq[int32])
		case Type_I64:
			return fromComparator(cmpr.ComparableEq[int64])
		case Type_Int:
			return fromComparator(cmpr.ComparableEq[int])
		case Type_U8:
			return fromComparator(cmpr.ComparableEq[uint8])
		case Type_U16:
			return fromComparator(cmpr.ComparableEq[uint16])
		case Type_U32:
			return fromComparator(cmpr.ComparableEq[uint32])
		case Type_U64:
			return fromComparator(cmpr.ComparableEq[uint64])
		case Type_Uint:
			return fromComparator(cmpr.ComparableEq[uint])
		case Type_UintPtr:
			return fromComparator(cmpr.ComparableEq[uintptr])
		case Type_F32:
			return fromComparator(cmpr.ComparableEq[float32])
		case Type_F64:
			return fromComparator(cmpr.ComparableEq[float64])
		case Type_Complex64:
			return fromComparator(cmpr.ComparableEq[complex64])
		case Type_Complex128:
			return fromComparator(cmpr.ComparableEq[complex128])
		case Type_Bool:
			return fromComparator(cmpr.ComparableEq[bool])
		case Type_String:
			return fromComparator(cmpr.ComparableEq[string])
		case Type_Blob:
			return fromComparator(cmpr.BlobEq[[]byte])
		case Type_DateTime:
			return fromComparator(cmpr.TimeEq)
		case Type_DateTimeRange:
			return fromComparator(cmpr.TimeRangeEq)
		default:
			return BinaryFalse
		}
	} else {
		if is3VL {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComparableEq[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable3VL(cmpr.BlobEq[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable3VL(cmpr.TimeEq))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable3VL(cmpr.TimeRangeEq))
			default:
				return BinaryFalse
			}
		} else {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComparableEq[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable2VL(cmpr.BlobEq[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable2VL(cmpr.TimeEq))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable2VL(cmpr.TimeRangeEq))
			default:
				return BinaryFalse
			}
		}
	}
}

func GetComparatorNotEq(ty DataColumnType, is3VL bool) func(a, b interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return fromComparator(cmpr.ComparableNotEq[int8])
		case Type_I16:
			return fromComparator(cmpr.ComparableNotEq[int16])
		case Type_I32:
			return fromComparator(cmpr.ComparableNotEq[int32])
		case Type_I64:
			return fromComparator(cmpr.ComparableNotEq[int64])
		case Type_Int:
			return fromComparator(cmpr.ComparableNotEq[int])
		case Type_U8:
			return fromComparator(cmpr.ComparableNotEq[uint8])
		case Type_U16:
			return fromComparator(cmpr.ComparableNotEq[uint16])
		case Type_U32:
			return fromComparator(cmpr.ComparableNotEq[uint32])
		case Type_U64:
			return fromComparator(cmpr.ComparableNotEq[uint64])
		case Type_Uint:
			return fromComparator(cmpr.ComparableNotEq[uint])
		case Type_UintPtr:
			return fromComparator(cmpr.ComparableNotEq[uintptr])
		case Type_F32:
			return fromComparator(cmpr.ComparableNotEq[float32])
		case Type_F64:
			return fromComparator(cmpr.ComparableNotEq[float64])
		case Type_Complex64:
			return fromComparator(cmpr.ComparableNotEq[complex64])
		case Type_Complex128:
			return fromComparator(cmpr.ComparableNotEq[complex128])
		case Type_Bool:
			return fromComparator(cmpr.ComparableNotEq[bool])
		case Type_String:
			return fromComparator(cmpr.ComparableNotEq[string])
		case Type_Blob:
			return fromComparator(cmpr.BlobNotEq[[]byte])
		case Type_DateTime:
			return fromComparator(cmpr.TimeNotEq)
		case Type_DateTimeRange:
			return fromComparator(cmpr.TimeRangeNotEq)
		default:
			return BinaryFalse
		}
	} else {
		if is3VL {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.ComparableEq[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.BlobEq[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.TimeEq))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable3VLNot(cmpr.TimeRangeEq))
			default:
				return BinaryFalse
			}
		} else {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.ComparableEq[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.BlobEq[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.TimeEq))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable2VLNot(cmpr.TimeRangeEq))
			default:
				return BinaryFalse
			}
		}
	}
}
