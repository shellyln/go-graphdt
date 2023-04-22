package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/filter/cmpr"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func GetComparatorLt(ty DataColumnType, is3VL bool) func(a, b interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return fromComparator(cmpr.OrderedLt[int8])
		case Type_I16:
			return fromComparator(cmpr.OrderedLt[int16])
		case Type_I32:
			return fromComparator(cmpr.OrderedLt[int32])
		case Type_I64:
			return fromComparator(cmpr.OrderedLt[int64])
		case Type_Int:
			return fromComparator(cmpr.OrderedLt[int])
		case Type_U8:
			return fromComparator(cmpr.OrderedLt[uint8])
		case Type_U16:
			return fromComparator(cmpr.OrderedLt[uint16])
		case Type_U32:
			return fromComparator(cmpr.OrderedLt[uint32])
		case Type_U64:
			return fromComparator(cmpr.OrderedLt[uint64])
		case Type_Uint:
			return fromComparator(cmpr.OrderedLt[uint])
		case Type_UintPtr:
			return fromComparator(cmpr.OrderedLt[uintptr])
		case Type_F32:
			return fromComparator(cmpr.OrderedLt[float32])
		case Type_F64:
			return fromComparator(cmpr.OrderedLt[float64])
		case Type_Complex64:
			return fromComparator(cmpr.ComplexLt[complex64])
		case Type_Complex128:
			return fromComparator(cmpr.ComplexLt[complex128])
		case Type_Bool:
			return fromComparator(cmpr.BoolLt[bool])
		case Type_String:
			return fromComparator(cmpr.OrderedLt[string])
		case Type_Blob:
			return fromComparator(cmpr.BlobLt[[]byte])
		case Type_DateTime:
			return fromComparator(cmpr.TimeLt)
		case Type_DateTimeRange:
			return fromComparator(cmpr.TimeRangeLt)
		default:
			return BinaryFalse
		}
	} else {
		if is3VL {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComplexLt[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComplexLt[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable3VL(cmpr.BoolLt[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLt[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable3VL(cmpr.BlobLt[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable3VL(cmpr.TimeLt))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable3VL(cmpr.TimeRangeLt))
			default:
				return BinaryFalse
			}
		} else {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComplexLt[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComplexLt[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable2VL(cmpr.BoolLt[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLt[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable2VL(cmpr.BlobLt[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable2VL(cmpr.TimeLt))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable2VL(cmpr.TimeRangeLt))
			default:
				return BinaryFalse
			}
		}
	}
}

func GetComparatorLe(ty DataColumnType, is3VL bool) func(a, b interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return fromComparator(cmpr.OrderedLe[int8])
		case Type_I16:
			return fromComparator(cmpr.OrderedLe[int16])
		case Type_I32:
			return fromComparator(cmpr.OrderedLe[int32])
		case Type_I64:
			return fromComparator(cmpr.OrderedLe[int64])
		case Type_Int:
			return fromComparator(cmpr.OrderedLe[int])
		case Type_U8:
			return fromComparator(cmpr.OrderedLe[uint8])
		case Type_U16:
			return fromComparator(cmpr.OrderedLe[uint16])
		case Type_U32:
			return fromComparator(cmpr.OrderedLe[uint32])
		case Type_U64:
			return fromComparator(cmpr.OrderedLe[uint64])
		case Type_Uint:
			return fromComparator(cmpr.OrderedLe[uint])
		case Type_UintPtr:
			return fromComparator(cmpr.OrderedLe[uintptr])
		case Type_F32:
			return fromComparator(cmpr.OrderedLe[float32])
		case Type_F64:
			return fromComparator(cmpr.OrderedLe[float64])
		case Type_Complex64:
			return fromComparator(cmpr.ComplexLe[complex64])
		case Type_Complex128:
			return fromComparator(cmpr.ComplexLe[complex128])
		case Type_Bool:
			return fromComparator(cmpr.BoolLe[bool])
		case Type_String:
			return fromComparator(cmpr.OrderedLe[string])
		case Type_Blob:
			return fromComparator(cmpr.BlobLe[[]byte])
		case Type_DateTime:
			return fromComparator(cmpr.TimeLe)
		case Type_DateTimeRange:
			return fromComparator(cmpr.TimeRangeLe)
		default:
			return BinaryFalse
		}
	} else {
		if is3VL {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComplexLe[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable3VL(cmpr.ComplexLe[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable3VL(cmpr.BoolLe[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable3VL(cmpr.OrderedLe[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable3VL(cmpr.BlobLe[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable3VL(cmpr.TimeLe))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable3VL(cmpr.TimeRangeLe))
			default:
				return BinaryFalse
			}
		} else {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[int8]))
			case Type_I16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[int16]))
			case Type_I32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[int32]))
			case Type_I64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[int64]))
			case Type_Int:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[int]))
			case Type_U8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[uint8]))
			case Type_U16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[uint16]))
			case Type_U32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[uint32]))
			case Type_U64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[uint64]))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[uint]))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[uintptr]))
			case Type_F32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[float32]))
			case Type_F64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[float64]))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComplexLe[complex64]))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable2VL(cmpr.ComplexLe[complex128]))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable2VL(cmpr.BoolLe[bool]))
			case Type_String:
				return binaryCmpFuncOf(Nullable2VL(cmpr.OrderedLe[string]))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable2VL(cmpr.BlobLe[[]byte]))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable2VL(cmpr.TimeLe))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable2VL(cmpr.TimeRangeLe))
			default:
				return BinaryFalse
			}
		}
	}
}
