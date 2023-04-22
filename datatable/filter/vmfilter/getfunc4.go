package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/filter/cmpr"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func GetComparatorGt(ty DataColumnType, is3VL bool) func(a, b interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[int8]))
		case Type_I16:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[int16]))
		case Type_I32:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[int32]))
		case Type_I64:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[int64]))
		case Type_Int:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[int]))
		case Type_U8:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[uint8]))
		case Type_U16:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[uint16]))
		case Type_U32:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[uint32]))
		case Type_U64:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[uint64]))
		case Type_Uint:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[uint]))
		case Type_UintPtr:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[uintptr]))
		case Type_F32:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[float32]))
		case Type_F64:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[float64]))
		case Type_Complex64:
			return fromComparator(cmpr.Swap(cmpr.ComplexLt[complex64]))
		case Type_Complex128:
			return fromComparator(cmpr.Swap(cmpr.ComplexLt[complex128]))
		case Type_Bool:
			return fromComparator(cmpr.Swap(cmpr.BoolLt[bool]))
		case Type_String:
			return fromComparator(cmpr.Swap(cmpr.OrderedLt[string]))
		case Type_Blob:
			return fromComparator(cmpr.Swap(cmpr.BlobLt[[]byte]))
		case Type_DateTime:
			return fromComparator(cmpr.Swap(cmpr.TimeLt))
		case Type_DateTimeRange:
			return fromComparator(cmpr.Swap(cmpr.TimeRangeLt))
		default:
			return BinaryFalse
		}
	} else {
		if is3VL {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[int8])))
			case Type_I16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[int16])))
			case Type_I32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[int32])))
			case Type_I64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[int64])))
			case Type_Int:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[int])))
			case Type_U8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[uint8])))
			case Type_U16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[uint16])))
			case Type_U32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[uint32])))
			case Type_U64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[uint64])))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[uint])))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[uintptr])))
			case Type_F32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[float32])))
			case Type_F64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[float64])))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.ComplexLt[complex64])))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.ComplexLt[complex128])))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.BoolLt[bool])))
			case Type_String:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLt[string])))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.BlobLt[[]byte])))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.TimeLt)))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.TimeRangeLt)))
			default:
				return BinaryFalse
			}
		} else {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[int8])))
			case Type_I16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[int16])))
			case Type_I32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[int32])))
			case Type_I64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[int64])))
			case Type_Int:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[int])))
			case Type_U8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[uint8])))
			case Type_U16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[uint16])))
			case Type_U32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[uint32])))
			case Type_U64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[uint64])))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[uint])))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[uintptr])))
			case Type_F32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[float32])))
			case Type_F64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[float64])))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.ComplexLt[complex64])))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.ComplexLt[complex128])))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.BoolLt[bool])))
			case Type_String:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLt[string])))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.BlobLt[[]byte])))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.TimeLt)))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.TimeRangeLt)))
			default:
				return BinaryFalse
			}
		}
	}
}

func GetComparatorGe(ty DataColumnType, is3VL bool) func(a, b interface{}) Bool3VL {
	if ty&Type_Flag_Nullable == 0 {
		switch ty & Type_Mask_Element {
		case Type_I8:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[int8]))
		case Type_I16:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[int16]))
		case Type_I32:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[int32]))
		case Type_I64:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[int64]))
		case Type_Int:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[int]))
		case Type_U8:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[uint8]))
		case Type_U16:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[uint16]))
		case Type_U32:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[uint32]))
		case Type_U64:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[uint64]))
		case Type_Uint:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[uint]))
		case Type_UintPtr:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[uintptr]))
		case Type_F32:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[float32]))
		case Type_F64:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[float64]))
		case Type_Complex64:
			return fromComparator(cmpr.Swap(cmpr.ComplexLe[complex64]))
		case Type_Complex128:
			return fromComparator(cmpr.Swap(cmpr.ComplexLe[complex128]))
		case Type_Bool:
			return fromComparator(cmpr.Swap(cmpr.BoolLe[bool]))
		case Type_String:
			return fromComparator(cmpr.Swap(cmpr.OrderedLe[string]))
		case Type_Blob:
			return fromComparator(cmpr.Swap(cmpr.BlobLe[[]byte]))
		case Type_DateTime:
			return fromComparator(cmpr.Swap(cmpr.TimeLe))
		case Type_DateTimeRange:
			return fromComparator(cmpr.Swap(cmpr.TimeRangeLe))
		default:
			return BinaryFalse
		}
	} else {
		if is3VL {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[int8])))
			case Type_I16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[int16])))
			case Type_I32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[int32])))
			case Type_I64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[int64])))
			case Type_Int:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[int])))
			case Type_U8:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[uint8])))
			case Type_U16:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[uint16])))
			case Type_U32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[uint32])))
			case Type_U64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[uint64])))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[uint])))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[uintptr])))
			case Type_F32:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[float32])))
			case Type_F64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[float64])))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.ComplexLe[complex64])))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.ComplexLe[complex128])))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.BoolLe[bool])))
			case Type_String:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.OrderedLe[string])))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.BlobLe[[]byte])))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.TimeLe)))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable3VL(cmpr.Swap(cmpr.TimeRangeLe)))
			default:
				return BinaryFalse
			}
		} else {
			switch ty & Type_Mask_Element {
			case Type_I8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[int8])))
			case Type_I16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[int16])))
			case Type_I32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[int32])))
			case Type_I64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[int64])))
			case Type_Int:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[int])))
			case Type_U8:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[uint8])))
			case Type_U16:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[uint16])))
			case Type_U32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[uint32])))
			case Type_U64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[uint64])))
			case Type_Uint:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[uint])))
			case Type_UintPtr:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[uintptr])))
			case Type_F32:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[float32])))
			case Type_F64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[float64])))
			case Type_Complex64:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.ComplexLe[complex64])))
			case Type_Complex128:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.ComplexLe[complex128])))
			case Type_Bool:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.BoolLe[bool])))
			case Type_String:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.OrderedLe[string])))
			case Type_Blob:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.BlobLe[[]byte])))
			case Type_DateTime:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.TimeLe)))
			case Type_DateTimeRange:
				return binaryCmpFuncOf(Nullable2VL(cmpr.Swap(cmpr.TimeRangeLe)))
			default:
				return BinaryFalse
			}
		}
	}
}
