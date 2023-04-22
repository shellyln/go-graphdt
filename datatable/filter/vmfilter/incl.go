package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/filter/anycmpr"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Includes2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getIncludesPromotedType, afterCastIncludesOp2, getComparatorIncludes)
}

func Excludes2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getIncludesPromotedType, afterCastIncludesOp2, getComparatorExcludes)
}

func Includes3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getIncludesPromotedType, afterCastIncludesOp2, getComparatorIncludes)
}

func Excludes3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getIncludesPromotedType, afterCastIncludesOp2, getComparatorExcludes)
}

func getIncludesPromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	if a|Type_Flag_Nullable != 0 {
		return Type_Nullable_String, Type_Nullable_String | Type_Flag_Array
	} else {
		return Type_String, Type_String | Type_Flag_Array
	}
}

func afterCastIncludesOp2(ty DataColumnType, is3VL bool) func(castFn CastFunc) CastFunc {
	if ty|Type_Flag_Nullable != 0 {
		return CastIncludesNullableOp2
	} else {
		return CastIncludesOp2
	}
}

func getComparatorIncludes(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	if ty|Type_Flag_Nullable != 0 {
		if is3VL {
			return anycmpr.Nullable3VLIncludesComparator()
		} else {
			return anycmpr.Nullable2VLIncludesComparator()
		}
	} else {
		return anycmpr.IncludesComparator()
	}
}

func getComparatorExcludes(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	if ty|Type_Flag_Nullable != 0 {
		if is3VL {
			return anycmpr.Nullable3VLExcludesComparator()
		} else {
			return anycmpr.Nullable2VLExcludesComparator()
		}
	} else {
		return anycmpr.ExcludesComparator()
	}
}
