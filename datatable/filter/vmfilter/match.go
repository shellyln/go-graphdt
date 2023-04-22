package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/filter/anycmpr"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Match2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getMatchPromotedType, afterCastMatchOp2, getComparatorMatch)
}

func NotMatch2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getMatchPromotedType, afterCastMatchOp2, getComparatorNotMatch)
}

func Match3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getMatchPromotedType, afterCastMatchOp2, getComparatorMatch)
}

func NotMatch3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getMatchPromotedType, afterCastMatchOp2, getComparatorNotMatch)
}

func getMatchPromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	if a|Type_Flag_Nullable != 0 {
		return Type_Nullable_String, Type_Nullable_String
	} else {
		return Type_String, Type_String
	}
}

func afterCastMatchOp2(ty DataColumnType, is3VL bool) func(castFn CastFunc) CastFunc {
	if ty|Type_Flag_Nullable != 0 {
		return CastMatchNullableOp2
	} else {
		return CastMatchOp2
	}
}

func getComparatorMatch(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	if ty|Type_Flag_Nullable != 0 {
		if is3VL {
			return anycmpr.Nullable3VLMatchComparator()
		} else {
			return anycmpr.Nullable2VLMatchComparator()
		}
	} else {
		return anycmpr.MatchComparator()
	}
}

func getComparatorNotMatch(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	if ty|Type_Flag_Nullable != 0 {
		if is3VL {
			return anycmpr.Nullable3VLNotMatchComparator()
		} else {
			return anycmpr.Nullable2VLNotMatchComparator()
		}
	} else {
		return anycmpr.NotMatchComparator()
	}
}
