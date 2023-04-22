package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/filter/anycmpr"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Like2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getLikePromotedType, afterCastLikeOp2, getComparatorLike)
}

func NotLike2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getLikePromotedType, afterCastLikeOp2, getComparatorNotLike)
}

func Like3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getLikePromotedType, afterCastLikeOp2, getComparatorLike)
}

func NotLike3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getLikePromotedType, afterCastLikeOp2, getComparatorNotLike)
}

func getLikePromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	if a|Type_Flag_Nullable != 0 {
		return Type_Nullable_String, Type_Nullable_String
	} else {
		return Type_String, Type_String
	}
}

func afterCastLikeOp2(ty DataColumnType, is3VL bool) func(castFn CastFunc) CastFunc {
	if ty|Type_Flag_Nullable != 0 {
		return CastLikeNullableOp2
	} else {
		return CastLikeOp2
	}
}

func getComparatorLike(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	if ty|Type_Flag_Nullable != 0 {
		if is3VL {
			return anycmpr.Nullable3VLLikeComparator()
		} else {
			return anycmpr.Nullable2VLLikeComparator()
		}
	} else {
		return anycmpr.LikeComparator()
	}
}

func getComparatorNotLike(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	if ty|Type_Flag_Nullable != 0 {
		if is3VL {
			return anycmpr.Nullable3VLNotLikeComparator()
		} else {
			return anycmpr.Nullable2VLNotLikeComparator()
		}
	} else {
		return anycmpr.NotLikeComparator()
	}
}
