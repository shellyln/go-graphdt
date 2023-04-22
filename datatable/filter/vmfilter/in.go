package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/filter/anycmpr"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func In2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getInPromotedType, nil, getComparatorIn)
}

func NotIn2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, true, getInPromotedType, nil, getComparatorNotIn)
}

func In3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getInPromotedType, nil, getComparatorIn)
}

func NotIn3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, true, getInPromotedType, nil, getComparatorNotIn)
}

func getInPromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	tyR, _ := cast.GetComparingPromotedType(a&(Type_Mask_Element|Type_Flag_Nullable), b&(Type_Mask_Element|Type_Flag_Nullable))
	return tyR, tyR | Type_Flag_Array
}

func getComparatorIn(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	cmpFn := GetComparatorEq(ty, is3VL)
	if cmpFn == nil {
		return nil
	}
	return anycmpr.InComparator(cmpFn)
}

func getComparatorNotIn(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL {
	cmpFn := GetComparatorEq(ty, is3VL)
	if cmpFn == nil {
		return nil
	}
	return anycmpr.NotInComparator(cmpFn)
}
