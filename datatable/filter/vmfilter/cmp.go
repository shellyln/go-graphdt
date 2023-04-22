package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Equals2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, false, cast.GetComparingPromotedType, nil, GetComparatorEq)
}

func IsNull2VL(rt *runtime.Runtime) FilterGenFunc {
	return unaryInstruction(rt, false, GetComparatorIsNull)
}

func NotEquals2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, false, cast.GetComparingPromotedType, nil, GetComparatorNotEq)
}

func IsNotNull2VL(rt *runtime.Runtime) FilterGenFunc {
	return unaryInstruction(rt, false, GetComparatorIsNotNull)
}

func Equals3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, false, cast.GetComparingPromotedType, nil, GetComparatorEq)
}

func IsNull3VL(rt *runtime.Runtime) FilterGenFunc {
	return unaryInstruction(rt, true, GetComparatorIsNull)
}

func NotEquals3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, false, cast.GetComparingPromotedType, nil, GetComparatorNotEq)
}

func IsNotNull3VL(rt *runtime.Runtime) FilterGenFunc {
	return unaryInstruction(rt, true, GetComparatorIsNotNull)
}
