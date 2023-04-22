package vmfilter

import (
	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func LessThan2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, false, cast.GetComparingPromotedType, nil, GetComparatorLt)
}

func LessThanOrEquals2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, false, cast.GetComparingPromotedType, nil, GetComparatorLe)
}

func GreaterThan2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, false, cast.GetComparingPromotedType, nil, GetComparatorGt)
}

func GreaterThanOrEquals2VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, false, false, cast.GetComparingPromotedType, nil, GetComparatorGe)
}

func LessThan3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, false, cast.GetComparingPromotedType, nil, GetComparatorLt)
}

func LessThanOrEquals3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, false, cast.GetComparingPromotedType, nil, GetComparatorLe)
}

func GreaterThan3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, false, cast.GetComparingPromotedType, nil, GetComparatorGt)
}

func GreaterThanOrEquals3VL(rt *runtime.Runtime) FilterGenFunc {
	return binaryInstruction(rt, true, false, cast.GetComparingPromotedType, nil, GetComparatorGe)
}
