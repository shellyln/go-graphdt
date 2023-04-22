package vmfilter

import (
	"fmt"

	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func unaryInstruction(rt *runtime.Runtime, is3VL bool, getCmp func(ty DataColumnType, is3VL bool) func(a interface{}) Bool3VL) FilterGenFunc {
	// filterStack: pop=1, push=1
	// execStack:   pop=1, push=0
	return func(filterStack *[]FilterStackLeaf) error {
		length := len(*filterStack)
		if length < 1 {
			return fmt.Errorf("UnaryInstruction failed: len(stack)=%v", length)
		}

		isConst1 := (*filterStack)[length-1].IsConst
		op1 := (*filterStack)[length-1].Fn
		ty1 := (*filterStack)[length-1].Type

		cmp := getCmp(ty1&(Type_Mask_Element|Type_Flag_Nullable), is3VL)

		rtStack := &rt.Stack

		var fn func(i int) Bool3VL

		if isConst1 {
			op1(0)

			rtStackLen := len(*rtStack)
			v1 := &(*rtStack)[rtStackLen-2]
			(*rtStack) = (*rtStack)[:rtStackLen-1]

			ret := cmp(v1)

			fn = func(i int) Bool3VL {
				return ret
			}
		} else {
			fn = func(i int) Bool3VL {
				op1(i)

				rtStackLen := len(*rtStack)
				v1 := &(*rtStack)[rtStackLen-2]
				(*rtStack) = (*rtStack)[:rtStackLen-1]

				return cmp(v1)
			}
		}

		(*filterStack)[length-1] = FilterStackLeaf{
			Type:    Type_Invalid,
			Fn:      fn,
			IsConst: isConst1,
		}

		rt.StackUsed = true
		return nil
	}
}

func binaryInstruction(rt *runtime.Runtime, is3VL, forcePromotion bool,
	promotionFn func(a, b DataColumnType) (DataColumnType, DataColumnType),
	getAfterCast2 func(ty DataColumnType, is3VL bool) func(castFn CastFunc) CastFunc,
	getCmp func(ty DataColumnType, is3VL bool) func(a interface{}, b interface{}) Bool3VL) FilterGenFunc {

	// filterStack: pop=2, push=1
	// execStack:   pop=2, push=0
	return func(filterStack *[]FilterStackLeaf) error {
		length := len(*filterStack)
		if length < 2 {
			return fmt.Errorf("BinaryInstruction failed: len(stack)=%v", length)
		}

		isConst1 := (*filterStack)[length-2].IsConst
		isConst2 := (*filterStack)[length-1].IsConst

		isCol1 := (*filterStack)[length-2].IsCol

		op1 := (*filterStack)[length-2].Fn
		ty1 := (*filterStack)[length-2].Type
		op2 := (*filterStack)[length-1].Fn
		ty2 := (*filterStack)[length-1].Type

		var tyR1, tyR2 DataColumnType

		if isCol1 && !forcePromotion {
			tyR1 = ty1
			tyR2 = ty1
			if !cast.CanCast(ty2, ty1) {
				return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
			}
		} else {
			tyR1, tyR2 = promotionFn(ty1, ty2)
			if tyR1 == Type_Invalid {
				return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
			}
		}

		cast1 := cast.GetCastFunc(ty1, tyR1)
		if cast1 == nil {
			return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
		}

		cast2 := cast.GetCastFunc(ty2, tyR2)
		if cast2 == nil {
			return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
		}
		if getAfterCast2 != nil {
			cast2 = getAfterCast2(tyR2, is3VL)(cast2)
		}

		cmp := getCmp(tyR1&(Type_Mask_Element|Type_Flag_Nullable), is3VL)

		rtStack := &rt.Stack

		var fn func(i int) Bool3VL

		if isConst1 && isConst2 {
			op1(0)
			op2(0)

			rtStackLen := len(*rtStack)
			v1 := &(*rtStack)[rtStackLen-2]
			v2 := &(*rtStack)[rtStackLen-1]
			(*rtStack) = (*rtStack)[:rtStackLen-2]

			cv1, ok := cast1(v1.Val)
			if !ok {
				return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
			}
			cv2, ok := cast2(v2.Val)
			if !ok {
				return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
			}

			ret := cmp(cv1, cv2)

			fn = func(i int) Bool3VL {
				return ret
			}
		} else if isConst1 {
			op1(0)

			preRtStackLen := len(*rtStack)
			v1 := &(*rtStack)[preRtStackLen-1]
			(*rtStack) = (*rtStack)[:preRtStackLen-1]

			cv1, ok := cast1(v1.Val)
			if !ok {
				return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
			}

			fn = func(i int) Bool3VL {
				op2(i)

				rtStackLen := len(*rtStack)
				v2 := &(*rtStack)[rtStackLen-1]
				(*rtStack) = (*rtStack)[:rtStackLen-1]

				cv2, ok := cast2(v2.Val)
				if !ok {
					return False3VL
				}

				return cmp(cv1, cv2)
			}
		} else if isConst2 {
			op2(0)

			preRtStackLen := len(*rtStack)
			v2 := &(*rtStack)[preRtStackLen-1]
			(*rtStack) = (*rtStack)[:preRtStackLen-1]

			cv2, ok := cast2(v2.Val)
			if !ok {
				return fmt.Errorf("BinaryInstruction failed: ty1=%v, ty2=%v", ty1, ty2)
			}

			fn = func(i int) Bool3VL {
				op1(i)

				rtStackLen := len(*rtStack)
				v1 := &(*rtStack)[rtStackLen-1]
				(*rtStack) = (*rtStack)[:rtStackLen-1]

				cv1, ok := cast1(v1.Val)
				if !ok {
					return False3VL
				}

				return cmp(cv1, cv2)
			}
		} else {
			fn = func(i int) Bool3VL {
				op1(i)
				op2(i)

				rtStackLen := len(*rtStack)
				v1 := &(*rtStack)[rtStackLen-2]
				v2 := &(*rtStack)[rtStackLen-1]
				(*rtStack) = (*rtStack)[:rtStackLen-2]

				cv1, ok := cast1(v1.Val)
				if !ok {
					return False3VL
				}
				cv2, ok := cast2(v2.Val)
				if !ok {
					return False3VL
				}

				return cmp(cv1, cv2)
			}
		}

		(*filterStack)[length-2] = FilterStackLeaf{
			Type:    Type_Invalid,
			Fn:      fn,
			IsConst: isConst1 && isConst2,
		}
		(*filterStack) = (*filterStack)[:length-1]

		rt.StackUsed = true
		return nil
	}
}
