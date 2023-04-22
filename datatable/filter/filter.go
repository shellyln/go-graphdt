package filter

import (
	"fmt"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

func True() FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := func(i int) Bool3VL {
			return True3VL
		}
		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

func False() FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := func(i int) Bool3VL {
			return False3VL
		}
		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

func Unknown() FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		fn := func(i int) Bool3VL {
			return Unknown3VL
		}
		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Invalid,
			Fn:   fn,
		})
		return nil
	}
}

func Not() FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		length := len(*filterStack)
		if length < 1 {
			return fmt.Errorf("Generating a filter operator Not failed: len(stack)=%v", length)
		}

		isConst1 := (*filterStack)[length-1].IsConst
		op1 := (*filterStack)[length-1].Fn

		fn := func(i int) Bool3VL {
			ret := ^op1(i) & True3VL // bitwise
			switch ret {
			case True3VL, False3VL:
				return ret
			default:
				return Unknown3VL
			}
		}

		(*filterStack)[length-1] = FilterStackLeaf{
			Type:    Type_Invalid,
			Fn:      fn,
			IsConst: isConst1,
		}
		return nil
	}
}

func And() FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		length := len(*filterStack)
		if length < 2 {
			return fmt.Errorf("Generating a filter operator And failed: len(stack)=%v", length)
		}

		isConst1 := (*filterStack)[length-2].IsConst
		isConst2 := (*filterStack)[length-1].IsConst

		op1 := (*filterStack)[length-2].Fn
		op2 := (*filterStack)[length-1].Fn

		fn := func(i int) Bool3VL {
			r1 := op1(i)
			if r1 == False3VL {
				return False3VL
			}
			return r1 & op2(i) // bitwise
		}

		(*filterStack)[length-2] = FilterStackLeaf{
			Type:    Type_Invalid,
			Fn:      fn,
			IsConst: isConst1 && isConst2,
		}
		(*filterStack) = (*filterStack)[:length-1]
		return nil
	}
}

func Or() FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		length := len(*filterStack)
		if length < 2 {
			return fmt.Errorf("Generating a filter operator Or failed: len(stack)=%v", length)
		}

		isConst1 := (*filterStack)[length-2].IsConst
		isConst2 := (*filterStack)[length-1].IsConst

		op1 := (*filterStack)[length-2].Fn
		op2 := (*filterStack)[length-1].Fn

		fn := func(i int) Bool3VL {
			r1 := op1(i)
			if r1 == True3VL {
				return True3VL
			}
			ret := r1 | op2(i) // bitwise
			switch ret {
			case True3VL, False3VL:
				return ret
			default:
				return Unknown3VL
			}
		}

		(*filterStack)[length-2] = FilterStackLeaf{
			Type:    Type_Invalid,
			Fn:      fn,
			IsConst: isConst1 && isConst2,
		}
		(*filterStack) = (*filterStack)[:length-1]
		return nil
	}
}
