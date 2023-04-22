package sort

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

type SortType int

const (
	SortType_Asc SortType = iota + 1
	SortType_Desc
	SortType_AscNullsFirst
	SortType_DescNullsFirst
	SortType_AscNullsLast
	SortType_DescNullsLast
)

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func CombineSortLessFuncs(funcs []func(a, b int) Bool3VL) func(a, b int) bool {
	next := funcs[len(funcs)-1]
	for i := len(funcs) - 2; i >= 0; i-- {
		fn1 := funcs[i]
		fn2 := next
		next = func(a, b int) Bool3VL {
			r := fn1(a, b)
			switch r {
			case True3VL, False3VL:
				return r
			}
			return fn2(a, b)
		}
	}
	combined := next
	return func(a, b int) bool {
		return combined(a, b) == True3VL
	}
}

func Dummy(a, b int) Bool3VL {
	return True3VL
}

func Asc[T Ordered](a, b T) Bool3VL {
	if a < b {
		return True3VL
	} else if a == b {
		return Unknown3VL
	} else {
		return False3VL
	}
}

func Desc[T Ordered](a, b T) Bool3VL {
	if b < a {
		return True3VL
	} else if b == a {
		return Unknown3VL
	} else {
		return False3VL
	}
}
