package vmfilter

import (
	"fmt"
	"reflect"

	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func LoadImmediate(rt *runtime.Runtime, v interface{}) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		ty := cast.GetType(reflect.TypeOf(v))
		if ty == Type_Invalid {
			return fmt.Errorf("Generating a filter operator LoadImmediate failed: v=%v", v)
		}

		rtStack := &rt.Stack
		fn := func(i int) Bool3VL {
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: ty,
				Val:  v,
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:    ty,
			Fn:      fn,
			IsConst: true,
		})
		rt.StackUsed = true
		return nil
	}
}

func LoadConst(rt *runtime.Runtime, name string) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		v, ok := rt.Ctx.Consts[name]
		if !ok {
			return fmt.Errorf("Generating a filter operator LoadConst failed: name=%v", name)
		}

		ty := cast.GetType(reflect.TypeOf(v))
		if ty == Type_Invalid {
			return fmt.Errorf("Generating a filter operator LoadConst failed: name=%v, v=%v", name, v)
		}

		rtStack := &rt.Stack
		fn := func(i int) Bool3VL {
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: ty,
				Val:  v,
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:    ty,
			Fn:      fn,
			IsConst: true,
		})
		rt.StackUsed = true
		return nil
	}
}

func LoadVar(rt *runtime.Runtime, name string) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		v, ok := rt.Ctx.Vars[name]
		if !ok {
			return fmt.Errorf("Generating a filter operator LoadVar failed: name=%v", name)
		}

		ty := cast.GetType(reflect.TypeOf(v))
		if ty == Type_Invalid {
			return fmt.Errorf("Generating a filter operator LoadVar failed: name=%v, v=%v", name, v)
		}

		rtStack := &rt.Stack
		fn := func(i int) Bool3VL {
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: ty,
				Val:  v,
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:    ty,
			Fn:      fn,
			IsConst: true,
		})
		rt.StackUsed = true
		return nil
	}
}

func LoadCol(rt *runtime.Runtime, adc AnyDataColumn) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		dc := adc.GetImpl()
		ty := dc.GetType()
		if ty == Type_Invalid {
			return fmt.Errorf("Generating a filter operator LoadCol failed")
		}

		rtStack := &rt.Stack
		fn := func(i int) Bool3VL {
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: ty,
				Val:  dc.GetAny(i),
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:  ty,
			Fn:    fn,
			IsCol: true,
		})
		rt.StackUsed = true
		return nil
	}
}

func LoadRowId(rt *runtime.Runtime) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		rtStack := &rt.Stack
		fn := func(i int) Bool3VL {
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: Type_Int,
				Val:  i,
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:  Type_Int,
			Fn:    fn,
			IsCol: true,
		})
		rt.StackUsed = true
		return nil
	}
}

func LoadPreviousCol(rt *runtime.Runtime, adc AnyDataColumn) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		dc := adc.GetImpl()
		ty := dc.GetType()
		if ty == Type_Invalid {
			return fmt.Errorf("Generating a filter operator LoadPreviousCol failed")
		}

		rtStack := &rt.Stack
		fn := func(i int) Bool3VL {
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: ty,
				Val:  dc.GetAny(rt.PrevRow),
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:  ty,
			Fn:    fn,
			IsCol: true,
		})
		rt.StackUsed = true
		return nil
	}
}

func LoadPreviousRowId(rt *runtime.Runtime) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		rtStack := &rt.Stack
		fn := func(i int) Bool3VL {
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: Type_Int,
				Val:  rt.PrevRow,
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:  Type_Int,
			Fn:    fn,
			IsCol: true,
		})
		rt.StackUsed = true
		return nil
	}
}

func LoadColAsList(rt *runtime.Runtime, index *datacolimpl.DataColumnImpl[int], adc AnyDataColumn) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		rtStack := &rt.Stack

		dc := adc.GetImpl()
		ty := dc.GetType()
		if ty == Type_Invalid {
			return fmt.Errorf("Generating a filter operator LoadColAsList failed")
		}
		ty |= Type_Flag_Array

		buf := dc.MakeBufferAsAny(dc.Len())

		fn := func(i int) Bool3VL {
			rawIndex := index.GetRawValues()
			dc.CopyBufferByIndex(&buf, rawIndex)
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: ty,
				Val:  buf,
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: ty,
			Fn:   fn,
		})

		rt.StackUsed = true
		return nil
	}
}

func LoadRowIdsAsList(rt *runtime.Runtime, index *datacolimpl.DataColumnImpl[int]) FilterGenFunc {
	// filterStack: pop=0, push=1
	// execStack:   pop=0, push=1
	return func(filterStack *[]FilterStackLeaf) error {
		rtStack := &rt.Stack

		fn := func(i int) Bool3VL {
			rawIndex := index.GetRawValues()
			*rtStack = append(*rtStack, runtime.StackLeaf{
				Type: Type_Int | Type_Flag_Array,
				Val:  rawIndex,
			})
			return False3VL
		}

		*filterStack = append(*filterStack, FilterStackLeaf{
			Type: Type_Int | Type_Flag_Array,
			Fn:   fn,
		})

		rt.StackUsed = true
		return nil
	}
}
