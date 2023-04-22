package vmfilter

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func CallScalarFunction(rt *runtime.Runtime, name string, nArgs int) FilterGenFunc {
	return func(filterStack *[]FilterStackLeaf) error {
		length := len(*filterStack)
		if length < nArgs {
			return fmt.Errorf("Generating a filter operator And failed: len(stack)=%v", length)
		}

		scalarFnGen, ok := rt.Ctx.Funcs[strings.ToLower(name)]
		if !ok {
			return fmt.Errorf("CallScalarFunction failed: name=%v", name)
		}

		ops := make([]Filter3VLFunc, nArgs)
		args := make([]runtime.ArgInfo, nArgs)

		for i := 0; i < nArgs; i++ {
			item := &(*filterStack)[length-nArgs+i]
			args[i].Type = item.Type

			if item.IsConst {
				args[i].IsConst = item.IsConst
				item.Fn(0)

				rtStackLen := len(rt.Stack)
				if rtStackLen == 0 {
					return fmt.Errorf("CallScalarFunction failed: name=%v", name)
				}

				args[i].ConstVal = rt.Stack[rtStackLen-1].Val
				rt.Stack = rt.Stack[:rtStackLen-1]
			} else {
				ops[i] = item.Fn
			}
		}

		scalarFn, tyR, err := scalarFnGen(rt, args)
		if err != nil {
			return err
		}

		argBuf := make([]interface{}, nArgs)

		fn := func(i int) Bool3VL {
			for p, op := range ops {
				if op != nil {
					op(i)
					rtStackLen := len(rt.Stack)
					argBuf[p] = rt.Stack[rtStackLen-1].Val
					rt.Stack = rt.Stack[:rtStackLen-1]
				} else {
					argBuf[p] = args[p].ConstVal
				}
			}

			ret, err := scalarFn(i, argBuf...)

			if tyR != Type_Invalid {
				rt.Stack = append(rt.Stack, runtime.StackLeaf{
					Type: tyR,
					Val:  ret,
				})
			}

			if err != nil {
				atomic.StoreInt32(&rt.ErrorFlag, 1)
				rt.Err = err
				return False3VL
			}

			return False3VL
		}

		*filterStack = (*filterStack)[:len(*filterStack)-nArgs]
		*filterStack = append(*filterStack, FilterStackLeaf{
			Type:    tyR,
			Fn:      fn,
			IsConst: false,
		})
		rt.StackUsed = true
		return nil
	}
}
