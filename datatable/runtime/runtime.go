package runtime

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

const (
	runtime_DefaultStackSize = 64
)

type ArgInfo struct {
	Type     DataColumnType
	IsConst  bool
	ConstVal interface{}
}

type HostFunction = func(i int, args ...interface{}) (interface{}, error)
type HostFuncGen = func(rt *Runtime, argsInfo []ArgInfo) (HostFunction, DataColumnType, error)

type StackLeaf struct {
	Type DataColumnType
	Val  interface{}
}

type RuntimeContext struct {
	Consts map[string]interface{}
	Vars   map[string]interface{}
	Funcs  map[string]HostFuncGen
}

func NewRuntimeContext() *RuntimeContext {
	return &RuntimeContext{
		Consts: map[string]interface{}{},
		Vars:   map[string]interface{}{},
		Funcs:  map[string]HostFuncGen{},
	}
}

type Runtime struct {
	ErrorFlag int32
	Err       error
	Ctx       *RuntimeContext
	Stack     []StackLeaf
	StackUsed bool
	PrevRow   int
}

func NewRuntime(ctx *RuntimeContext) *Runtime {
	return &Runtime{
		Ctx:   ctx,
		Stack: make([]StackLeaf, 0, runtime_DefaultStackSize),
	}
}
