package datatable

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// Filter rows. The index is changed.
// The order of the physical rows does not change.
// Destructive method.
func (p *DataTable) Filter(filters ...FilterInfo) error {
	p.indexChanged = true

	// TODO: Use goroutine if data is large; slice -> call filterCore -> join; filter func may be built each sliced chunk

	rt := runtime.NewRuntime(p.rtCtx)

	filterFn, _, err := p.buildFilterFunc(rt, filters...)
	if err != nil {
		return err
	}

	errFlag := &rt.ErrorFlag

	newIndex := make([]int, 0, p.index.Len())
	// NOTE: It may faster than p.index.Filter()

	if rt.StackUsed {
		rtStack := &rt.Stack
		for _, v := range p.index.GetRawValues() {
			if filterFn(v) == True3VL {
				newIndex = append(newIndex, v)
			}
			if atomic.LoadInt32(errFlag) != 0 {
				if rt.Err != nil {
					return rt.Err
				} else {
					return errors.New("DataTable:Filter failed: Error occurred")
				}
			}
			if len(*rtStack) > 0 {
				return fmt.Errorf("DataTable:Filter failed: Stack is not clean: %v", len(*rtStack))
			}
		}
	} else {
		for _, v := range p.index.GetRawValues() {
			if filterFn(v) == True3VL {
				newIndex = append(newIndex, v)
			}
			if atomic.LoadInt32(errFlag) != 0 {
				if rt.Err != nil {
					return rt.Err
				} else {
					return errors.New("DataTable:Filter failed: Error occurred")
				}
			}
		}
	}

	// Set and own
	p.index.SetRawValues(newIndex)

	return nil
}
