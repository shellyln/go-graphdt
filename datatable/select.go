package datatable

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/shellyln/go-graphdt/datatable/datacolumn"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// Returns a new data table with the columns transformed.
// If all columns of a new table are not populated by formulas,
// the columns are shared with the original data table.
func (p *DataTable) Select(cols ...SelectInfo) (*DataTable, error) {

	ret := &DataTable{
		is3VL:        p.is3VL,
		header:       make([]dtHeader, len(cols)),
		cols:         make([]AnyDataColumn, len(cols)),
		index:        p.index.Borrow(),
		indexChanged: p.indexChanged,
		rtCtx:        p.rtCtx,
		relations:    p.relations,
	}

	colIndexDict := make(map[int]int)
	for i := 0; i < len(cols); i++ {
		if cols[i].Formula == nil {
			// simple column
			colIndexDict[i] = cols[i].Col
		}
	}

	simple := true
	for i := 0; i < len(cols); i++ {
		if cols[i].Formula == nil {
			if cols[i].Col >= len(p.header) || cols[i].Col < 0 {
				return nil, errors.New("DataTable:Select failed: Invalid column index")
			}
			ret.header[i] = p.header[cols[i].Col]

			if idx, ok := colIndexDict[ret.header[i].parentColIndex]; ok {
				ret.header[i].parentColIndex = idx
			} else {
				ret.header[i].parentColIndex = -1
			}

			if cols[i].As != "" {
				ret.header[i].name = []string{cols[i].As}
			}

			ret.cols[i] = p.cols[cols[i].Col].BorrowAsAny()
		} else {
			simple = false
		}
	}

	for i := 0; i < len(ret.relations); i++ {
		if idx, ok := colIndexDict[ret.relations[i].keyColIndex]; ok {
			ret.relations[i].keyColIndex = idx
		} else {
			ret.relations[i].keyColIndex = -1
		}
	}

	if !simple {
		ret.Materialize()

		// TODO: Use goroutine if data is large

		rt := runtime.NewRuntime(p.rtCtx)
		errFlag := &rt.ErrorFlag
		rtStack := &rt.Stack

		for i := 0; i < len(cols); i++ {
			if cols[i].Formula != nil {
				selectFn, tyR, err := p.buildFilterFunc(rt, cols[i].Formula...)
				if err != nil {
					return nil, err
				}

				ret.header[i].parentColIndex = -1
				ret.header[i].relIndex = -1

				if cols[i].As != "" {
					ret.header[i].name = []string{cols[i].As}
				}

				ret.cols[i] = datacolumn.NewDataColumnWithSize(p.cols[0].Len(), p.cols[0].Cap(), tyR)

				col := ret.cols[i].GetImpl()

				for _, v := range p.index.GetRawValues() {
					selectFn(v)

					if atomic.LoadInt32(errFlag) != 0 {
						if rt.Err != nil {
							return nil, rt.Err
						} else {
							return nil, errors.New("DataTable:Filter failed: Error occurred")
						}
					}
					if len(*rtStack) != 1 {
						return nil, fmt.Errorf("DataTable:Filter failed: Stack is not clean: %v", len(*rtStack))
					}

					if err := col.SetAnyNoCast(v, (*rtStack)[0].Val); err != nil {
						return nil, err
					}

					*rtStack = (*rtStack)[0:0]
				}
			}
		}
	}

	colNames := make([][]string, len(ret.header))
	for i, hdr := range ret.header {
		colNames[i] = hdr.name
	}
	if err := ret.SetHeader(colNames); err != nil {
		return nil, err
	}

	return ret, nil
}
