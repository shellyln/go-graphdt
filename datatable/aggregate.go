package datatable

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/datacolumn"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// Aggregate rows by `group-by` columns and apply aggregate functions to the other columns.
// Filter aggregated rows by `having` clause.
func (p *DataTable) Aggregate(groupBy []int, having []FilterInfo, cols ...SelectInfo) (*DataTable, error) {

	isGroupByCol := func(col int) bool {
		for _, v := range groupBy {
			if v == col {
				return true
			}
		}
		return false
	}

	transformFormula := func(fl []FilterInfo) []FilterInfo {
		formula := make([]FormulaInfo, len(fl))
		copy(formula, fl)

		for j := 0; j < len(formula); j++ {
			if isGroupByCol(formula[j].Col) && formula[j].Op == Op_LoadCol {
				formula[j].Op = Op_LoadColAsList
			}

			if formula[j].Op == Op_Call {
				if name, ok := formula[j].Param.(string); ok && strings.EqualFold("count", name) {
					if formula[j].NArgs == 0 {
						formula[j].NArgs = 1
						formula = insert(formula, j, FormulaInfo{
							Op: Op_LoadRowIdsAsList,
						})
						j++
					}
				}
			}
		}
		return formula
	}

	ret := &DataTable{
		is3VL:        p.is3VL,
		header:       make([]dtHeader, len(cols)),
		cols:         make([]AnyDataColumn, len(cols)),
		index:        datacolimpl.NewDataColumnImplWithSize[int](0, datacolimpl.DataColumnImpl_DefaultSize, Type_Int),
		indexChanged: false,
		rtCtx:        p.rtCtx,
		relations:    p.relations,
	}

	sortInfos := make([]SortInfo, len(groupBy))
	for i, col := range groupBy {
		sortInfos[i].Col = col
	}

	tmp := p.Borrow()
	if err := tmp.Sort(sortInfos...); err != nil {
		return nil, err
	}

	colIndexDict := make(map[int]int)
	for i := 0; i < len(cols); i++ {
		if cols[i].Formula == nil {
			// simple column
			colIndexDict[i] = cols[i].Col
		}
	}

	for i := 0; i < len(cols); i++ {
		if cols[i].Formula == nil {
			// simple column

			if cols[i].Col >= len(tmp.header) || cols[i].Col < 0 {
				return nil, errors.New("DataTable:Aggregate failed: Invalid column index")
			}
			if !isGroupByCol(cols[i].Col) {
				return nil, errors.New("DataTable:Aggregate failed: Column is not in groupBy columns")
			}

			ret.header[i] = tmp.header[cols[i].Col]

			if idx, ok := colIndexDict[ret.header[i].parentColIndex]; ok {
				ret.header[i].parentColIndex = idx
			} else {
				ret.header[i].parentColIndex = -1
			}

			if cols[i].As != "" {
				ret.header[i].name = []string{cols[i].As}
			}

			ret.cols[i] = datacolumn.NewDataColumnWithSize(0, datacolimpl.DataColumnImpl_DefaultSize, tmp.cols[cols[i].Col].GetType())
		}
	}

	for i := 0; i < len(ret.relations); i++ {
		if idx, ok := colIndexDict[ret.relations[i].keyColIndex]; ok {
			ret.relations[i].keyColIndex = idx
		} else {
			ret.relations[i].keyColIndex = -1
		}
	}

	rt := runtime.NewRuntime(tmp.rtCtx)
	errFlag := &rt.ErrorFlag
	rtStack := &rt.Stack

	var havingFn Filter3VLFunc
	selectFuncs := make([]Filter3VLFunc, len(cols))

	if len(having) > 0 {
		if f, _, err := tmp.buildFilterFunc(rt, transformFormula(having)...); err != nil {
			return nil, err
		} else {
			havingFn = f
		}
	}

	for i := 0; i < len(cols); i++ {
		if cols[i].Formula != nil {
			// complex column

			formula := transformFormula(cols[i].Formula)

			selectFn, tyR, err := tmp.buildFilterFunc(rt, formula...)
			if err != nil {
				return nil, err
			}
			selectFuncs[i] = selectFn

			ret.header[i].parentColIndex = -1
			ret.header[i].relIndex = -1

			if cols[i].As != "" {
				ret.header[i].name = []string{cols[i].As}
			}

			ret.cols[i] = datacolumn.NewDataColumnWithSize(0, datacolimpl.DataColumnImpl_DefaultSize, tyR)
		}
	}

	rawIndexSaved := tmp.index.GetRawValues()

	if err := tmp.grouping(func(start, end int) error {
		rawIndex := rawIndexSaved[start:end]
		tmp.index.SetRawValues(rawIndex)

		if havingFn != nil {
			r := havingFn(rawIndex[0])
			*rtStack = (*rtStack)[0:0]

			if r != True3VL {
				return nil
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

		length := ret.Len() + 1
		ret.Grow(length)

		for i := 0; i < len(cols); i++ {
			if cols[i].Formula == nil {
				// simple column
				ret.cols[i].SetAny(length-1, tmp.cols[cols[i].Col].GetAny(rawIndex[0]))
			} else {
				// complex column

				selectFuncs[i](rawIndex[0])

				if atomic.LoadInt32(errFlag) != 0 {
					if rt.Err != nil {
						return rt.Err
					} else {
						return errors.New("DataTable:Filter failed: Error occurred")
					}
				}
				if len(*rtStack) != 1 {
					return fmt.Errorf("DataTable:Filter failed: Stack is not clean: %v", len(*rtStack))
				}

				col := ret.cols[i].GetImpl()
				if err := col.SetAnyNoCast(length-1, (*rtStack)[0].Val); err != nil {
					return err
				}

				*rtStack = (*rtStack)[0:0]
			}
		}

		return nil
	}, groupBy); err != nil {
		return nil, err
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

// Helper function for grouping, it will call iter for each group.
func (p *DataTable) grouping(iter func(start, end int) error, groupBy []int) error {
	filterInfos := make([]FilterInfo, len(groupBy)*4)
	for i, col := range groupBy {
		filterInfos[i*4] = FilterInfo{
			Op:  Op_LoadPreviousCol,
			Col: col,
		}
		filterInfos[i*4+1] = FilterInfo{
			Op:  Op_LoadCol,
			Col: col,
		}
		filterInfos[i*4+2] = FilterInfo{
			Op: Op_Eq,
		}
		if i > 0 {
			filterInfos[i*4+3] = FilterInfo{
				Op: Op_And,
			}
		} else {
			filterInfos[i*4+3] = FilterInfo{
				Op: Op_Noop,
			}
		}
	}

	rt := runtime.NewRuntime(p.rtCtx)
	filterFn, _, err := p.buildFilterFunc(rt, filterInfos...)
	if err != nil {
		return err
	}

	errFlag := &rt.ErrorFlag
	rawIndex := p.index.GetRawValues()
	length := p.index.Len()
	rtStack := &rt.Stack

	// TODO: Use goroutine if data is large

	start := 0
	rt.PrevRow = rawIndex[0]

	for i := 1; i < length; i++ {
		currRow := rawIndex[i]
		if filterFn(currRow) != True3VL {
			// group ends
			if err := iter(start, i); err != nil {
				return err
			}
			start = i
			rt.PrevRow = currRow
		}
		if atomic.LoadInt32(errFlag) != 0 {
			if rt.Err != nil {
				return rt.Err
			} else {
				return errors.New("DataTable:grouping failed: Error occurred")
			}
		}
		if len(*rtStack) > 0 {
			return fmt.Errorf("DataTable:grouping failed: Stack is not clean: %v", len(*rtStack))
		}
	}
	if length > 0 {
		if err := iter(start, length); err != nil {
			return err
		}
	}

	return nil
}
