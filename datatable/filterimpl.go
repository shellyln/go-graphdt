package datatable

import (
	"errors"
	"strconv"

	"github.com/shellyln/go-graphdt/datatable/filter"
	"github.com/shellyln/go-graphdt/datatable/filter/vmfilter"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// Check filter parameters.
func (p *DataTable) checkFilterParams(rt *runtime.Runtime, filters ...FilterInfo) error {
	for i := 0; i < len(filters); i++ {
		switch filters[i].Op {
		case Op_LoadCol, Op_LoadPreviousCol, Op_LoadColAsList:
			if filters[i].Col < 0 || len(p.cols) <= filters[i].Col {
				return errors.New("Op_LoadCol: Column out of range")
			}
		case Op_LoadVar:
			name, ok := filters[i].Param.(string)
			if !ok {
				return errors.New("Op_LoadVar: bad parameter type")
			}
			_, ok = rt.Ctx.Vars[name]
			if !ok {
				return errors.New("Op_LoadVar: unknown name")
			}
		case Op_LoadConst:
			name, ok := filters[i].Param.(string)
			if !ok {
				return errors.New("Op_LoadConst: bad parameter type")
			}
			_, ok = rt.Ctx.Consts[name]
			if !ok {
				return errors.New("Op_LoadConst: unknown name")
			}
		case Op_Call:
			name, ok := filters[i].Param.(string)
			if !ok {
				return errors.New("Op_Call: bad parameter type")
			}
			_, ok = rt.Ctx.Funcs[name]
			if !ok {
				return errors.New("Op_Call: unknown name")
			}
		}
	}
	return nil
}

// Build filter function.
func (p *DataTable) buildFilterFunc(rt *runtime.Runtime, filters ...FilterInfo) (Filter3VLFunc, DataColumnType, error) {
	var err error

	if err = p.checkFilterParams(rt, filters...); err != nil {
		return nil, Type_Invalid, err
	}

	filterStack := make([]FilterStackLeaf, 0)

	// Optimize operators
	for i := 0; i < len(filters)-2; i++ {
		if filters[i].Op == Op_LoadCol && filters[i+1].Op == Op_LoadImmediate && !DbgNoFastFilter {
			switch filters[i+2].Op {
			case Op_Eq,
				Op_NotEq,
				Op_IsNull,
				Op_IsNotNull,
				Op_Lt,
				Op_Le,
				Op_Gt,
				Op_Ge,
				Op_Like,
				Op_NotLike,
				Op_Match,
				Op_NotMatch,
				Op_In,
				Op_NotIn,
				Op_Includes,
				Op_Excludes:

				filters[i].Op = filters[i+2].Op + Op_FastOp_Magic
				filters[i].Param = filters[i+1].Param
				filters[i+1].Op = Op_Noop
				filters[i+2].Op = Op_Noop
				i += 2
			}
		}
	}

LOOP:
	for _, fr := range filters {
		switch fr.Op {
		case Op_Noop:
			// do nothing
		case Op_Not:
			err = filter.Not()(&filterStack)
		case Op_And:
			err = filter.And()(&filterStack)
		case Op_Or:
			err = filter.Or()(&filterStack)

		case Op_Eq:
			// TODO: if filterStack operand1 is DateTime and operand2 is DateTimeRange, change to InRange/NotInRange
			if p.is3VL {
				err = vmfilter.Equals3VL(rt)(&filterStack)
			} else {
				err = vmfilter.Equals2VL(rt)(&filterStack)
			}
		case Op_NotEq:
			// TODO: if filterStack operand1 is DateTime and operand2 is DateTimeRange, change to InRange/NotInRange
			if p.is3VL {
				err = vmfilter.NotEquals3VL(rt)(&filterStack)
			} else {
				err = vmfilter.NotEquals2VL(rt)(&filterStack)
			}
		case Op_IsNull:
			if p.is3VL {
				err = vmfilter.IsNull3VL(rt)(&filterStack)
			} else {
				err = vmfilter.IsNull2VL(rt)(&filterStack)
			}
		case Op_IsNotNull:
			if p.is3VL {
				err = vmfilter.IsNotNull3VL(rt)(&filterStack)
			} else {
				err = vmfilter.IsNotNull2VL(rt)(&filterStack)
			}
		case Op_Lt:
			if p.is3VL {
				err = vmfilter.LessThan3VL(rt)(&filterStack)
			} else {
				err = vmfilter.LessThan2VL(rt)(&filterStack)
			}
		case Op_Le:
			if p.is3VL {
				err = vmfilter.LessThanOrEquals3VL(rt)(&filterStack)
			} else {
				err = vmfilter.LessThanOrEquals2VL(rt)(&filterStack)
			}
		case Op_Gt:
			if p.is3VL {
				err = vmfilter.GreaterThan3VL(rt)(&filterStack)
			} else {
				err = vmfilter.GreaterThan2VL(rt)(&filterStack)
			}
		case Op_Ge:
			if p.is3VL {
				err = vmfilter.GreaterThanOrEquals3VL(rt)(&filterStack)
			} else {
				err = vmfilter.GreaterThanOrEquals2VL(rt)(&filterStack)
			}
		case Op_Like:
			if p.is3VL {
				err = vmfilter.Like3VL(rt)(&filterStack)
			} else {
				err = vmfilter.Like2VL(rt)(&filterStack)
			}
		case Op_NotLike:
			if p.is3VL {
				err = vmfilter.NotLike3VL(rt)(&filterStack)
			} else {
				err = vmfilter.NotLike2VL(rt)(&filterStack)
			}
		case Op_Match:
			if p.is3VL {
				err = vmfilter.Match3VL(rt)(&filterStack)
			} else {
				err = vmfilter.Match2VL(rt)(&filterStack)
			}
		case Op_NotMatch:
			if p.is3VL {
				err = vmfilter.NotMatch3VL(rt)(&filterStack)
			} else {
				err = vmfilter.NotMatch2VL(rt)(&filterStack)
			}
		case Op_In:
			if p.is3VL {
				err = vmfilter.In3VL(rt)(&filterStack)
			} else {
				err = vmfilter.In2VL(rt)(&filterStack)
			}
		case Op_NotIn:
			if p.is3VL {
				err = vmfilter.NotIn3VL(rt)(&filterStack)
			} else {
				err = vmfilter.NotIn2VL(rt)(&filterStack)
			}
		case Op_Includes:
			if p.is3VL {
				err = vmfilter.Includes3VL(rt)(&filterStack)
			} else {
				err = vmfilter.Includes2VL(rt)(&filterStack)
			}
		case Op_Excludes:
			if p.is3VL {
				err = vmfilter.Excludes3VL(rt)(&filterStack)
			} else {
				err = vmfilter.Excludes2VL(rt)(&filterStack)
			}

		case Op_Fast_Eq,
			Op_Fast_NotEq,
			Op_Fast_IsNull,
			Op_Fast_IsNotNull,
			Op_Fast_Lt,
			Op_Fast_Le,
			Op_Fast_Gt,
			Op_Fast_Ge,
			Op_Fast_Like,
			Op_Fast_NotLike,
			Op_Fast_Match,
			Op_Fast_NotMatch,
			Op_Fast_In,
			Op_Fast_NotIn,
			Op_Fast_Includes,
			Op_Fast_Excludes:
			// TODO: if op is Eq/NotEq and Col is T and Param is ValueRange[T], Change op to InRange/NotInRange

			var genFn FilterGenFunc
			if p.is3VL {
				genFn, err = p.cols[fr.Col].GetFilter3VL(fr.Op, fr.Param)
			} else {
				genFn, err = p.cols[fr.Col].GetFilter2VL(fr.Op, fr.Param)
			}
			if err != nil {
				break LOOP
			}
			err = genFn(&filterStack)

		case Op_LoadImmediate:
			err = vmfilter.LoadImmediate(rt, fr.Param)(&filterStack)
		case Op_LoadVar:
			name, ok := fr.Param.(string)
			if !ok {
				err = errors.New("DataTable.Filter: Incorrect parameter type")
				break LOOP
			}
			err = vmfilter.LoadVar(rt, name)(&filterStack)
		case Op_LoadConst:
			name, ok := fr.Param.(string)
			if !ok {
				err = errors.New("DataTable.Filter: Incorrect parameter type")
				break LOOP
			}
			err = vmfilter.LoadConst(rt, name)(&filterStack)
		case Op_LoadCol:
			err = vmfilter.LoadCol(rt, p.cols[fr.Col])(&filterStack)
		case Op_LoadRowId:
			err = vmfilter.LoadRowId(rt)(&filterStack)
		case Op_LoadPreviousCol:
			err = vmfilter.LoadPreviousCol(rt, p.cols[fr.Col])(&filterStack)
		case Op_LoadPreviousRowId:
			err = vmfilter.LoadPreviousRowId(rt)(&filterStack)
		case Op_LoadColAsList:
			err = vmfilter.LoadColAsList(rt, p.index, p.cols[fr.Col])(&filterStack)
		case Op_LoadRowIdsAsList:
			err = vmfilter.LoadRowIdsAsList(rt, p.index)(&filterStack)

		case Op_Call:
			name, ok := fr.Param.(string)
			if !ok {
				err = errors.New("DataTable.Filter: Incorrect parameter type")
				break LOOP
			}
			err = vmfilter.CallScalarFunction(rt, name, fr.NArgs)(&filterStack)

		default:
			err = errors.New("DataTable.Filter: Unknown operator:" + strconv.Itoa(int(fr.Op)))
			break LOOP
		}

		if err != nil {
			break
		}
	}
	if err != nil {
		return nil, Type_Invalid, err
	}

	if len(filterStack) != 1 {
		return nil, Type_Invalid, errors.New("DataTable.Filter: Filter generation failed")
	}
	return filterStack[0].Fn, filterStack[0].Type, nil
}
