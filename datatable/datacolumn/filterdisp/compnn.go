package filterdisp

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/filter"
	"github.com/shellyln/go-graphdt/datatable/filter/fastfilter"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func ComparableNonNull[T comparable](
	col *datacolimpl.DataColumnImpl[T],
	cmpLt func(a, b T) bool, cmpLe func(a, b T) bool,
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Eq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.Equals(col, val)
		}
	case Op_Fast_NotEq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.NotEquals(col, val)
		}
	case Op_Fast_IsNull:
		fn = filter.False()
	case Op_Fast_IsNotNull:
		fn = filter.True()
	case Op_Fast_Lt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.XLessThan(col, val, cmpLt)
		}
	case Op_Fast_Le:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.XLessThanOrEquals(col, val, cmpLe)
		}
	case Op_Fast_Gt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.XGreaterThan(col, val, cmpLt)
		}
	case Op_Fast_Ge:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.XGreaterThanOrEquals(col, val, cmpLe)
		}
	case Op_Fast_In:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.In(col, val)
		}
	case Op_Fast_NotIn:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("ComparableNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.NotIn(col, val)
		}
	case Op_Fast_Like,
		Op_Fast_NotLike,
		Op_Fast_Match,
		Op_Fast_NotMatch,
		Op_Fast_Includes,
		Op_Fast_Excludes:
		err = errors.New("ComparableNonNull: Invalid operator for the column type")
	default:
		err = errors.New("ComparableNonNull: Unknown operator")
	}

	return fn, err
}
