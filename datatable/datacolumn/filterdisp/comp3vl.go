package filterdisp

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/filter/fastfilter"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func ComparableNullable3VL[T comparable](
	col *datacolimpl.DataColumnImpl[Nullable[T]],
	cmpLt func(a, b T) bool, cmpLe func(a, b T) bool,
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Eq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableEquals3VL(col, val)
		}
	case Op_Fast_NotEq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotEquals3VL(col, val)
		}
	case Op_Fast_IsNull:
		fn = fastfilter.NullableIsNull(col)
	case Op_Fast_IsNotNull:
		fn = fastfilter.NullableIsNotNull(col)
	case Op_Fast_Lt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.XNullableLessThan3VL(col, val, cmpLt)
		}
	case Op_Fast_Le:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.XNullableLessThanOrEquals3VL(col, val, cmpLe)
		}
	case Op_Fast_Gt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.XNullableGreaterThan3VL(col, val, cmpLt)
		}
	case Op_Fast_Ge:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.XNullableGreaterThanOrEquals3VL(col, val, cmpLe)
		}
	case Op_Fast_In:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableIn3VL(col, val)
		}
	case Op_Fast_NotIn:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("ComparableNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotIn3VL(col, val)
		}
	case Op_Fast_Like,
		Op_Fast_NotLike,
		Op_Fast_Match,
		Op_Fast_NotMatch,
		Op_Fast_Includes,
		Op_Fast_Excludes:
		err = errors.New("ComparableNullable3VL: Invalid operator for the column type")
	default:
		err = errors.New("ComparableNullable3VL: Unknown operator")
	}

	return fn, err
}
