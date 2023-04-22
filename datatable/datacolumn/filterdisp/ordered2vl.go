package filterdisp

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/filter/fastfilter"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Nullable2VL[T Ordered](
	col *datacolimpl.DataColumnImpl[Nullable[T]],
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Eq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableEquals2VL(col, val)
		}
	case Op_Fast_NotEq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotEquals2VL(col, val)
		}
	case Op_Fast_IsNull:
		fn = fastfilter.NullableIsNull(col)
	case Op_Fast_IsNotNull:
		fn = fastfilter.NullableIsNotNull(col)
	case Op_Fast_Lt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableLessThan2VL(col, val)
		}
	case Op_Fast_Le:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableLessThanOrEquals2VL(col, val)
		}
	case Op_Fast_Gt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableGreaterThan2VL(col, val)
		}
	case Op_Fast_Ge:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableGreaterThanOrEquals2VL(col, val)
		}
	case Op_Fast_In:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableIn2VL(col, val)
		}
	case Op_Fast_NotIn:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("Nullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotIn2VL(col, val)
		}
	case Op_Fast_Like,
		Op_Fast_NotLike,
		Op_Fast_Match,
		Op_Fast_NotMatch,
		Op_Fast_Includes,
		Op_Fast_Excludes:
		err = errors.New("Nullable2VL: Unknown operator")
	default:
		err = errors.New("Nullable2VL: Unknown operator")
	}

	return fn, err
}

func StringNullable2VL[T ~string](
	col *datacolimpl.DataColumnImpl[Nullable[T]],
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Like:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable2VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableLike2VL(col, val)
		}
	case Op_Fast_NotLike:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable2VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotLike2VL(col, val)
		}
	case Op_Fast_Match:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable2VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableMatch2VL(col, val)
		}
	case Op_Fast_NotMatch:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable2VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotMatch2VL(col, val)
		}
	case Op_Fast_Includes:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable2VL: Incorrect column type")
		}
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("StringNullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableIncludes2VL(col, val)
		}
	case Op_Fast_Excludes:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable2VL: Incorrect column type")
		}
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("StringNullable2VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableExcludes2VL(col, val)
		}
	default:
		fn, err = Nullable2VL(col, op, v)
	}

	return fn, err
}
