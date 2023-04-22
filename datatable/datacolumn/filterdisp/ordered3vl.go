package filterdisp

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/filter/fastfilter"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func Nullable3VL[T Ordered](
	col *datacolimpl.DataColumnImpl[Nullable[T]],
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Eq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableEquals3VL(col, val)
		}
	case Op_Fast_NotEq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable3VL: Incorrect parameter type")
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
			err = errors.New("Nullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableLessThan3VL(col, val)
		}
	case Op_Fast_Le:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableLessThanOrEquals3VL(col, val)
		}
	case Op_Fast_Gt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableGreaterThan3VL(col, val)
		}
	case Op_Fast_Ge:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("Nullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableGreaterThanOrEquals3VL(col, val)
		}
	case Op_Fast_In:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("Nullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableIn3VL(col, val)
		}
	case Op_Fast_NotIn:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("Nullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotIn3VL(col, val)
		}
	case Op_Fast_Like,
		Op_Fast_NotLike,
		Op_Fast_Match,
		Op_Fast_NotMatch,
		Op_Fast_Includes,
		Op_Fast_Excludes:
		err = errors.New("Nullable3VL: Unknown operator")
	default:
		err = errors.New("Nullable3VL: Unknown operator")
	}

	return fn, err
}

func StringNullable3VL[T ~string](
	col *datacolimpl.DataColumnImpl[Nullable[T]],
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Like:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable3VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableLike3VL(col, val)
		}
	case Op_Fast_NotLike:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable3VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotLike3VL(col, val)
		}
	case Op_Fast_Match:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable3VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableMatch3VL(col, val)
		}
	case Op_Fast_NotMatch:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable3VL: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableNotMatch3VL(col, val)
		}
	case Op_Fast_Includes:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable3VL: Incorrect column type")
		}
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("StringNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableIncludes3VL(col, val)
		}
	case Op_Fast_Excludes:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNullable3VL: Incorrect column type")
		}
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("StringNullable3VL: Incorrect parameter type")
		} else {
			fn = fastfilter.NullableExcludes3VL(col, val)
		}
	default:
		fn, err = Nullable3VL(col, op, v)
	}

	return fn, err
}
