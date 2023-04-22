package filterdisp

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/filter"
	"github.com/shellyln/go-graphdt/datatable/filter/fastfilter"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func NonNull[T Ordered](
	col *datacolimpl.DataColumnImpl[T],
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Eq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("NonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.Equals(col, val)
		}
	case Op_Fast_NotEq:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("NonNull: Incorrect parameter type")
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
			err = errors.New("NonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.LessThan(col, val)
		}
	case Op_Fast_Le:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("NonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.LessThanOrEquals(col, val)
		}
	case Op_Fast_Gt:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("NonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.GreaterThan(col, val)
		}
	case Op_Fast_Ge:
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("NonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.GreaterThanOrEquals(col, val)
		}
	case Op_Fast_In:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("NonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.In(col, val)
		}
	case Op_Fast_NotIn:
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("NonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.NotIn(col, val)
		}
	case Op_Fast_Like,
		Op_Fast_NotLike,
		Op_Fast_Match,
		Op_Fast_NotMatch,
		Op_Fast_Includes,
		Op_Fast_Excludes:
		err = errors.New("NonNull: Unknown operator")
	default:
		err = errors.New("NonNull: Unknown operator")
	}

	return fn, err
}

func StringNonNull[T ~string](
	col *datacolimpl.DataColumnImpl[T],
	op FilterOp, v interface{}) (FilterGenFunc, error) {

	var err error
	var fn FilterGenFunc

	switch op {
	case Op_Fast_Like:
		if col.GetType() != Type_String {
			err = errors.New("StringNonNull: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.Like(col, val)
		}
	case Op_Fast_NotLike:
		if col.GetType() != Type_String {
			err = errors.New("StringNonNull: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.NotLike(col, val)
		}
	case Op_Fast_Match:
		if col.GetType() != Type_String {
			err = errors.New("StringNonNull: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.Match(col, val)
		}
	case Op_Fast_NotMatch:
		if col.GetType() != Type_String {
			err = errors.New("StringNonNull: Incorrect column type")
		}
		val, ok := col.Cast(v)
		if !ok {
			err = errors.New("StringNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.NotMatch(col, val)
		}
	case Op_Fast_Includes:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNonNull: Incorrect column type")
		}
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("StringNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.Includes(col, val)
		}
	case Op_Fast_Excludes:
		if col.GetType() != Type_Nullable_String {
			err = errors.New("StringNonNull: Incorrect column type")
		}
		val, ok := col.CastArray(v)
		if !ok {
			err = errors.New("StringNonNull: Incorrect parameter type")
		} else {
			fn = fastfilter.Excludes(col, val)
		}
	default:
		fn, err = NonNull(col, op, v)
	}

	return fn, err
}
