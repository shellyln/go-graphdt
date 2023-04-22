package datacolumn

import (
	"errors"
	"time"

	impl "github.com/shellyln/go-graphdt/datatable/datacolimpl"
	disp "github.com/shellyln/go-graphdt/datatable/datacolumn/filterdisp"
	"github.com/shellyln/go-graphdt/datatable/filter/cmpr"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// interface AnyDataColumn
func (p *DataColumn) GetFilter2VL(op FilterOp, v interface{}) (FilterGenFunc, error) {
	var err error
	var fn FilterGenFunc

	switch p.typ {
	case Type_I8:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[int8]), op, v)

	case Type_I16:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[int16]), op, v)

	case Type_I32:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[int32]), op, v)

	case Type_I64:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[int64]), op, v)

	case Type_Int:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[int]), op, v)

	case Type_U8:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[uint8]), op, v)

	case Type_U16:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[uint16]), op, v)

	case Type_U32:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[uint32]), op, v)

	case Type_U64:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[uint64]), op, v)

	case Type_Uint:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[uint]), op, v)

	case Type_UintPtr:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[uintptr]), op, v)

	case Type_F32:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[float32]), op, v)

	case Type_F64:
		fn, err = disp.NonNull(
			p.col.(*impl.DataColumnImpl[float64]), op, v)

	case Type_Complex64:
		fn, err = disp.ComparableNonNull(
			p.col.(*impl.DataColumnImpl[complex64]),
			cmpr.ComplexLt[complex64], cmpr.ComplexLe[complex64], op, v)

	case Type_Complex128:
		fn, err = disp.ComparableNonNull(
			p.col.(*impl.DataColumnImpl[complex128]),
			cmpr.ComplexLt[complex128], cmpr.ComplexLe[complex128], op, v)

	case Type_Bool:
		fn, err = disp.ComparableNonNull(
			p.col.(*impl.DataColumnImpl[bool]),
			cmpr.BoolLt[bool], cmpr.BoolLe[bool], op, v)

	case Type_String:
		fn, err = disp.StringNonNull(
			p.col.(*impl.DataColumnImpl[string]), op, v)

	case Type_Blob:
		fn, err = disp.AnyNonNull(
			p.col.(*impl.DataColumnImpl[[]byte]),
			cmpr.BlobEq[[]byte], cmpr.BlobLt[[]byte], cmpr.BlobLe[[]byte], op, v)

	case Type_DateTime:
		fn, err = disp.AnyNonNull(
			p.col.(*impl.DataColumnImpl[time.Time]),
			cmpr.TimeEq, cmpr.TimeLt, cmpr.TimeLe, op, v)

	case Type_DateTimeRange:
		fn, err = disp.AnyNonNull(
			p.col.(*impl.DataColumnImpl[TimeRange]),
			cmpr.TimeRangeEq, cmpr.TimeRangeLt, cmpr.TimeRangeLe, op, v)

	case Type_Any:
		// TODO: not impl
		err = errors.New("DataTable.Filter: Unknown operator")

	case Type_Nullable_I8:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[int8]]), op, v)

	case Type_Nullable_I16:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[int16]]), op, v)

	case Type_Nullable_I32:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[int32]]), op, v)

	case Type_Nullable_I64:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[int64]]), op, v)

	case Type_Nullable_Int:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[int]]), op, v)

	case Type_Nullable_U8:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[uint8]]), op, v)

	case Type_Nullable_U16:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[uint16]]), op, v)

	case Type_Nullable_U32:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[uint32]]), op, v)

	case Type_Nullable_U64:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[uint64]]), op, v)

	case Type_Nullable_Uint:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[uint]]), op, v)

	case Type_Nullable_UintPtr:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[uintptr]]), op, v)

	case Type_Nullable_F32:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[float32]]), op, v)

	case Type_Nullable_F64:
		fn, err = disp.Nullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[float64]]), op, v)

	case Type_Nullable_Complex64:
		fn, err = disp.ComparableNullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[complex64]]),
			cmpr.ComplexLt[complex64], cmpr.ComplexLe[complex64], op, v)

	case Type_Nullable_Complex128:
		fn, err = disp.ComparableNullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[complex128]]),
			cmpr.ComplexLt[complex128], cmpr.ComplexLe[complex128], op, v)

	case Type_Nullable_Bool:
		fn, err = disp.ComparableNullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[bool]]),
			cmpr.BoolLt[bool], cmpr.BoolLe[bool], op, v)

	case Type_Nullable_String:
		fn, err = disp.StringNullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[string]]), op, v)

	case Type_Nullable_Blob:
		fn, err = disp.AnyNullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[[]byte]]),
			cmpr.BlobEq[[]byte], cmpr.BlobLt[[]byte], cmpr.BlobLe[[]byte], op, v)

	case Type_Nullable_DateTime:
		fn, err = disp.AnyNullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[time.Time]]),
			cmpr.TimeEq, cmpr.TimeLt, cmpr.TimeLe, op, v)

	case Type_Nullable_DateTimeRange:
		fn, err = disp.AnyNullable2VL(
			p.col.(*impl.DataColumnImpl[Nullable[TimeRange]]),
			cmpr.TimeRangeEq, cmpr.TimeRangeLt, cmpr.TimeRangeLe, op, v)

	case Type_Nullable_Any:
		// TODO: not impl
		err = errors.New("DataTable.Filter: Unknown operator")

	default:
		err = errors.New("DataTable.Filter: Unknown operator")
	}

	return fn, err
}
