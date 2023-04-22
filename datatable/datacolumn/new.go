package datacolumn

import (
	"time"

	"github.com/shellyln/go-graphdt/datatable/cast"
	impl "github.com/shellyln/go-graphdt/datatable/datacolimpl"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// NewDataColumn creates a new DataColumn.
func NewDataColumn(ty DataColumnType) *DataColumn {
	return NewDataColumnWithSize(0, impl.DataColumnImpl_DefaultSize, ty)
}

// NewDataColumnWithSize creates a new DataColumn with the specified size and capacity.
func NewDataColumnWithSize(l, c int, ty DataColumnType) *DataColumn {
	var col AnyDataColumn

	if ty&Type_Flag_Nullable == 0 {
		switch ty {
		case Type_I8:
			raw := impl.NewDataColumnImplWithSize[int8](l, c, ty)
			raw.SetCast(cast.ToInt[int8], cast.ArrayOf(cast.ToInt[int8]))
			col = raw
		case Type_I16:
			raw := impl.NewDataColumnImplWithSize[int16](l, c, ty)
			raw.SetCast(cast.ToInt[int16], cast.ArrayOf(cast.ToInt[int16]))
			col = raw
		case Type_I32:
			raw := impl.NewDataColumnImplWithSize[int32](l, c, ty)
			raw.SetCast(cast.ToInt[int32], cast.ArrayOf(cast.ToInt[int32]))
			col = raw
		case Type_I64:
			raw := impl.NewDataColumnImplWithSize[int64](l, c, ty)
			raw.SetCast(cast.ToInt[int64], cast.ArrayOf(cast.ToInt[int64]))
			col = raw
		case Type_Int:
			raw := impl.NewDataColumnImplWithSize[int](l, c, ty)
			raw.SetCast(cast.ToInt[int], cast.ArrayOf(cast.ToInt[int]))
			col = raw
		case Type_U8:
			raw := impl.NewDataColumnImplWithSize[uint8](l, c, ty)
			raw.SetCast(cast.ToUint[uint8], cast.ArrayOf(cast.ToUint[uint8]))
			col = raw
		case Type_U16:
			raw := impl.NewDataColumnImplWithSize[uint16](l, c, ty)
			raw.SetCast(cast.ToUint[uint16], cast.ArrayOf(cast.ToUint[uint16]))
			col = raw
		case Type_U32:
			raw := impl.NewDataColumnImplWithSize[uint32](l, c, ty)
			raw.SetCast(cast.ToUint[uint32], cast.ArrayOf(cast.ToUint[uint32]))
			col = raw
		case Type_U64:
			raw := impl.NewDataColumnImplWithSize[uint64](l, c, ty)
			raw.SetCast(cast.ToUint[uint64], cast.ArrayOf(cast.ToUint[uint64]))
			col = raw
		case Type_Uint:
			raw := impl.NewDataColumnImplWithSize[uint](l, c, ty)
			raw.SetCast(cast.ToUint[uint], cast.ArrayOf(cast.ToUint[uint]))
			col = raw
		case Type_UintPtr:
			raw := impl.NewDataColumnImplWithSize[uintptr](l, c, ty)
			raw.SetCast(cast.ToUint[uintptr], cast.ArrayOf(cast.ToUint[uintptr]))
			col = raw
		case Type_F32:
			raw := impl.NewDataColumnImplWithSize[float32](l, c, ty)
			raw.SetCast(cast.ToFloat[float32], cast.ArrayOf(cast.ToFloat[float32]))
			col = raw
		case Type_F64:
			raw := impl.NewDataColumnImplWithSize[float64](l, c, ty)
			raw.SetCast(cast.ToFloat[float64], cast.ArrayOf(cast.ToFloat[float64]))
			col = raw
		case Type_Complex64:
			raw := impl.NewDataColumnImplWithSize[complex64](l, c, ty)
			raw.SetCast(cast.ToComplex[complex64], cast.ArrayOf(cast.ToComplex[complex64]))
			col = raw
		case Type_Complex128:
			raw := impl.NewDataColumnImplWithSize[complex128](l, c, ty)
			raw.SetCast(cast.ToComplex[complex128], cast.ArrayOf(cast.ToComplex[complex128]))
			col = raw
		case Type_Bool:
			raw := impl.NewDataColumnImplWithSize[bool](l, c, ty)
			raw.SetCast(cast.ToBool[bool], cast.ArrayOf(cast.ToBool[bool]))
			col = raw
		case Type_String:
			raw := impl.NewDataColumnImplWithSize[string](l, c, ty)
			raw.SetCast(cast.ToString[string], cast.ArrayOf(cast.ToString[string]))
			col = raw
		case Type_Blob:
			raw := impl.NewDataColumnImplWithSize[[]byte](l, c, ty)
			raw.SetCast(cast.ToBlob[[]byte], cast.ArrayOf(cast.ToBlob[[]byte]))
			col = raw
		case Type_DateTime:
			raw := impl.NewDataColumnImplWithSize[time.Time](l, c, ty)
			raw.SetCast(cast.ToTime, cast.ArrayOf(cast.ToTime))
			col = raw
		case Type_DateTimeRange:
			raw := impl.NewDataColumnImplWithSize[TimeRange](l, c, ty)
			raw.SetCast(cast.ToTimeRange, cast.ArrayOf(cast.ToTimeRange))
			col = raw

		case Type_Any:
			fallthrough
		default:
			raw := impl.NewDataColumnImplWithSize[interface{}](l, c, ty)
			raw.SetCast(cast.ToAny, cast.ArrayOf(cast.ToAny))
			col = raw
		}
	} else {
		switch ty {
		case Type_Nullable_I8:
			raw := impl.NewDataColumnImplWithSize[Nullable[int8]](l, c, ty)
			raw.SetCheckNull(checkNull[int8])
			raw.SetCast(cast.ToNullableInt[int8], cast.ArrayOf(cast.ToNullableInt[int8]))
			col = raw
		case Type_Nullable_I16:
			raw := impl.NewDataColumnImplWithSize[Nullable[int16]](l, c, ty)
			raw.SetCheckNull(checkNull[int16])
			raw.SetCast(cast.ToNullableInt[int16], cast.ArrayOf(cast.ToNullableInt[int16]))
			col = raw
		case Type_Nullable_I32:
			raw := impl.NewDataColumnImplWithSize[Nullable[int32]](l, c, ty)
			raw.SetCheckNull(checkNull[int32])
			raw.SetCast(cast.ToNullableInt[int32], cast.ArrayOf(cast.ToNullableInt[int32]))
			col = raw
		case Type_Nullable_I64:
			raw := impl.NewDataColumnImplWithSize[Nullable[int64]](l, c, ty)
			raw.SetCheckNull(checkNull[int64])
			raw.SetCast(cast.ToNullableInt[int64], cast.ArrayOf(cast.ToNullableInt[int64]))
			col = raw
		case Type_Nullable_Int:
			raw := impl.NewDataColumnImplWithSize[Nullable[int]](l, c, ty)
			raw.SetCheckNull(checkNull[int])
			raw.SetCast(cast.ToNullableInt[int], cast.ArrayOf(cast.ToNullableInt[int]))
			col = raw
		case Type_Nullable_U8:
			raw := impl.NewDataColumnImplWithSize[Nullable[uint8]](l, c, ty)
			raw.SetCheckNull(checkNull[uint8])
			raw.SetCast(cast.ToNullableUint[uint8], cast.ArrayOf(cast.ToNullableUint[uint8]))
			col = raw
		case Type_Nullable_U16:
			raw := impl.NewDataColumnImplWithSize[Nullable[uint16]](l, c, ty)
			raw.SetCheckNull(checkNull[uint16])
			raw.SetCast(cast.ToNullableUint[uint16], cast.ArrayOf(cast.ToNullableUint[uint16]))
			col = raw
		case Type_Nullable_U32:
			raw := impl.NewDataColumnImplWithSize[Nullable[uint32]](l, c, ty)
			raw.SetCheckNull(checkNull[uint32])
			raw.SetCast(cast.ToNullableUint[uint32], cast.ArrayOf(cast.ToNullableUint[uint32]))
			col = raw
		case Type_Nullable_U64:
			raw := impl.NewDataColumnImplWithSize[Nullable[uint64]](l, c, ty)
			raw.SetCheckNull(checkNull[uint64])
			raw.SetCast(cast.ToNullableUint[uint64], cast.ArrayOf(cast.ToNullableUint[uint64]))
			col = raw
		case Type_Nullable_Uint:
			raw := impl.NewDataColumnImplWithSize[Nullable[uint]](l, c, ty)
			raw.SetCheckNull(checkNull[uint])
			raw.SetCast(cast.ToNullableUint[uint], cast.ArrayOf(cast.ToNullableUint[uint]))
			col = raw
		case Type_Nullable_UintPtr:
			raw := impl.NewDataColumnImplWithSize[Nullable[uintptr]](l, c, ty)
			raw.SetCheckNull(checkNull[uintptr])
			raw.SetCast(cast.ToNullableUint[uintptr], cast.ArrayOf(cast.ToNullableUint[uintptr]))
			col = raw
		case Type_Nullable_F32:
			raw := impl.NewDataColumnImplWithSize[Nullable[float32]](l, c, ty)
			raw.SetCheckNull(checkNull[float32])
			raw.SetCast(cast.ToNullableFloat[float32], cast.ArrayOf(cast.ToNullableFloat[float32]))
			col = raw
		case Type_Nullable_F64:
			raw := impl.NewDataColumnImplWithSize[Nullable[float64]](l, c, ty)
			raw.SetCheckNull(checkNull[float64])
			raw.SetCast(cast.ToNullableFloat[float64], cast.ArrayOf(cast.ToNullableFloat[float64]))
			col = raw
		case Type_Nullable_Complex64:
			raw := impl.NewDataColumnImplWithSize[Nullable[complex64]](l, c, ty)
			raw.SetCheckNull(checkNull[complex64])
			raw.SetCast(cast.ToNullableComplex[complex64], cast.ArrayOf(cast.ToNullableComplex[complex64]))
			col = raw
		case Type_Nullable_Complex128:
			raw := impl.NewDataColumnImplWithSize[Nullable[complex128]](l, c, ty)
			raw.SetCheckNull(checkNull[complex128])
			raw.SetCast(cast.ToNullableComplex[complex128], cast.ArrayOf(cast.ToNullableComplex[complex128]))
			col = raw
		case Type_Nullable_Bool:
			raw := impl.NewDataColumnImplWithSize[Nullable[bool]](l, c, ty)
			raw.SetCheckNull(checkNull[bool])
			raw.SetCast(cast.ToNullableBool[bool], cast.ArrayOf(cast.ToNullableBool[bool]))
			col = raw
		case Type_Nullable_String:
			raw := impl.NewDataColumnImplWithSize[Nullable[string]](l, c, ty)
			raw.SetCheckNull(checkNull[string])
			raw.SetCast(cast.ToNullableString[string], cast.ArrayOf(cast.ToNullableString[string]))
			col = raw
		case Type_Nullable_Blob:
			raw := impl.NewDataColumnImplWithSize[Nullable[[]byte]](l, c, ty)
			raw.SetCheckNull(checkNull[[]byte])
			raw.SetCast(cast.ToNullableBlob[[]byte], cast.ArrayOf(cast.ToNullableBlob[[]byte]))
			col = raw
		case Type_Nullable_DateTime:
			raw := impl.NewDataColumnImplWithSize[Nullable[time.Time]](l, c, ty)
			raw.SetCheckNull(checkNull[time.Time])
			raw.SetCast(cast.ToNullableTime, cast.ArrayOf(cast.ToNullableTime))
			col = raw
		case Type_Nullable_DateTimeRange:
			raw := impl.NewDataColumnImplWithSize[Nullable[TimeRange]](l, c, ty)
			raw.SetCheckNull(checkNull[TimeRange])
			raw.SetCast(cast.ToNullableTimeRange, cast.ArrayOf(cast.ToNullableTimeRange))
			col = raw

		case Type_Nullable_Any:
			fallthrough
		default:
			raw := impl.NewDataColumnImplWithSize[Nullable[interface{}]](l, c, ty)
			raw.SetCheckNull(checkNull[interface{}])
			raw.SetCast(cast.ToNullableAny, cast.ArrayOf(cast.ToNullableAny))
			col = raw
		}
	}

	ret := DataColumn{
		typ: ty,
		col: col,
	}
	return &ret
}

func checkNull[T any](v Nullable[T]) bool {
	return !v.Valid
}
