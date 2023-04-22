package cast

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func getDynamicCastFunc(from, to DataColumnType) CastFunc {
	if to&Type_Flag_Array == 0 {
		if to&Type_Flag_Nullable == 0 {
			switch to & Type_Mask_Element {
			case Type_Any:
				return ToAny
			case Type_I8:
				return CastFuncOf(ToInt[int8])
			case Type_I16:
				return CastFuncOf(ToInt[int16])
			case Type_I32:
				return CastFuncOf(ToInt[int32])
			case Type_I64:
				return CastFuncOf(ToInt[int64])
			case Type_Int:
				return CastFuncOf(ToInt[int])
			case Type_U8:
				return CastFuncOf(ToUint[uint8])
			case Type_U16:
				return CastFuncOf(ToUint[uint16])
			case Type_U32:
				return CastFuncOf(ToUint[uint32])
			case Type_U64:
				return CastFuncOf(ToUint[uint64])
			case Type_Uint:
				return CastFuncOf(ToUint[uint])
			case Type_UintPtr:
				return CastFuncOf(ToUint[uintptr])
			case Type_F32:
				return CastFuncOf(ToFloat[float32])
			case Type_F64:
				return CastFuncOf(ToFloat[float64])
			case Type_Complex64:
				return CastFuncOf(ToComplex[complex64])
			case Type_Complex128:
				return CastFuncOf(ToComplex[complex128])
			case Type_Bool:
				return CastFuncOf(ToBool[bool])
			case Type_String:
				return CastFuncOf(ToString[string])
			case Type_Blob:
				return CastFuncOf(ToBlob[[]byte])
			case Type_DateTime:
				return CastFuncOf(ToTime)
			case Type_DateTimeRange:
				return CastFuncOf(ToTimeRange)
			}
		} else {
			switch to & Type_Mask_Element {
			case Type_Any:
				return CastFuncOf(ToNullableAny)
			case Type_I8:
				return CastFuncOf(ToNullableInt[int8])
			case Type_I16:
				return CastFuncOf(ToNullableInt[int16])
			case Type_I32:
				return CastFuncOf(ToNullableInt[int32])
			case Type_I64:
				return CastFuncOf(ToNullableInt[int64])
			case Type_Int:
				return CastFuncOf(ToNullableInt[int])
			case Type_U8:
				return CastFuncOf(ToNullableUint[uint8])
			case Type_U16:
				return CastFuncOf(ToNullableUint[uint16])
			case Type_U32:
				return CastFuncOf(ToNullableUint[uint32])
			case Type_U64:
				return CastFuncOf(ToNullableUint[uint64])
			case Type_Uint:
				return CastFuncOf(ToNullableUint[uint])
			case Type_UintPtr:
				return CastFuncOf(ToNullableUint[uintptr])
			case Type_F32:
				return CastFuncOf(ToNullableFloat[float32])
			case Type_F64:
				return CastFuncOf(ToNullableFloat[float64])
			case Type_Complex64:
				return CastFuncOf(ToNullableComplex[complex64])
			case Type_Complex128:
				return CastFuncOf(ToNullableComplex[complex128])
			case Type_Bool:
				return CastFuncOf(ToNullableBool[bool])
			case Type_String:
				return CastFuncOf(ToNullableString[string])
			case Type_Blob:
				return CastFuncOf(ToNullableBlob[[]byte])
			case Type_DateTime:
				return CastFuncOf(ToNullableTime)
			case Type_DateTimeRange:
				return CastFuncOf(ToNullableTimeRange)
			}
		}
	} else {
		if to&Type_Flag_Nullable == 0 {
			switch to & Type_Mask_Element {
			case Type_Any:
				return CastFuncOf(ToAny)
			case Type_I8:
				return CastFuncOf(ArrayOf(ToInt[int8]))
			case Type_I16:
				return CastFuncOf(ArrayOf(ToInt[int16]))
			case Type_I32:
				return CastFuncOf(ArrayOf(ToInt[int32]))
			case Type_I64:
				return CastFuncOf(ArrayOf(ToInt[int64]))
			case Type_Int:
				return CastFuncOf(ArrayOf(ToInt[int]))
			case Type_U8:
				return CastFuncOf(ArrayOf(ToUint[uint8]))
			case Type_U16:
				return CastFuncOf(ArrayOf(ToUint[uint16]))
			case Type_U32:
				return CastFuncOf(ArrayOf(ToUint[uint32]))
			case Type_U64:
				return CastFuncOf(ArrayOf(ToUint[uint64]))
			case Type_Uint:
				return CastFuncOf(ArrayOf(ToUint[uint]))
			case Type_UintPtr:
				return CastFuncOf(ArrayOf(ToUint[uintptr]))
			case Type_F32:
				return CastFuncOf(ArrayOf(ToFloat[float32]))
			case Type_F64:
				return CastFuncOf(ArrayOf(ToFloat[float64]))
			case Type_Complex64:
				return CastFuncOf(ArrayOf(ToComplex[complex64]))
			case Type_Complex128:
				return CastFuncOf(ArrayOf(ToComplex[complex128]))
			case Type_Bool:
				return CastFuncOf(ArrayOf(ToBool[bool]))
			case Type_String:
				return CastFuncOf(ArrayOf(ToString[string]))
			case Type_Blob:
				return CastFuncOf(ArrayOf(ToBlob[[]byte]))
			case Type_DateTime:
				return CastFuncOf(ArrayOf(ToTime))
			case Type_DateTimeRange:
				return CastFuncOf(ArrayOf(ToTimeRange))
			}
		} else {
			switch to & Type_Mask_Element {
			case Type_Any:
				return CastFuncOf(ToNullableAny)
			case Type_I8:
				return CastFuncOf(ArrayOf(ToNullableInt[int8]))
			case Type_I16:
				return CastFuncOf(ArrayOf(ToNullableInt[int16]))
			case Type_I32:
				return CastFuncOf(ArrayOf(ToNullableInt[int32]))
			case Type_I64:
				return CastFuncOf(ArrayOf(ToNullableInt[int64]))
			case Type_Int:
				return CastFuncOf(ArrayOf(ToNullableInt[int]))
			case Type_U8:
				return CastFuncOf(ArrayOf(ToNullableUint[uint8]))
			case Type_U16:
				return CastFuncOf(ArrayOf(ToNullableUint[uint16]))
			case Type_U32:
				return CastFuncOf(ArrayOf(ToNullableUint[uint32]))
			case Type_U64:
				return CastFuncOf(ArrayOf(ToNullableUint[uint64]))
			case Type_Uint:
				return CastFuncOf(ArrayOf(ToNullableUint[uint]))
			case Type_UintPtr:
				return CastFuncOf(ArrayOf(ToNullableUint[uintptr]))
			case Type_F32:
				return CastFuncOf(ArrayOf(ToNullableFloat[float32]))
			case Type_F64:
				return CastFuncOf(ArrayOf(ToNullableFloat[float64]))
			case Type_Complex64:
				return CastFuncOf(ArrayOf(ToNullableComplex[complex64]))
			case Type_Complex128:
				return CastFuncOf(ArrayOf(ToNullableComplex[complex128]))
			case Type_Bool:
				return CastFuncOf(ArrayOf(ToNullableBool[bool]))
			case Type_String:
				return CastFuncOf(ArrayOf(ToNullableString[string]))
			case Type_Blob:
				return CastFuncOf(ArrayOf(ToNullableBlob[[]byte]))
			case Type_DateTime:
				return CastFuncOf(ArrayOf(ToNullableTime))
			case Type_DateTimeRange:
				return CastFuncOf(ArrayOf(ToNullableTimeRange))
			}
		}
	}
	return Fail
}
