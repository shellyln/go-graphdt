package cast

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func GetCastFunc(from, to DataColumnType) CastFunc {
	if from&(Type_Mask_Element|Type_Flag_Nullable) == Type_Any && from&Type_Flag_Array != to&Type_Flag_Array {
		from |= Type_Flag_Array
	}

	// TODO: Probably needless.
	// if from&Type_Mask_Element != Type_Any {
	// 	switch to & (Type_Mask_Element | Type_Flag_Nullable) {
	// 	case Type_Any:
	// 		return GenerateCastFunc(from, to, ToAny)
	// 	case Type_Nullable_Any:
	// 		return GenerateCastFunc(from&^Type_Flag_Nullable, to&^Type_Flag_Nullable, ToNullableAny)
	// 	case Type_Blob,
	// 		Type_Nullable_Blob:
	// 		return GenerateCastFunc(from, to, ToBlob[[]byte])
	// 	}
	// }

	switch from & Type_Mask_Element {
	case Type_Any:
		return getDynamicCastFunc(from, to)
	case Type_I8:
		return getIntCastFunc[int8](from, to)
	case Type_I16:
		return getIntCastFunc[int16](from, to)
	case Type_I32:
		return getIntCastFunc[int32](from, to)
	case Type_I64:
		return getIntCastFunc[int64](from, to)
	case Type_Int:
		return getIntCastFunc[int](from, to)
	case Type_U8:
		return getUintCastFunc[uint8](from, to)
	case Type_U16:
		return getUintCastFunc[uint16](from, to)
	case Type_U32:
		return getUintCastFunc[uint32](from, to)
	case Type_U64:
		return getUintCastFunc[uint64](from, to)
	case Type_Uint:
		return getUintCastFunc[uint](from, to)
	case Type_UintPtr:
		return getUintCastFunc[uintptr](from, to)
	case Type_F32:
		return getFloatCastFunc[float32](from, to)
	case Type_F64:
		return getFloatCastFunc[float64](from, to)
	case Type_Complex64:
		return getComplexCastFunc[complex64](from, to)
	case Type_Complex128:
		return getComplexCastFunc[complex128](from, to)
	case Type_Bool:
		return getBoolCastFunc[bool](from, to)
	case Type_String:
		return getStringCastFunc[string](from, to)
	case Type_Blob:
		return getBlobCastFunc[[]byte](from, to)
	case Type_DateTime:
		return getTimeCastFunc(from, to)
	case Type_DateTimeRange:
		return getTimeRangeCastFunc(from, to)
	}
	return nil
}
