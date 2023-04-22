package cast

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

func CanCast(from, to DataColumnType) bool {
	if from&Type_Flag_Array != to&Type_Flag_Array {
		return false
	}

	fromEl := from & Type_Mask_Element
	toEl := to & Type_Mask_Element

	if fromEl == toEl {
		return true
	}
	if fromEl == Type_Any || toEl == Type_Any {
		return true
	}

	switch toEl {
	case Type_String, Type_Blob:
		return true
	}

	switch fromEl {
	case Type_Bool,
		Type_I8,
		Type_I16,
		Type_I32,
		Type_I64,
		Type_Int,
		Type_U8,
		Type_U16,
		Type_U32,
		Type_U64,
		Type_Uint,
		Type_UintPtr,
		Type_F32,
		Type_F64,
		Type_Complex64,
		Type_Complex128:
		switch toEl {
		case Type_Bool,
			Type_I8,
			Type_I16,
			Type_I32,
			Type_I64,
			Type_Int,
			Type_U8,
			Type_U16,
			Type_U32,
			Type_U64,
			Type_Uint,
			Type_UintPtr,
			Type_F32,
			Type_F64,
			Type_Complex64,
			Type_Complex128:
			return true
		default:
			return false
		}
	case Type_String:
		return true
	case Type_Blob,
		Type_DateTime,
		Type_DateTimeRange:
		return false
	}
	return false
}

// For `==`, `!=`
func GetComparingPromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	if a&Type_Flag_Array != b&Type_Flag_Array {
		return Type_Invalid, Type_Invalid
	}

	var ret DataColumnType
	el := [2]DataColumnType{
		a & Type_Mask_Element,
		b & Type_Mask_Element,
	}

	if el[0] != el[1] {
		for i := 0; i < 2; i++ {
			switch el[i] {
			case Type_Any:
				// do nothing
			case Type_Bool,
				Type_I8,
				Type_I16,
				Type_I32,
				Type_I64,
				Type_Int,
				Type_U8,
				Type_U16,
				Type_U32:
				el[i] = Type_I64
			case Type_U64,
				Type_Uint,
				Type_UintPtr:
				el[i] = Type_U64
			case Type_F32,
				Type_F64:
				el[i] = Type_F64
			case Type_Complex64,
				Type_Complex128:
				el[i] = Type_Complex128
			case Type_String:
				// do nothing
			default:
				return Type_Invalid, Type_Invalid
			}
		}
		if el[0] > el[1] {
			ret = el[0]
		} else {
			ret = el[1]
		}
	} else {
		ret = el[0]
	}

	if ret == Type_Any {
		return Type_Invalid, Type_Invalid
	}

	if ((a | b) & Type_Flag_Nullable) != 0 {
		ret |= Type_Flag_Nullable
	}
	if a&Type_Flag_Array != 0 {
		ret |= Type_Flag_Array
	}
	return ret, ret
}

// For `<`, `<=`, `>`, `>=` and arithmetic operators
func GetArithmeticPromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	if a&Type_Flag_Array != b&Type_Flag_Array {
		return Type_Invalid, Type_Invalid
	}

	var ret DataColumnType
	el := [2]DataColumnType{
		a & Type_Mask_Element,
		b & Type_Mask_Element,
	}

	if el[0] != el[1] {
		for i := 0; i < 2; i++ {
			switch el[i] {
			case Type_Any:
				// do nothing
			case Type_Bool,
				Type_I8,
				Type_I16,
				Type_I32,
				Type_I64,
				Type_Int,
				Type_U8,
				Type_U16,
				Type_U32:
				el[i] = Type_I64
			case Type_U64,
				Type_Uint,
				Type_UintPtr:
				el[i] = Type_U64
			case Type_F32,
				Type_F64:
				el[i] = Type_F64
			case Type_Complex64,
				Type_Complex128:
				el[i] = Type_Complex128
			case Type_String:
				el[i] = Type_Invalid
			default:
				return Type_Invalid, Type_Invalid
			}
		}
		if el[0] > el[1] {
			ret = el[0]
		} else {
			ret = el[1]
		}
	} else {
		ret = el[0]
	}

	if ret == Type_Any {
		return Type_Invalid, Type_Invalid
	}

	if ((a | b) & Type_Flag_Nullable) != 0 {
		ret |= Type_Flag_Nullable
	}
	if a&Type_Flag_Array != 0 {
		ret |= Type_Flag_Array
	}
	return ret, ret
}

func GetConcatPromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	if a&Type_Flag_Array != b&Type_Flag_Array {
		return Type_Invalid, Type_Invalid
	}

	ret := Type_String

	if ((a | b) & Type_Flag_Nullable) != 0 {
		ret |= Type_Flag_Nullable
	}
	if a&Type_Flag_Array != 0 {
		ret |= Type_Flag_Array
	}
	return ret, ret
}

func GetInRangePromotedType(a, b DataColumnType) (DataColumnType, DataColumnType) {
	if a&Type_Flag_Array != b&Type_Flag_Array {
		return Type_Invalid, Type_Invalid
	}

	el := [2]DataColumnType{
		a & Type_Mask_Element,
		b & Type_Mask_Element,
	}

	if (el[0] | Type_Flag_ValueRange) != el[1] {
		return Type_Invalid, Type_Invalid
	}

	if ((a | b) & Type_Flag_Nullable) != 0 {
		el[0] |= Type_Flag_Nullable
		el[1] |= Type_Flag_Nullable
	}
	if a&Type_Flag_Array != 0 {
		el[0] |= Type_Flag_Array
		el[1] |= Type_Flag_Array
	}
	return el[0], el[1]
}
