package cast

import (
	"reflect"
	"time"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

var (
	reflectType_Time                = reflect.TypeOf(time.Time{})
	reflectType_TimeRange           = reflect.TypeOf(TimeRange{})
	reflectType_Nullable_Any        = reflect.TypeOf(Nullable[interface{}]{})
	reflectType_Nullable_Int8       = reflect.TypeOf(Nullable[int8]{})
	reflectType_Nullable_Int16      = reflect.TypeOf(Nullable[int16]{})
	reflectType_Nullable_Int32      = reflect.TypeOf(Nullable[int32]{})
	reflectType_Nullable_Int64      = reflect.TypeOf(Nullable[int64]{})
	reflectType_Nullable_Int        = reflect.TypeOf(Nullable[int]{})
	reflectType_Nullable_Uint8      = reflect.TypeOf(Nullable[uint8]{})
	reflectType_Nullable_Uint16     = reflect.TypeOf(Nullable[uint16]{})
	reflectType_Nullable_Uint32     = reflect.TypeOf(Nullable[uint32]{})
	reflectType_Nullable_Uint64     = reflect.TypeOf(Nullable[uint64]{})
	reflectType_Nullable_Uint       = reflect.TypeOf(Nullable[uint]{})
	reflectType_Nullable_UintPtr    = reflect.TypeOf(Nullable[uintptr]{})
	reflectType_Nullable_Float32    = reflect.TypeOf(Nullable[float32]{})
	reflectType_Nullable_Float64    = reflect.TypeOf(Nullable[float64]{})
	reflectType_Nullable_Complex64  = reflect.TypeOf(Nullable[complex64]{})
	reflectType_Nullable_Complex128 = reflect.TypeOf(Nullable[complex128]{})
	reflectType_Nullable_Bool       = reflect.TypeOf(Nullable[int]{})
	reflectType_Nullable_String     = reflect.TypeOf(Nullable[string]{})
	reflectType_Nullable_Blob       = reflect.TypeOf(Nullable[[]byte]{})
	reflectType_Nullable_Time       = reflect.TypeOf(Nullable[time.Time]{})
	reflectType_Nullable_TimeRange  = reflect.TypeOf(Nullable[TimeRange]{})
)

func GetType(rt reflect.Type) DataColumnType {
	switch rt.Kind() {
	case reflect.Bool:
		return Type_Bool

	case reflect.Int8:
		return Type_I8
	case reflect.Int16:
		return Type_I16
	case reflect.Int32:
		return Type_I32
	case reflect.Int64:
		return Type_I64
	case reflect.Int:
		return Type_Int

	case reflect.Uint8:
		return Type_U8
	case reflect.Uint16:
		return Type_U16
	case reflect.Uint32:
		return Type_U32
	case reflect.Uint64:
		return Type_U64
	case reflect.Uint:
		return Type_Uint
	case reflect.Uintptr:
		return Type_UintPtr

	case reflect.Float32:
		return Type_F32
	case reflect.Float64:
		return Type_F64

	case reflect.Complex64:
		return Type_Complex64
	case reflect.Complex128:
		return Type_Complex128

	case reflect.String:
		return Type_String

	case reflect.Array,
		reflect.Slice:
		elRt := rt.Elem()
		switch elRt.Kind() {
		case reflect.Uint8:
			return Type_Blob
		default:
			return Type_Flag_Array | GetType(elRt)
		}

	case reflect.Struct:
		switch rt {
		case reflectType_Time:
			return Type_Nullable_DateTime
		case reflectType_TimeRange:
			return Type_Nullable_DateTimeRange

		case reflectType_Nullable_Bool:
			return Type_Nullable_Bool

		case reflectType_Nullable_Int8:
			return Type_Nullable_I8
		case reflectType_Nullable_Int16:
			return Type_Nullable_I16
		case reflectType_Nullable_Int32:
			return Type_Nullable_I32
		case reflectType_Nullable_Int64:
			return Type_Nullable_I64
		case reflectType_Nullable_Int:
			return Type_Nullable_Int

		case reflectType_Nullable_Uint8:
			return Type_Nullable_U8
		case reflectType_Nullable_Uint16:
			return Type_Nullable_U16
		case reflectType_Nullable_Uint32:
			return Type_Nullable_U32
		case reflectType_Nullable_Uint64:
			return Type_Nullable_U64
		case reflectType_Nullable_Uint:
			return Type_Nullable_Uint
		case reflectType_Nullable_UintPtr:
			return Type_Nullable_UintPtr

		case reflectType_Nullable_Float32:
			return Type_Nullable_F32
		case reflectType_Nullable_Float64:
			return Type_Nullable_F64

		case reflectType_Nullable_Complex64:
			return Type_Nullable_Complex64
		case reflectType_Nullable_Complex128:
			return Type_Nullable_Complex128

		case reflectType_Nullable_String:
			return Type_Nullable_String
		case reflectType_Nullable_Blob:
			return Type_Nullable_Blob

		case reflectType_Nullable_Time:
			return Type_Nullable_DateTime
		case reflectType_Nullable_TimeRange:
			return Type_Nullable_DateTimeRange

		case reflectType_Nullable_Any:
			return Type_Nullable_Any
		}
	}
	return Type_Any
}
