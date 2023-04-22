package types

import "time"

// DataColumnType represents the type of data column.
type DataColumnType int

const (
	Type_Mask_Element    DataColumnType = 0x00ff // Mask for element type. e.g. (Type_I64|Type_Flag_Nullable|Type_Flag_Array) & Type_Mask_Element == Type_I64
	Type_Flag_ValueRange DataColumnType = 0x0080 // Flag that indicates the type is `ValueRange[T]`. It is part of element type.
	Type_Flag_Nullable   DataColumnType = 0x0100 // Flag that indicates the type is `Nullable[T]`.
	Type_Flag_Array      DataColumnType = 0x0200 // Flag that indicates the type is `[]T`.

	// NOTE: (Type_I64|Type_Flag_ValueRange|Type_Flag_Nullable|Type_Flag_Array) indicates `[]Nullable[ValueRange[int64]]`.
)

const (
	Type_Invalid DataColumnType = iota // Invalid element type. It is not part of element type.

	Type_Start_Types // Start of element types. It is not part of element type.

	Type_Any        DataColumnType = iota - 1 // Indicates `interface{}`.
	Type_I8                                   // Indicates `int8`.
	Type_I16                                  // Indicates `int16`.
	Type_I32                                  // Indicates `int32`.
	Type_I64                                  // Indicates `int64`.
	Type_Int                                  // Indicates `int`.
	Type_U8                                   // Indicates `uint8`.
	Type_U16                                  // Indicates `uint16`.
	Type_U32                                  // Indicates `uint32`.
	Type_U64                                  // Indicates `uint64`.
	Type_Uint                                 // Indicates `uint`.
	Type_UintPtr                              // Indicates `uintptr`.
	Type_F32                                  // Indicates `float32`.
	Type_F64                                  // Indicates `float64`.
	Type_Complex64                            // Indicates `complex64`.
	Type_Complex128                           // Indicates `complex128`.
	Type_Bool                                 // Indicates `bool`.
	Type_String                               // Indicates `string`.
	Type_Blob                                 // Indicates `[]byte`.
	Type_DateTime                             // Indicates `time.Time`.
)

const (
	Type_DateTimeRange DataColumnType = Type_Flag_ValueRange + Type_DateTime + iota // Indicates `ValueRange[time.Time]`.
)

const (
	Type_Nullable_Any        DataColumnType = Type_Flag_Nullable + Type_Any + iota // Indicates `Nullable[interface{}]`.
	Type_Nullable_I8                                                               // Indicates `Nullable[int8]`.
	Type_Nullable_I16                                                              // Indicates `Nullable[int16]`.
	Type_Nullable_I32                                                              // Indicates `Nullable[int32]`.
	Type_Nullable_I64                                                              // Indicates `Nullable[int64]`.
	Type_Nullable_Int                                                              // Indicates `Nullable[int]`.
	Type_Nullable_U8                                                               // Indicates `Nullable[uint8]`.
	Type_Nullable_U16                                                              // Indicates `Nullable[uint16]`.
	Type_Nullable_U32                                                              // Indicates `Nullable[uint32]`.
	Type_Nullable_U64                                                              // Indicates `Nullable[uint64]`.
	Type_Nullable_Uint                                                             // Indicates `Nullable[uint]`.
	Type_Nullable_UintPtr                                                          // Indicates `Nullable[uintptr]`.
	Type_Nullable_F32                                                              // Indicates `Nullable[float32]`.
	Type_Nullable_F64                                                              // Indicates `Nullable[float64]`.
	Type_Nullable_Complex64                                                        // Indicates `Nullable[complex64]`.
	Type_Nullable_Complex128                                                       // Indicates `Nullable[complex128]`.
	Type_Nullable_Bool                                                             // Indicates `Nullable[bool]`.
	Type_Nullable_String                                                           // Indicates `Nullable[string]`.
	Type_Nullable_Blob                                                             // Indicates `Nullable[[]byte]`.
	Type_Nullable_DateTime                                                         // Indicates `Nullable[time.Time]`.
)

const (
	Type_Nullable_DateTimeRange DataColumnType = Type_Flag_Nullable + Type_DateTimeRange + iota // Indicates `Nullable[ValueRange[time.Time]]`.
)

// Sort order information
type SortInfo struct {
	Col       int
	Desc      bool
	NullsLast bool
}

// Filter operator
type FilterOp int

const (
	Op_Noop FilterOp = iota + 1 // no operation
	Op_Not                      // unary operator not
	Op_And                      // binary operator and
	Op_Or                       // binary operator or

	Op_Eq        // binary operator =
	Op_NotEq     // binary operator !=
	Op_IsNull    // unary operator isnull
	Op_IsNotNull // unary operator isnotnull
	Op_Lt        // binary operator <
	Op_Le        // binary operator <=
	Op_Gt        // binary operator >
	Op_Ge        // binary operator >=
	Op_Like      // binary operator like
	Op_NotLike   // binary operator not like
	Op_Match     // binary operator match
	Op_NotMatch  // binary operator not match
	Op_In        // binary operator in
	Op_NotIn     // binary operator not in
	Op_Includes  // binary operator includes
	Op_Excludes  // binary operator excludes

	// TODO: Match, NotMatch // use regexp
	// TODO: InRange, NotInRange

	Op_Fast_Eq        // binary operator =
	Op_Fast_NotEq     // binary operator !=
	Op_Fast_IsNull    // unary operator isnull
	Op_Fast_IsNotNull // unary operator isnotnull
	Op_Fast_Lt        // binary operator <
	Op_Fast_Le        // binary operator <=
	Op_Fast_Gt        // binary operator >
	Op_Fast_Ge        // binary operator >=
	Op_Fast_Like      // binary operator like
	Op_Fast_NotLike   // binary operator not like
	Op_Fast_Match     // binary operator match
	Op_Fast_NotMatch  // binary operator not match
	Op_Fast_In        // binary operator in
	Op_Fast_NotIn     // binary operator not in
	Op_Fast_Includes  // binary operator includes
	Op_Fast_Excludes  // binary operator excludes

	Op_LoadImmediate     // load immediate value
	Op_LoadConst         // load constant value
	Op_LoadVar           // load variable value
	Op_LoadCol           // load column value
	Op_LoadRowId         // load rowid value
	Op_LoadPreviousCol   // load previous column value
	Op_LoadPreviousRowId // load previous rowid value
	Op_LoadColAsList     // load column values as list
	Op_LoadRowIdsAsList  // load rowid values as list

	Op_Call // call function
)

const (
	Op_FastOp_Magic = Op_Fast_Eq - Op_Eq // magic number for fast operators
)

// Filter information
type FilterInfo struct {
	Op    FilterOp    // operator
	Col   int         // column parameter
	Param interface{} // other parameter
	NArgs int         // number of arguments
}

// Formula information
type FormulaInfo = FilterInfo

// 3-valued logic (3VL) boolean type
type Bool3VL int

const (
	False3VL   Bool3VL = 0 // 3VL False
	Unknown3VL Bool3VL = 1 // 3VL Unknown
	True3VL    Bool3VL = 3 // 3VL True
)

// Select information
type SelectInfo struct {
	Col     int           // simple result column
	Formula []FormulaInfo // complex result column
	As      string        // column alias name
}

// TODO: constraints.Ordered
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

type Int interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type Uint interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type Float interface {
	~float32 | ~float64
}

type Complex interface {
	~complex64 | ~complex128
}

type SortFunc = func(a, b int) Bool3VL
type FilterFunc = func(i int) bool
type Filter3VLFunc = func(i int) Bool3VL
type FilterGenFunc = func(filterStack *[]FilterStackLeaf) error
type EnumeratorFunc = func(i int)
type CastFunc = func(v interface{}) (interface{}, bool)

// Filter stack item value
type FilterStackLeaf struct {
	Type    DataColumnType
	Fn      Filter3VLFunc
	IsCol   bool
	IsConst bool
}

// Nullable value
type Nullable[T any] struct {
	Value T
	Valid bool
}

// Nullable value is valid (is not null)
func (s Nullable[T]) IsValid() bool {
	return s.Valid
}

// Construct a new nullable value
func NewNullable[T any](v T) Nullable[T] {
	return Nullable[T]{
		Value: v,
		Valid: true,
	}
}

// Construct a new nullable value with null value
func NewNullableAsNull[T any]() Nullable[T] {
	return Nullable[T]{}
}

// Value range
type ValueRange[T any] struct {
	Start T
	End   T
}

// Time range
type TimeRange ValueRange[time.Time]
