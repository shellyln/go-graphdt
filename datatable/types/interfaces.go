package types

type AnyDataColumn interface {
	// True if the data is owned by the column. Otherwise, it is borrowed.
	IsOwned() bool
	// Own the data. If the data is borrowed before the call, it copies.
	Own()
	// Borrow the data.
	BorrowAsAny() AnyDataColumn
	// Get the column data type.
	GetType() DataColumnType
	// If the column is non virtual, return the implementation instance. Otherwise, it returns itself.
	GetImpl() AnyDataColumn
	// Get the column length.
	Len() int
	// Get the column capacity.
	Cap() int
	// Resize the column.
	Resize(n int)
	// Resize the column. It is similar to Resize but it is for grow length.
	Grow(n int)
	// Check the value at the index is null.
	IsNull(i int) bool
	// Cast the value to the column type and return it as interface{}.
	CastAsAny(v interface{}) (interface{}, bool)
	// Cast the value to the array of column type and return it as interface{}.
	CastArrayAsAny(v interface{}) (interface{}, bool)
	// Get the value at the index as interface{}.
	GetAny(i int) interface{}
	// Set the interface{} value at the index.
	SetAny(i int, v interface{}) error
	// Set the interface{} value at the index without type casting.
	SetAnyNoCast(i int, v interface{}) error
	// Fill the column with the interface{} value.
	FillAny(s, e int, v interface{}) error
	// Get the raw values as interface{}.
	GetRawValuesAsAny() interface{}
	// Sort the column.
	Sort(less func(a, b int) bool)
	// Reverse the column.
	Reverse()
	// Copy the column.
	CopyAsAny() AnyDataColumn
	// Shallow copy the column with range.
	SliceAsAny(offset, limit int) AnyDataColumn
	// Iterate the column.
	For(iter EnumeratorFunc)
	// Iterate the column with filter.
	Filter(filter FilterFunc, iter EnumeratorFunc)
	// Append the values to the column.
	AppendAny(values ...interface{}) error
	// Append the values that have the another column.
	AppendAnyDataColumn(dc AnyDataColumn) error

	// Get the 2VL filter function for the column.
	GetFilter2VL(op FilterOp, v interface{}) (FilterGenFunc, error)
	// Get the 3VL filter function for the column.
	GetFilter3VL(op FilterOp, v interface{}) (FilterGenFunc, error)
	// Get the sort function for the column.
	GetSort(desc bool, nullsLast bool) SortFunc

	// Make the buffer for the column.
	MakeBufferAsAny(c int) interface{}
	// Copy indexed values to the buffer. `buf` will be sliced to the length of `index`.
	CopyBufferByIndex(buf *interface{}, index []int)
	// Copy indexed values from the `src` DataColumn to this DataColumn.
	// `dstRowMap` and `srcRowMap` are indices of rows to copy.
	// If `dstRowMap` is nil, it uses index number of `srcRowMap` as the destination index.
	FillByRowMap(dstRowMap []int, src AnyDataColumn, srcRowMap []int)

	// TODO:
	// RowsHaveEqualValue(is3VL bool, i, j int) bool
}
