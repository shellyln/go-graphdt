package datacolumn

import (
	"fmt"

	. "github.com/shellyln/go-graphdt/datatable/types"
)

// interface AnyDataColumn
func (p *DataColumn) BorrowAsAny() AnyDataColumn {
	ret := DataColumn{
		typ: p.typ,
		col: p.col.BorrowAsAny(),
	}
	return &ret
}

// interface AnyDataColumn
func (p *DataColumn) CastAsAny(v interface{}) (interface{}, bool) {
	return p.col.CastAsAny(v)
}

// interface AnyDataColumn
func (p *DataColumn) CastArrayAsAny(v interface{}) (interface{}, bool) {
	return p.col.CastArrayAsAny(v)
}

// interface AnyDataColumn
func (p *DataColumn) GetAny(i int) interface{} {
	return p.col.GetAny(i)
}

// interface AnyDataColumn
func (p *DataColumn) SetAny(i int, v interface{}) error {
	return p.col.SetAny(i, v)
}

// interface AnyDataColumn
func (p *DataColumn) SetAnyNoCast(i int, v interface{}) error {
	return p.col.SetAnyNoCast(i, v)
}

// interface AnyDataColumn
func (p *DataColumn) FillAny(s, e int, v interface{}) error {
	return p.col.FillAny(s, e, v)
}

// interface AnyDataColumn
func (p *DataColumn) GetRawValuesAsAny() interface{} {
	return p.col.GetRawValuesAsAny()
}

// interface AnyDataColumn
func (p *DataColumn) CopyAsAny() AnyDataColumn {
	ret := DataColumn{
		typ: p.typ,
		col: p.col.CopyAsAny(),
	}
	return &ret
}

// interface AnyDataColumn
func (p *DataColumn) SliceAsAny(offset, limit int) AnyDataColumn {
	ret := DataColumn{
		typ: p.typ,
		col: p.col.SliceAsAny(offset, limit),
	}
	return &ret
}

// interface AnyDataColumn
func (p *DataColumn) AppendAny(values ...interface{}) error {
	return p.col.AppendAny(values...)
}

// interface AnyDataColumn
func (p *DataColumn) AppendAnyDataColumn(dc AnyDataColumn) error {
	src, ok := dc.(*DataColumn)
	if !ok {
		return fmt.Errorf("Cannot convert type: %v", dc)
	}
	return p.col.AppendAnyDataColumn(src.col)
}

// interface AnyDataColumn
func (p *DataColumn) MakeBufferAsAny(c int) interface{} {
	return p.col.MakeBufferAsAny(c)
}

// interface AnyDataColumn
func (p *DataColumn) CopyBufferByIndex(buf *interface{}, index []int) {
	p.col.CopyBufferByIndex(buf, index)
}

// interface AnyDataColumn
func (p *DataColumn) FillByRowMap(dstRowMap []int, src AnyDataColumn, srcRowMap []int) {
	p.col.FillByRowMap(dstRowMap, src, srcRowMap)
}
