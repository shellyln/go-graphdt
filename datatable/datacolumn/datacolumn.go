package datacolumn

import (
	. "github.com/shellyln/go-graphdt/datatable/types"
)

type DataColumn struct {
	typ DataColumnType // column type
	col AnyDataColumn  // DataColumnImpl[T]
}

// interface AnyDataColumn
func (p *DataColumn) IsOwned() bool {
	return p.col.IsOwned()
}

// interface AnyDataColumn
func (p *DataColumn) Own() {
	p.col.Own()
}

// interface AnyDataColumn
func (p *DataColumn) Borrow() *DataColumn {
	ret := DataColumn{
		typ: p.typ,
		col: p.col.BorrowAsAny(),
	}
	return &ret
}

// interface AnyDataColumn
func (p *DataColumn) GetType() DataColumnType {
	return p.typ
}

// interface AnyDataColumn
func (p *DataColumn) GetImpl() AnyDataColumn {
	return p.col
}

// interface AnyDataColumn
func (p *DataColumn) Len() int {
	return p.col.Len()
}

// interface AnyDataColumn
func (p *DataColumn) Cap() int {
	return p.col.Cap()
}

// interface AnyDataColumn
func (p *DataColumn) Resize(n int) {
	p.col.Resize(n)
}

// interface AnyDataColumn
func (p *DataColumn) Grow(n int) {
	p.col.Grow(n)
}

// interface AnyDataColumn
func (p *DataColumn) IsNull(i int) bool {
	return p.col.IsNull(i)
}

// interface AnyDataColumn
func (p *DataColumn) Sort(less func(a, b int) bool) {
	p.col.Sort(less)
}

// interface AnyDataColumn
func (p *DataColumn) Reverse() {
	p.col.Reverse()
}

// interface AnyDataColumn
func (p *DataColumn) For(iter EnumeratorFunc) {
	p.col.For(iter)
}

// interface AnyDataColumn
func (p *DataColumn) Filter(filter FilterFunc, iter EnumeratorFunc) {
	p.col.Filter(filter, iter)
}
