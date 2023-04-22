package datatable

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/datacolumn"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// Take ownership of all columns and indices.
// Columns and indices that did not have ownership will be copied.
func (p *DataTable) Own() {
	for _, col := range p.cols {
		col.Own()
	}
	p.index.Own()
}

// Borrow all columns and indices.
// The newly returned DataTable shares memory for columns and indices.
func (p *DataTable) Borrow() *DataTable {
	ret := DataTable{
		is3VL:        p.is3VL,
		header:       p.header,
		cols:         make([]AnyDataColumn, len(p.cols)),
		index:        p.index.Borrow(),
		indexChanged: p.indexChanged,
		rtCtx:        p.rtCtx,
		relations:    p.relations,
	}
	for i := 0; i < len(p.cols); i++ {
		ret.cols[i] = p.cols[i].BorrowAsAny()
	}
	for i := 0; i < len(p.relations); i++ {
		ret.relations[i] = p.relations[i]
		ret.relations[i].vec = p.relations[i].vec.Borrow()
	}
	return &ret
}

// Convert the table to have physical columns of index order and size.
// Destructive method.
func (p *DataTable) Materialize() {
	if !p.indexChanged {
		return
	}

	p.index.Own()
	length := p.index.Len()

	for i, srcCol := range p.cols {
		if srcCol == nil {
			// NOTE: In the case of a call from Select(), it is nilable.
			continue
		}
		dstCol := datacolumn.NewDataColumnWithSize(length, length, srcCol.GetType())
		srcLen := srcCol.Len()
		for j, k := range p.index.GetRawValues() {
			if k < srcLen {
				// NOTE: In the case of a call from Resize(), the column Len is shorter than the index Len.
				dstCol.SetAnyNoCast(j, srcCol.GetAny(k)) // TODO: swap
			}
		}
		p.cols[i] = dstCol
	}

	for i, srcRelVec := range p.relations {
		dstRelVec := datacolimpl.NewDataColumnImplWithSize[int](length, length, Type_Int)
		srcLen := srcRelVec.vec.Len()
		rawSrcRelVec := srcRelVec.vec.GetRawValues()
		rawDstRelVec := dstRelVec.GetRawValues()
		for j, k := range p.index.GetRawValues() {
			if k < srcLen {
				rawDstRelVec[j] = rawSrcRelVec[k]
			}
		}
		p.relations[i].vec = dstRelVec
	}

	rawIndex := p.index.GetRawValues()
	for i := 0; i < length; i++ {
		rawIndex[i] = i
	}

	p.indexChanged = false
}
