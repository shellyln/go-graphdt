package datatable

import (
	"github.com/shellyln/go-graphdt/datatable/sort"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// Reset the index to the same order as the physical rows.
// Destructive method.
func (p *DataTable) ResetIndex() {
	if len(p.cols) < 1 {
		return
	}

	length := p.cols[0].Len()
	newIndex := make([]int, 0, p.cols[0].Len())

	// NOTE: It may faster than p.index.Filter()
	for i := 0; i < length; i++ {
		newIndex[i] = i
	}

	// Set and own
	p.index.SetRawValues(newIndex)

	p.indexChanged = false
}

// Index in reverse order.
func (p *DataTable) Reverse() {
	p.index.Own()
	p.indexChanged = true
	p.index.Reverse()
}

// Sort rows. The index is changed.
// The order of the physical rows does not change.
// Destructive method.
func (p *DataTable) Sort(orders ...SortInfo) error {
	p.index.Own()
	p.indexChanged = true

	// TODO: Check column range
	funcs := make([]SortFunc, len(orders))
	for i, ord := range orders {
		funcs[i] = p.cols[ord.Col].GetSort(ord.Desc, ord.NullsLast)
	}
	less := sort.CombineSortLessFuncs(funcs)

	rawIndex := p.index.GetRawValues()
	p.index.Sort(func(a, b int) bool {
		return less(rawIndex[a], rawIndex[b])
	})

	return nil
}

// Returns a new data table with sliced rows.
// The columns are shared with the original data table.
func (p *DataTable) Slice(offset, limit int) *DataTable {
	ret := p.Borrow()
	ret.index = ret.index.Slice(offset, limit)
	ret.indexChanged = true
	return ret
}
