package datatable

// Append rows. The argument `i` of the lambda is a loop counter.
func (p *DataTable) Append(iter func(i int, row DataRow) bool) {
	n := p.Len() + 1
	p.Grow(n)

	ctx := NewDataIterContextWithCache(p)
	rel := DataRelation{DataIterContext: ctx, cols: ctx.RootObjectCols(), relIndex: -1}

	rr := DataRowRange{DataRelation: &rel, start: 0, end: 0}

	for !iter(n-1, DataRow{DataRowRange: rr, row: n - 1}) {
		n++
		p.Grow(n)
	}

	p.Grow(n - 1)
}
