package datatable

// Iterate rows. The argument `i` of the lambda is a loop counter.
func (p *DataTable) ForEach(iter func(i int, row DataRow) bool) {
	length := p.index.Len()

	ctx := NewDataIterContextWithCache(p)
	rel := DataRelation{DataIterContext: ctx, cols: nil, relIndex: -1}

	rr := DataRowRange{DataRelation: &rel, start: 0, end: 0}

	for i := 0; i < length; i++ {
		if iter(i, DataRow{DataRowRange: rr, row: p.index.Get(i)}) {
			break
		}
	}
}

// Get the number of columns of the root objects.
func (p *DataTable) RootObjectColLen() int {
	var n int
	hdr := p.header

	for c := 0; c < len(hdr); c++ {
		relIdx := hdr[c].relIndex
		if relIdx == -1 || (relIdx >= 0 && p.relations[relIdx].parentRelIndex == -1) {
			n++
		}
	}
	return n
}

// Get the root object iterator.
func (p *DataTable) RootObjectsRowRange() DataRowRange {
	var ri int
	if len(p.relations) == 0 {
		ri = -1
	}

	ctx := NewDataIterContextWithCache(p)
	rel := DataRelation{DataIterContext: ctx, cols: ctx.RootObjectCols(), relIndex: ri}

	return DataRowRange{DataRelation: &rel, start: 0, end: p.index.Len()}
}

// Iterate the object graph.
func (p *DataTable) Walk(
	iter func(lv, i int, path []string, fieldNames []string, vals []interface{}, row DataRow, cols []int) bool) bool { // TODO: iter param `isOnce`

	dtColLen := p.ColLen()
	colIndices := make([]int, 0, dtColLen)
	names := make([][]string, 0, dtColLen)
	simpleNames := make([]string, 0, dtColLen)
	values := make([]interface{}, 0, dtColLen)
	rootRange := p.RootObjectsRowRange()

	var fn func(lv int, rr DataRowRange) bool

	fn = func(lv int, rr DataRowRange) bool {
		// TODO: cache namespace, colNames, simpleColNames
		var cnt int
		colLen := rr.ColLen()
		colIndices = colIndices[:colLen]
		names = names[:colLen]
		simpleNames = simpleNames[:colLen]
		values = values[:colLen]

		rr.Names(names)
		var namespace []string
		if colLen > 0 && len(names[0]) > 0 {
			namespace = names[0][:len(names[0])-1]
		}

		return rr.ForEachObjects(func(i int, row DataRow) bool {
			// NOTE: buffers are shared by recursive call. recover values.
			copy(colIndices, rr.cols)
			rr.SimpleNames(simpleNames)

			row.Values(values)

			if iter(lv, cnt, namespace, simpleNames, values, row, colIndices) {
				return true
			}
			cnt++

			return row.ForEachChildRelations(func(j int, rr DataRowRange) bool {
				return fn(lv+1, rr)
			})
		})
	}

	return fn(0, rootRange)
}
