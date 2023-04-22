package datatable

// Physically resize the rows.
// Destructive method.
func (p *DataTable) Resize(n int) {
	if n < 0 {
		n = 0
	}

	origLen := p.index.Len()
	if !p.indexChanged && origLen == n {
		return
	}

	p.index.Resize(n)
	rawIndex := p.index.GetRawValues()
	for i := origLen; i < n; i++ {
		rawIndex[i] = i
	}

	if p.indexChanged {
		p.Materialize()
	} else {
		for _, col := range p.cols {
			col.Resize(n)
		}
		for _, rel := range p.relations {
			rel.vec.Resize(n)
		}
	}
}

// Similar to Resize() but it is for extend rows.
func (p *DataTable) Grow(n int) {
	if n < 0 {
		n = 0
	}

	origLen := p.index.Len()
	if !p.indexChanged && origLen == n {
		return
	}

	p.index.Resize(n)
	rawIndex := p.index.GetRawValues()
	for i := origLen; i < n; i++ {
		rawIndex[i] = i
	}

	if p.indexChanged {
		p.Materialize()
	} else {
		for _, col := range p.cols {
			col.Grow(n)
		}
		for _, rel := range p.relations {
			rel.vec.Grow(n)
		}
	}
}
