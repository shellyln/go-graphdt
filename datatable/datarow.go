package datatable

type DataIterContext struct {
	dt              *DataTable               // data table
	targetRelsCache map[int]map[int]struct{} // relIndex to targetRels
	colsCache       map[int][]int            // relIndex to cols
}

type DataRelation struct {
	DataIterContext
	cols     []int // column indices
	relIndex int   //
}

type DataRowRange struct {
	*DataRelation
	start int // logical row index start
	end   int // logical row index end; the range not including `end`
}

// DataRow represents a row in the data table.
type DataRow struct {
	DataRowRange
	row int // phisical row index
}

// Make a new data iterator context.
func NewDataIterContextWithCache(dt *DataTable) DataIterContext {
	ctx := DataIterContext{
		dt:              dt,
		targetRelsCache: make(map[int]map[int]struct{}), // Child relations of the relation.
		colsCache:       make(map[int][]int),            // Column indices of the relation.
	}

	hdr := dt.header

	for i := range dt.relations {
		targetRels := make(map[int]struct{})
		cols := make([]int, 0, len(hdr))

		for c := 0; c < len(hdr); c++ {
			if hdr[c].relIndex >= 0 && dt.relations[hdr[c].relIndex].parentRelIndex == i {
				targetRels[hdr[c].relIndex] = struct{}{}
			}
			if hdr[c].relIndex == i {
				cols = append(cols, c)
			}
		}
		ctx.targetRelsCache[i] = targetRels
		ctx.colsCache[i] = cols
	}
	return ctx
}

// Get the number of columns of the root objects.
func (s DataIterContext) RootObjectCols() []int {
	if s.colsCache != nil {
		if cs, ok := s.colsCache[0]; ok {
			return cs
		}
	}

	hdr := s.dt.header
	cols := make([]int, 0, len(hdr))

	for c := 0; c < len(hdr); c++ {
		relIdx := hdr[c].relIndex
		if relIdx == -1 || (relIdx >= 0 && s.dt.relations[relIdx].parentRelIndex == -1) {
			cols = append(cols, c)
		}
	}
	return cols
}

// Get the number of columns of the current objects.
func (s DataRelation) ColLen() int {
	if s.cols != nil {
		return len(s.cols)
	} else {
		return len(s.dt.header)
	}
}

// Get the column names as a slice.
func (s DataRelation) Names(ns [][]string) [][]string {
	cols := s.cols
	hdr := s.dt.header
	if cols != nil {
		length := len(cols)
		for c := 0; c < length; c++ {
			ns[c] = hdr[cols[c]].name
		}
	} else {
		length := len(hdr)
		for c := 0; c < length; c++ {
			ns[c] = hdr[c].name
		}
	}
	return ns
}

// Get the column names as a slice.
func (s DataRelation) SimpleNames(ns []string) []string {
	cols := s.cols
	hdr := s.dt.header
	if cols != nil {
		length := len(cols)
		for c := 0; c < length; c++ {
			if hdr[cols[c]].name != nil {
				ns[c] = hdr[cols[c]].name[len(hdr[cols[c]].name)-1]
			} else {
				ns[c] = ""
			}
		}
	} else {
		length := len(hdr)
		for c := 0; c < length; c++ {
			if hdr[c].name != nil {
				ns[c] = hdr[c].name[len(hdr[c].name)-1]
			} else {
				ns[c] = ""
			}
		}
	}
	return ns
}

// Iterate the child relations.
func (s DataRelation) ForEachChildRelationDescribes(iter func(i int, rel DataRelation) bool) bool {
	if s.relIndex < 0 {
		return false
	}

	k := 0
	for relIndex := range s.targetRelsCache[s.relIndex] {
		cols := s.colsCache[relIndex]

		rel := DataRelation{DataIterContext: s.DataIterContext, cols: cols, relIndex: relIndex}

		if iter(k, rel) {
			return true
		}
		k++
	}
	return false
}

// Iterate the objects.
func (s DataRowRange) ForEachObjects(iter func(i int, row DataRow) bool) bool {
	rawIndex := s.dt.index.GetRawValues()
	rr := s

	if s.relIndex >= 0 {
		relVec := s.dt.relations[s.relIndex].vec.GetRawValues()

		for i, prevVal := s.start, -1; i < s.end; i++ {
			row := rawIndex[i]
			val := relVec[row]
			if val >= 0 && val != prevVal {
				j := i + 1
				for ; j < s.end; j++ {
					if val != relVec[rawIndex[j]] {
						break
					}
				}

				rr.start = i
				rr.end = j
				if iter(i, DataRow{DataRowRange: rr, row: row}) {
					return true
				}

				i = j - 1
			}
			prevVal = val
		}
	} else {
		for i := s.start; i < s.end; i++ {
			rr.start = i
			rr.end = i + 1
			if iter(i, DataRow{DataRowRange: rr, row: rawIndex[i]}) {
				return true
			}
		}
	}
	return false
}

// Get the column value at the index.
func (s DataRow) Get(col int) interface{} {
	return s.dt.cols[col].GetAny(s.row)
}

// Set the column value at the index.
func (s DataRow) Set(col int, v interface{}) error {
	return s.dt.cols[col].SetAny(s.row, v)
}

// Get the column values as a slice.
func (s DataRow) Values(vs []interface{}) []interface{} {
	columns := s.dt.cols
	cols := s.cols
	if cols != nil {
		length := len(cols)
		for c := 0; c < length; c++ {
			col := columns[cols[c]]
			if col.IsNull(s.row) {
				vs[c] = nil
			} else {
				// TODO: unwrap Nullable[T]
				vs[c] = col.GetAny(s.row)
			}
		}
	} else {
		length := len(columns)
		for c := 0; c < length; c++ {
			col := columns[c]
			if col.IsNull(s.row) {
				vs[c] = nil
			} else {
				// TODO: unwrap Nullable[T]
				vs[c] = col.GetAny(s.row)
			}
		}
	}
	return vs
}

// Get the column raw values as a slice.
func (s DataRow) RawValues(vs []interface{}) []interface{} {
	columns := s.dt.cols
	cols := s.cols
	if cols != nil {
		length := len(cols)
		for c := 0; c < length; c++ {
			vs[c] = columns[cols[c]].GetAny(s.row)
		}
	} else {
		length := len(columns)
		for c := 0; c < length; c++ {
			vs[c] = columns[c].GetAny(s.row)
		}
	}
	return vs
}

// Iterate the column.
func (s DataRow) ForEachColumns(iter func(c int, name []string, v interface{})) {
	columns := s.dt.cols
	cols := s.cols
	hdr := s.dt.header
	if cols != nil {
		length := len(cols)
		for c := 0; c < length; c++ {
			iter(c, hdr[cols[c]].name, columns[cols[c]].GetAny(s.row))
		}
	} else {
		length := len(columns)
		for c := 0; c < length; c++ {
			iter(c, hdr[c].name, columns[c].GetAny(s.row))
		}
	}
}

// Iterate the child relations.
func (s DataRow) ForEachChildRelations(iter func(i int, rr DataRowRange) bool) bool {
	if s.relIndex < 0 {
		return false
	}

	k := 0
	for relIndex := range s.targetRelsCache[s.relIndex] {
		cols := s.colsCache[relIndex]

		rel := DataRelation{DataIterContext: s.DataIterContext, cols: cols, relIndex: relIndex}

		if iter(k, DataRowRange{DataRelation: &rel, start: s.start, end: s.end}) {
			return true
		}
		k++
	}
	return false
}
