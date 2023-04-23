package datatable

import (
	"bufio"
	"encoding/json"
	"io"

	"github.com/shellyln/go-nameutil/nameutil"
)

// AppendFromJSON appends rows from JSON Lines stream.
func (p *DataTable) AppendFromJSONLines(reader io.Reader) error {

	type dtJSONGraphNode struct {
		namespace      []string       // namespace
		parentRelIndex int            // parent relations index
		relIndex       int            // relations index
		cols           []int          // column indices
		colMap         map[string]int // column name to column index
		lastCopiedRow  int            // last copied row
	}

	n := p.Len()

	bufReader := bufio.NewReader(reader)
	decoder := json.NewDecoder(bufReader)

	baseNamespace := []string{}
	if len(p.relations) > 0 && nameutil.IsValidName(p.relations[0].namespace) {
		baseNamespace = p.relations[0].namespace
	}

	hdr := p.header
	relsDictByName := make(map[string]*dtJSONGraphNode)
	relsDictByRelIdx := make(map[int]*dtJSONGraphNode)

	for i, rel := range p.relations {
		cols := make([]int, 0, len(hdr))
		colMap := make(map[string]int)

		leaf := dtJSONGraphNode{
			namespace:      rel.namespace,
			parentRelIndex: -1,
			relIndex:       i,
			colMap:         colMap,
			lastCopiedRow:  -1,
		}

		for c := 0; c < len(hdr); c++ {
			if hdr[c].relIndex == i {
				cols = append(cols, c)
				if len(hdr[c].name) > 0 {
					colMap[hdr[c].name[len(hdr[c].name)-1]] = c
				}
			}
		}
		if len(cols) > 0 && p.header[cols[0]].relIndex >= 0 {
			leaf.parentRelIndex = p.relations[p.header[cols[0]].relIndex].parentRelIndex
		}
		leaf.cols = cols

		var key string
		if nameutil.IsValidName(rel.namespace) {
			key = nameutil.MakeDottedKeyIgnoreCase(rel.namespace, len(rel.namespace))
		}

		relsDictByName[key] = &leaf
		relsDictByRelIdx[i] = &leaf
	}

	rowCursors := make([]int, len(p.relations))
	prevRowCursors := make([]int, len(p.relations))
	rowIds := make([]int, len(p.relations))

	rootCount := 0
	for {
		var jsonObject map[string]interface{}
		err := decoder.Decode(&jsonObject)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		for i := range rowCursors {
			rowCursors[i] = n
			prevRowCursors[i] = -1
		}

		walkJSONObject(0, rootCount, baseNamespace, true, jsonObject, func(
			lv, i int, path []string, isOnce bool, fieldNames []string, vals []interface{}, cur interface{}) {

			key := nameutil.MakeDottedKeyIgnoreCase(path, len(path))
			leaf, ok := relsDictByName[key]
			if !ok {
				return
			}

			row := rowCursors[leaf.relIndex]
			prevRowCursors[leaf.relIndex] = rowCursors[leaf.relIndex]

			if row >= n {
				// Zero level first or repeat within same path
				rowCursors[leaf.relIndex]++
				n = rowCursors[leaf.relIndex]
				p.Grow(n)

				for j := range p.relations {
					p.relations[j].vec.Set(p.index.Get(row), -1)
				}
				for next := leaf.parentRelIndex; next >= 0; next = p.relations[next].parentRelIndex {
					rowCursors[next] = n
				}
			} else {
				// When the path changed
				rowCursors[leaf.relIndex] = n
			}

			rawIndex := p.index.GetRawValues()

			for j, name := range fieldNames {
				if c, ok := leaf.colMap[name]; ok {
					p.cols[c].SetAny(rawIndex[row], vals[j])
				}
			}
			p.relations[leaf.relIndex].vec.Set(rawIndex[row], rowIds[leaf.relIndex])
			rowIds[leaf.relIndex]++

			if lv > 0 && i > 0 {
				for next := leaf.parentRelIndex; next >= 0; next = p.relations[next].parentRelIndex {
					l2 := relsDictByRelIdx[next]
					if l2.lastCopiedRow < row {
						relPrevRow := prevRowCursors[l2.relIndex]

						start := relPrevRow
						if start < l2.lastCopiedRow {
							start = l2.lastCopiedRow
						}

						for k := start + 1; k <= row; k++ {
							for _, c := range l2.cols {
								p.cols[c].SetAny(rawIndex[k], p.cols[c].GetAny(rawIndex[relPrevRow]))
							}
							p.relations[next].vec.Set(rawIndex[k], p.relations[next].vec.Get(rawIndex[relPrevRow]))
						}
					}
					l2.lastCopiedRow = row
				}
			}
		})
		rootCount++
	}
	return nil
}

// walkJSONObject walks the JSON object and calls iter for each object.
func walkJSONObject(
	lv, idx int, path []string, isOnce bool, obj map[string]interface{},
	iter func(lv, i int, path []string, isOnce bool, fieldNames []string, vals []interface{}, cur interface{})) {

	names := make([]string, 0, 16)
	values := make([]interface{}, 0, 16)
	onceRelNames := make([]string, 0, 4)
	onceRels := make([]map[string]interface{}, 0, 4)
	manyRelNames := make([]string, 0, 4)
	manyRels := make([][]map[string]interface{}, 0, 4)

	for k, v := range obj {
		switch tv := v.(type) {
		case map[string]interface{}:
			onceRelNames = append(onceRelNames, k)
			onceRels = append(onceRels, tv)
		case []interface{}:
			isArray := true
			ar := make([]map[string]interface{}, 0, len(tv))
			for j := range tv {
				if tv[j] == nil {
					continue
				}
				if m, ok := tv[j].(map[string]interface{}); ok {
					ar = append(ar, m)
				} else {
					isArray = false
					break
				}
			}
			if isArray {
				manyRelNames = append(manyRelNames, k)
				manyRels = append(manyRels, ar)
			} else {
				names = append(names, k)
				values = append(values, tv)
			}
		default:
			names = append(names, k)
			values = append(values, tv)
		}
	}

	iter(lv, idx, path, isOnce, names, values, obj)

	childPath := make([]string, len(path)+1)
	copy(childPath, path)
	childPathTail := len(childPath) - 1

	for i := range onceRelNames {
		childPath[childPathTail] = onceRelNames[i]
		walkJSONObject(lv+1, 0, childPath, true, onceRels[i], iter)
	}

	for i := range manyRelNames {
		childPath[childPathTail] = manyRelNames[i]
		for j := range manyRels[i] {
			walkJSONObject(lv+1, j, childPath, false, manyRels[i][j], iter)
		}
	}
}

// ToUntyped converts the data table record to untyped object (`map[string]interface{}`).
// Object graph is recovered from the data table.
func (p *DataTable) ToUntyped(iter func(i int, record map[string]interface{}) bool) bool {
	rootCount := -1
	objs := make([]interface{}, len(p.relations))

	if p.Walk(func(lv, i int, path, fieldNames []string, vals []interface{}, row DataRow, cols []int) bool {
		if lv == 0 {
			rootCount++
			if rootCount > 0 {
				// previous object
				if iter(rootCount, objs[0].(map[string]interface{})) {
					return true
				}
			}
		}

		item := make(map[string]interface{})
		isOneToOne := p.relations[row.relIndex].OneToOne

		if lv == 0 || isOneToOne {
			objs[lv] = item
		} else {
			if i == 0 {
				ar := make([]map[string]interface{}, 1, 8)
				ar[0] = item
				objs[lv] = ar
			} else {
				objs[lv] = append(objs[lv].([]map[string]interface{}), item)
			}
		}

		if lv > 0 {
			switch parent := objs[lv-1].(type) {
			case []map[string]interface{}:
				parent[len(parent)-1][path[len(path)-1]] = objs[lv]
			case map[string]interface{}:
				parent[path[len(path)-1]] = objs[lv]
			}
		}

		for c := range fieldNames {
			item[fieldNames[c]] = vals[c]
		}

		return false
	}) {
		return true
	}

	if rootCount >= 0 {
		// last object
		if iter(rootCount, objs[0].(map[string]interface{})) {
			return true
		}
	}

	return false
}
