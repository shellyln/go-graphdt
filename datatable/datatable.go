package datatable

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
	"github.com/shellyln/go-nameutil/nameutil"
)

var (
	// This is for internal debugging only. DO NOT USE THIS in your code. Disable "fast filter" if this is true.
	DbgNoFastFilter = false
)

// Data table
type DataTable struct {
	is3VL        bool                             // 3VL (Three-valued logic) mode
	header       []dtHeader                       // table header informations
	cols         []AnyDataColumn                  // table column data
	index        *datacolimpl.DataColumnImpl[int] // table index for sorting and filtering
	indexChanged bool                             // true if index is changed by sorting or filtering
	rtCtx        *runtime.RuntimeContext          // runtime context for evaluating expressions

	// It is added by table joins.
	// If column refers to the vec, rows that have the same vec value belong to the same object instance.
	// If column does not refer to the vec, each row is an object instance.
	// The rowid of the original table is used for the value of vec.
	relations []dtRelation
}

// Header information of a datatable column.
type dtHeader struct {
	name           []string // column name with namespaces
	parentColIndex int      //
	relIndex       int      // relations index. -1 if this is not has relation.
}

// Relation information
type dtRelation struct {
	namespace      []string                         // namespace of the relation
	vec            *datacolimpl.DataColumnImpl[int] // relation vector
	parentRelIndex int                              //
	keyColIndex    int                              //
	OneToOne       bool                             //
	Required       bool                             //
}

// Returns the number of rows in all columns and indices.
func (p *DataTable) Len() int {
	return p.index.Len()
}

// Returns the capacity of columns and indexes.
func (p *DataTable) Cap() int {
	if len(p.cols) > 0 {
		return p.cols[0].Cap()
	}
	return 0
}

// Returns the number of columns.
func (p *DataTable) ColLen() int {
	return len(p.cols)
}

// Returns the column names with namespaces.
func (p *DataTable) ColNames() [][]string {
	ret := make([][]string, len(p.cols))
	for i := range p.header {
		if nameutil.IsValidName(p.header[i].name) {
			z := make([]string, len(p.header[i].name))
			copy(z, p.header[i].name)
			ret[i] = z
		} else {
			ret[i] = []string{}
		}
	}
	return ret
}

// Returns the column names without namespaces.
func (p *DataTable) ColSimpleNames() []string {
	ret := make([]string, len(p.cols))
	for i := range p.header {
		if nameutil.IsValidName(p.header[i].name) {
			ret[i] = p.header[i].name[len(p.header[i].name)-1]
		} else {
			ret[i] = ""
		}
	}
	return ret
}
