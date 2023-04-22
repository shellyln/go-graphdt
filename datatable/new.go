package datatable

import (
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/datacolumn"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

// Create a new DataTable.
func NewDataTable(rtCtx *runtime.RuntimeContext, types ...DataColumnType) *DataTable {
	return NewDataTableWithSize(rtCtx, 0, datacolimpl.DataColumnImpl_DefaultSize, types...)
}

// Create a new DataTable with size and capacity.
func NewDataTableWithSize(rtCtx *runtime.RuntimeContext, l, c int, types ...DataColumnType) *DataTable {
	dt := &DataTable{
		is3VL:        true,
		header:       make([]dtHeader, len(types)),
		cols:         make([]AnyDataColumn, len(types)),
		index:        datacolimpl.NewDataColumnImplWithSize[int](l, c, Type_Int),
		indexChanged: false,
		rtCtx:        rtCtx,
		relations:    nil,
	}

	for i := 0; i < len(dt.header); i++ {
		dt.header[i].parentColIndex = -1
		dt.header[i].relIndex = -1
	}

	for i, ty := range types {
		dt.cols[i] = datacolumn.NewDataColumnWithSize(l, c, ty)
	}

	rawIndex := dt.index.GetRawValues()
	for i := 0; i < l; i++ {
		rawIndex[i] = i
	}

	return dt
}

func NewDatatableFromDescribe(rtCtx *runtime.RuntimeContext, describeObj *DescribeObject) (*DataTable, error) {
	dtL := newSingleDtFromDescribeObj(rtCtx, describeObj)

	var fn func(rel DescribeRelation, colL int) error
	fn = func(rel DescribeRelation, colL int) error {
		var err error
		colLen := dtL.ColLen()
		dtR := newSingleDtFromDescribeObj(rtCtx, rel.Target)

		if rel.Required {
			dtL, err = dtL.InnerJoin(colL, dtR, 0, rel.Name, JoinOptions{RightMustZeroOrOnce: rel.OneToOne})
		} else {
			dtL, err = dtL.LeftJoin(colL, dtR, 0, rel.Name, JoinOptions{RightMustZeroOrOnce: rel.OneToOne})
		}
		if err != nil {
			return err
		}

		for _, next := range rel.Target.Relations {
			if err := fn(next, colLen); err != nil {
				return err
			}
		}
		return nil
	}

	for _, rel := range describeObj.Relations {
		if err := fn(rel, 0); err != nil {
			return nil, err
		}
	}

	return dtL, nil
}

func newSingleDtFromDescribeObj(rtCtx *runtime.RuntimeContext, describeObj *DescribeObject) *DataTable {
	colTypes := make([]DataColumnType, len(describeObj.Fields))
	colNames := make([][]string, len(describeObj.Fields))

	for i, fld := range describeObj.Fields {
		colTypes[i] = fld.Type
		colNames[i] = []string{describeObj.Name, describeObj.Fields[i].Name}
	}

	dt := NewDataTableWithSize(rtCtx, 0, 0, colTypes...)
	dt.SetHeader(colNames)

	return dt
}
