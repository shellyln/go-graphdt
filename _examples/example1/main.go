package main

import (
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

var csv1Bytes []byte
var csv2Bytes []byte

func main() {
	csv1Bytes = []byte(`Foo,Bar,Baz
1,2,3`)
	csv2Bytes = []byte(`Foo,Qux,Quux,Corge
1,1,2,3
1,11,12,13`)

	ctx := runtime.NewRuntimeContext()

	dt1 := datatable.NewDataTableWithSize(
		ctx,
		0, 0,
		types.Type_Nullable_String,
		types.Type_Nullable_I64,
		types.Type_Nullable_F64,
	)
	dt1.SetSimpleHeader([]string{"Foo", "Bar", "Baz"})

	dt2 := datatable.NewDataTableWithSize(
		ctx,
		0, 0,
		types.Type_Nullable_String,
		types.Type_Nullable_String,
		types.Type_Nullable_I64,
		types.Type_Nullable_F64,
	)
	dt2.SetSimpleHeader([]string{"Foo", "Qux", "Quux", "Corge"})

	buf1 := bytes.NewReader(csv1Bytes)
	reader1 := csv.NewReader(buf1)

	if err := dt1.AppendFromCSV(reader1, datatable.CSVOptions{HasHeader: true}); err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	err := dt1.Filter([]types.FilterInfo{
		{Op: types.Op_LoadCol, Col: 1},
		{Op: types.Op_LoadImmediate, Param: 63},
		{Op: types.Op_Lt},
		{Op: types.Op_LoadCol, Col: 1},
		{Op: types.Op_LoadImmediate, Param: 64},
		{Op: types.Op_Gt},
		{Op: types.Op_Or},
	}...)
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	err = dt1.Sort([]types.SortInfo{
		{Col: 0, Desc: false, NullsLast: true},
		{Col: 2},
	}...)
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	buf2 := bytes.NewReader(csv2Bytes)
	reader2 := csv.NewReader(buf2)

	if err := dt2.AppendFromCSV(reader2, datatable.CSVOptions{HasHeader: true}); err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	result, err := dt1.LeftJoin(0, dt2, 0, "quxes", datatable.JoinOptions{})
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	fmt.Println(result.ColNames())
	fmt.Println(result.ColSimpleNames())

	values := make([]interface{}, result.ColLen())
	result.ForEach(func(i int, row datatable.DataRow) bool {
		fmt.Println(row.Values(values))
		return false
	})

	result.ToUntyped(func(i int, record map[string]interface{}) bool {
		fmt.Println(record)
		return false
	})
}
