# GraphDT

A data table that represents object graphs.

## ⭐️ Features

* Sort by multiple columns
* Filter by multiple columns and/or formulas that can include function calls
* Select multiple columns and/or formulas that can include scalar function calls
* Aggregate multiple columns and/or formulas that can include aggregation function calls
    * Group by multiple columns
    * Filter by aggregated values
* Simple iteration of rows
* Object graph based traversal for each row
* Create new data table from describe
* Import and export to CSV
* Import and export to JSON Lines
* Unmarshal to untyped objects
* Unmarshal to typed objects

## 🚀 Getting started

```go
package main

import (
    _ "embed"
    "bytes"
    "encoding/csv"
    "fmt"
    "github.com/shellyln/go-graphdt/datatable"
    "github.com/shellyln/go-graphdt/datatable/runtime"
    "github.com/shellyln/go-graphdt/datatable/types"
)

//go:embed _testdata/test1.csv
var csv1Bytes []byte
//go:embed _testdata/test2.csv
var csv2Bytes []byte

func main() {
    ctx := runtime.NewRuntimeContext()

    dt1 := datatable.NewDataTableWithSize(
        ctx,
        1000, 1000,
        types.Type_Nullable_String,
        types.Type_Nullable_I64,
        types.Type_Nullable_F64,
    )
    dt1.SetSimpleHeader([]string{"Foo", "Bar", "Baz"})

    dt2 := datatable.NewDataTableWithSize(
        ctx,
        1000, 1000,
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

    err = dt1.Filter([]types.FilterInfo{
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

    var dst []Foo
    if err := result.Unmarshal(&dst); err != nil {
        t.Errorf("error = %v\n", err)
        return
    }
}
```

## 🚧 TODO

* Standard scalar / aggregation functions
* Data adapter interface
* Unit tests
* Documents


## ⚖️ License

MIT  
Copyright (c) 2023 Shellyl_N and Authors.
