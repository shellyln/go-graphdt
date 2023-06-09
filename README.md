# GraphDT

A datatable that represents object graphs.


[![Test](https://github.com/shellyln/go-graphdt/actions/workflows/test.yml/badge.svg)](https://github.com/shellyln/go-graphdt/actions/workflows/test.yml)
[![release](https://img.shields.io/github/v/release/shellyln/go-graphdt)](https://github.com/shellyln/go-graphdt/releases)
[![Go version](https://img.shields.io/github/go-mod/go-version/shellyln/go-graphdt)](https://github.com/shellyln/go-graphdt)

<img src="https://raw.githubusercontent.com/shellyln/go-graphdt/master/_assets/logo-graphdt.svg" alt="logo" style="width:250px;" width="250">

---


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
        ctx, 0, 0,
        types.Type_Nullable_String,
        types.Type_Nullable_I64,
        types.Type_Nullable_F64,
    )
    dt1.SetSimpleHeader([]string{"Foo", "Bar", "Baz"})

    dt2 := datatable.NewDataTableWithSize(
        ctx, 0, 0,
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
```

## 🚧 TODO

* Standard scalar / aggregation functions
* Data adapter interface
* Unit tests
* Documents


## ⚖️ License

MIT  
Copyright (c) 2023 Shellyl_N and Authors.
