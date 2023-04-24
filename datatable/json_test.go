package datatable_test

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

//go:embed _testdata/test1.jsonl
var test1JsonBytes []byte

func TestReadEmbedJson1(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{{
		name:    "1",
		args:    args{s: ``},
		want:    nil,
		wantErr: false,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(test1JsonBytes)
			ctx := runtime.NewRuntimeContext()

			describe := types.DescribeObject{
				Name: "Foo",
				Fields: []types.DescribeField{
					{Name: "fieldA1", Type: types.Type_Nullable_String},
					{Name: "fieldA2", Type: types.Type_Nullable_String},
					{Name: "fieldA3", Type: types.Type_Nullable_String},
					{Name: "fieldA4", Type: types.Type_Nullable_String},
				},
				Relations: []types.DescribeRelation{
					{
						Name: "bar",
						Target: &types.DescribeObject{
							Fields: []types.DescribeField{
								{Name: "fieldB1", Type: types.Type_Nullable_String},
								{Name: "fieldB2", Type: types.Type_Nullable_String},
								{Name: "fieldB3", Type: types.Type_Nullable_String},
								{Name: "fieldB4", Type: types.Type_Nullable_String},
							},
							Relations: []types.DescribeRelation{
								{
									Name: "quxes",
									Target: &types.DescribeObject{
										Fields: []types.DescribeField{
											{Name: "fieldC1", Type: types.Type_Nullable_String},
											{Name: "fieldC2", Type: types.Type_Nullable_String},
											{Name: "fieldC3", Type: types.Type_Nullable_String},
											{Name: "fieldC4", Type: types.Type_Nullable_String},
										},
									},
								},
							},
						},
						OneToOne: true,
					},
					{
						Name: "quuxes",
						Target: &types.DescribeObject{
							Fields: []types.DescribeField{
								{Name: "fieldD1", Type: types.Type_Nullable_String},
								{Name: "fieldD2", Type: types.Type_Nullable_String},
								{Name: "fieldD3", Type: types.Type_Nullable_String},
								{Name: "fieldD4", Type: types.Type_Nullable_String},
							},
							Relations: []types.DescribeRelation{
								{
									Name: "corges",
									Target: &types.DescribeObject{
										Fields: []types.DescribeField{
											{Name: "fieldE1", Type: types.Type_Nullable_String},
											{Name: "fieldE2", Type: types.Type_Nullable_String},
											{Name: "fieldE3", Type: types.Type_Nullable_String},
											{Name: "fieldE4", Type: types.Type_Nullable_String},
										},
									},
									Required: true,
								},
								{
									Name: "graults",
									Target: &types.DescribeObject{
										Fields: []types.DescribeField{
											{Name: "fieldF1", Type: types.Type_Nullable_String},
											{Name: "fieldF2", Type: types.Type_Nullable_String},
											{Name: "fieldF3", Type: types.Type_Nullable_String},
											{Name: "fieldF4", Type: types.Type_Nullable_String},
										},
									},
								},
							},
						},
					},
				},
			}

			dtFoo, err := datatable.NewDatatableFromDescribe(ctx, &describe)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}

			if err := dtFoo.AppendFromJSONLines(buf); err != nil {
				t.Errorf("error = %v", err)
				return
			}

			w := bufio.NewWriter(os.Stdout)
			defer w.Flush()

			names := make([][]string, dtFoo.ColLen())
			values := make([]interface{}, dtFoo.ColLen())

			dtFoo.ForEach(func(i int, row datatable.DataRow) bool {
				fmt.Fprintln(w, row.Names(names))
				return true
			})

			fmt.Fprintln(w, dtFoo.ColNames())
			fmt.Fprintln(w, dtFoo.ColSimpleNames())

			dtFoo.ForEach(func(i int, row datatable.DataRow) bool {
				// t.Logf("row = %v", row)
				fmt.Fprintf(w, "row[%d] = %v\n", i, row.Values(values))
				return false
			})

			dtFoo.Walk(func(lv, i int, path, fieldNames []string, vals []interface{}, row datatable.DataRow, cols []int) bool {
				fmt.Fprintln(w, lv, i, path, fieldNames, vals, cols)
				return false
			})

			dtFoo.ToUntyped(func(i int, record map[string]interface{}) bool {
				if b, err := json.MarshalIndent(record, "", "    "); err != nil {
					t.Errorf("error = %v", err)
					return true
				} else {
					fmt.Fprintln(w, string(b))
				}
				return false
			})

			{
				describeObj := dtFoo.GetDescribe()
				if b, err := json.MarshalIndent(describeObj, "", "    "); err != nil {
					t.Errorf("error = %v", err)
				} else {
					fmt.Fprintln(w, string(b))
				}
			}
		})
	}
}
