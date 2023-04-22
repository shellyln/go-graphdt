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

			describe := datatable.DescribeObject{
				Name: "Foo",
				Fields: []datatable.DescribeField{
					{"fieldA1", types.Type_Nullable_String},
					{"fieldA2", types.Type_Nullable_String},
					{"fieldA3", types.Type_Nullable_String},
					{"fieldA4", types.Type_Nullable_String},
				},
				Relations: []datatable.DescribeRelation{
					{
						Name: "bar",
						Target: &datatable.DescribeObject{
							Fields: []datatable.DescribeField{
								{"fieldB1", types.Type_Nullable_String},
								{"fieldB2", types.Type_Nullable_String},
								{"fieldB3", types.Type_Nullable_String},
								{"fieldB4", types.Type_Nullable_String},
							},
							Relations: []datatable.DescribeRelation{
								{
									Name: "quxes",
									Target: &datatable.DescribeObject{
										Fields: []datatable.DescribeField{
											{"fieldC1", types.Type_Nullable_String},
											{"fieldC2", types.Type_Nullable_String},
											{"fieldC3", types.Type_Nullable_String},
											{"fieldC4", types.Type_Nullable_String},
										},
									},
								},
							},
						},
						OneToOne: true,
					},
					{
						Name: "quuxes",
						Target: &datatable.DescribeObject{
							Fields: []datatable.DescribeField{
								{"fieldD1", types.Type_Nullable_String},
								{"fieldD2", types.Type_Nullable_String},
								{"fieldD3", types.Type_Nullable_String},
								{"fieldD4", types.Type_Nullable_String},
							},
							Relations: []datatable.DescribeRelation{
								{
									Name: "corges",
									Target: &datatable.DescribeObject{
										Fields: []datatable.DescribeField{
											{"fieldE1", types.Type_Nullable_String},
											{"fieldE2", types.Type_Nullable_String},
											{"fieldE3", types.Type_Nullable_String},
											{"fieldE4", types.Type_Nullable_String},
										},
									},
									Required: true,
								},
								{
									Name: "graults",
									Target: &datatable.DescribeObject{
										Fields: []datatable.DescribeField{
											{"fieldF1", types.Type_Nullable_String},
											{"fieldF2", types.Type_Nullable_String},
											{"fieldF3", types.Type_Nullable_String},
											{"fieldF4", types.Type_Nullable_String},
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
