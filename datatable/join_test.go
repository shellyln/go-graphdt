package datatable_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func TestDataTable_LeftJoin(t *testing.T) {
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
			ctx := runtime.NewRuntimeContext()

			dtL := datatable.NewDataTableWithSize(
				ctx,
				//1500, 1500,
				3*2, 3*2,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)
			dtL.SetHeader([][]string{
				{"Qwerty", "col0"},
				{"Qwerty", "col1"},
				{"Qwerty", "col2"},
				{"Qwerty", "col3"},
				{"Qwerty", "col4"},
				{"Qwerty", "col5"},
			})

			dtL.ForEach(func(i int, row datatable.DataRow) bool {
				switch i % 3 {
				case 0:
					// do nothing (set null)
				case 1:
					row.Set(0, types.NewNullable("quux"+strconv.Itoa(i/3))) // unmatched keys
				default:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa(i/3))) // matched keys
				}
				row.Set(1, types.NewNullable(uint16(i)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dtR := datatable.NewDataTableWithSize(
				ctx,
				//2000, 2000,
				4*2, 4*2,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)
			dtR.SetHeader([][]string{
				{"Asdfgh", "field0"},
				{"Asdfgh", "field1"},
				{"Asdfgh", "field2"},
				{"Asdfgh", "field3"},
				{"Asdfgh", "field4"},
				{"Asdfgh", "field5"},
			})

			dtR.ForEach(func(i int, row datatable.DataRow) bool {
				switch (dtR.Len() - 1 - i) % 4 {
				case 0:
					// do nothing (set null)
				case 1:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa((dtR.Len()-1-i)/4+10000))) // unmatched keys
				default:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa((dtR.Len()-1-i)/4))) // matched keys x 2
				}
				row.Set(1, types.NewNullable(uint16(dtR.Len()-1-i)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			result, err := dtL.LeftJoin(0, dtR, 0, "foobars", datatable.JoinOptions{})
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if result.Len() != dtR.Len() {
				t.Errorf("filter result len = %v", result.Len())
				return
			}

			result2, err := result.LeftJoin(6, dtR, 0, "quuxs", datatable.JoinOptions{})
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if result2.Len() != dtL.Len()*2 {
				t.Errorf("filter result len = %v", result2.Len())
				return
			}

			values := make([]interface{}, result2.ColLen())
			result2.ForEach(func(i int, row datatable.DataRow) bool {
				// t.Logf("row = %v", row)
				fmt.Printf("row[%d] = %v\n", i, row.Values(values))
				return false
			})

			result2.Walk(func(lv, i int, path, fieldNames []string, vals []interface{}, row datatable.DataRow, cols []int) bool {
				fmt.Println(lv, i, path, fieldNames, vals, cols)
				return false
			})
		})
	}
}

func TestDataTable_InnerJoin(t *testing.T) {
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
			ctx := runtime.NewRuntimeContext()

			dtL := datatable.NewDataTableWithSize(
				ctx,
				//1500, 1500,
				3*2, 3*2,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)
			dtL.SetHeader(nil)

			dtL.ForEach(func(i int, row datatable.DataRow) bool {
				switch i % 3 {
				case 0:
					// do nothing (set null)
				case 1:
					row.Set(0, types.NewNullable("quux"+strconv.Itoa(i/3))) // unmatched keys
				default:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa(i/3))) // matched keys
				}
				row.Set(1, types.NewNullable(uint16(i)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dtR := datatable.NewDataTableWithSize(
				ctx,
				//2000, 2000,
				4*2, 4*2,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)
			dtR.SetHeader(nil)

			dtR.ForEach(func(i int, row datatable.DataRow) bool {
				switch (dtR.Len() - 1 - i) % 4 {
				case 0:
					// do nothing (set null)
				case 1:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa((dtR.Len()-1-i)/4+10000))) // unmatched keys
				default:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa((dtR.Len()-1-i)/4))) // matched keys x 2
				}
				row.Set(1, types.NewNullable(uint16(dtR.Len()-1-i)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			result, err := dtL.InnerJoin(0, dtR, 0, "foobars", datatable.JoinOptions{})
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if result.Len() != dtR.Len()/2 {
				t.Errorf("filter result len = %v", result.Len())
				return
			}
			// result.For(func(i int, row datatable.DataRow) {
			// 	t.Logf("row = %v", row)
			// })

			result2, err := result.InnerJoin(0, dtR, 0, "quuxs", datatable.JoinOptions{})
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if result2.Len() != dtR.Len() {
				t.Errorf("filter result len = %v", result2.Len())
				return
			}
		})
	}
}

func TestDataTable_FullJoin(t *testing.T) {
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
			ctx := runtime.NewRuntimeContext()

			dtL := datatable.NewDataTableWithSize(
				ctx,
				//1500, 1500,
				3*2, 3*2,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)
			dtL.SetHeader(nil)

			dtL.ForEach(func(i int, row datatable.DataRow) bool {
				switch i % 3 {
				case 0:
					// do nothing (set null)
				case 1:
					row.Set(0, types.NewNullable("quux"+strconv.Itoa(i/3))) // unmatched keys
				default:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa(i/3))) // matched keys
				}
				row.Set(1, types.NewNullable(uint16(i)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dtR := datatable.NewDataTableWithSize(
				ctx,
				//2000, 2000,
				4*2, 4*2,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)
			dtR.SetHeader(nil)

			dtR.ForEach(func(i int, row datatable.DataRow) bool {
				switch (dtR.Len() - 1 - i) % 4 {
				case 0:
					// do nothing (set null)
				case 1:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa((dtR.Len()-1-i)/4+10000))) // unmatched keys
				default:
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa((dtR.Len()-1-i)/4))) // matched keys x 2
				}
				row.Set(1, types.NewNullable(uint16(dtR.Len()-1-i)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			result, err := dtL.FullJoin(0, dtR, 0, "foobars", datatable.JoinOptions{})
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if result.Len() != dtL.Len()*2 {
				t.Errorf("filter result len = %v", result.Len())
				return
			}
			// result.For(func(i int, row datatable.DataRow) {
			// 	t.Logf("row = %v", row)
			// })

			result2, err := result.FullJoin(0, dtR, 0, "quuxs", datatable.JoinOptions{})
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if result2.Len() != dtL.Len()*2+dtR.Len() {
				t.Errorf("filter result len = %v", result2.Len())
				return
			}
		})
	}
}
