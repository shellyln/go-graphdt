package datatable_test

import (
	_ "embed"
	"math/rand"
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/function/scalar"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func TestSortAndFilter1(t *testing.T) {
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
			// datatable.DbgNoFastFilter = true

			ctx := runtime.NewRuntimeContext()
			dt := datatable.NewDataTableWithSize(
				ctx,
				1000, 1000,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)

			dt.ForEach(func(i int, row datatable.DataRow) bool {
				row.Set(0, types.NewNullable("foobar"))
				// row.Set(1, types.NewNullable(int64(rand.Int31())))
				row.Set(1, types.NewNullable(uint16(63)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			err := dt.Sort([]types.SortInfo{
				{Col: 1, Desc: false, NullsLast: true},
				{Col: 2, Desc: true},
				{Col: 0},
			}...)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			// dt.Slice(100000-10, 200)
			// buf := make([]interface{}, dt.ColLen())
			// dt.ForEach(func(i int, row datatable.DataRow) {
			// 	fmt.Printf("%v\n", row.Values(buf))
			// })
			// fmt.Printf("==================================\n")

			err = dt.Filter([]types.FilterInfo{
				// 1
				{Op: types.Op_LoadCol, Col: 0},
				{Op: types.Op_LoadImmediate, Param: "fo%"},
				{Op: types.Op_Like},
				// 1
				{Op: types.Op_LoadCol, Col: 0},
				{Op: types.Op_LoadImmediate, Param: "^fo.+$"},
				{Op: types.Op_Match},
				// 1
				{Op: types.Op_LoadCol, Col: 0},
				{Op: types.Op_LoadImmediate, Param: "qo%"},
				{Op: types.Op_NotLike},
				// 1
				{Op: types.Op_LoadCol, Col: 0},
				{Op: types.Op_LoadImmediate, Param: "^qo.+$"},
				{Op: types.Op_NotMatch},
				//
				{Op: types.Op_And},
				{Op: types.Op_And},
				{Op: types.Op_And},
				// 2
				{Op: types.Op_LoadCol, Col: 1},
				{Op: types.Op_LoadImmediate, Param: "63"},
				{Op: types.Op_Eq},
				{Op: types.Op_LoadCol, Col: 1},
				{Op: types.Op_LoadImmediate, Param: "64"},
				{Op: types.Op_NotEq},
				{Op: types.Op_And},
				// 3
				{Op: types.Op_LoadCol, Col: 2},
				{Op: types.Op_LoadImmediate, Param: types.NewNullable(float32(0.8))},
				{Op: types.Op_Ge},
				// 4
				{Op: types.Op_LoadCol, Col: 2},
				{Op: types.Op_LoadImmediate, Param: types.NewNullable(float32(0.2))},
				{Op: types.Op_Le},
				// 3 or 4 -> 5
				{Op: types.Op_Or},
				// 2 and 5 -> 6
				{Op: types.Op_And},
				// 1 and 6 -> 7
				{Op: types.Op_And},
				// 8
				{Op: types.Op_LoadCol, Col: 1},
				{Op: types.Op_LoadImmediate, Param: []any{types.NewNullable(float32(61)), types.NewNullable(float32(63)), types.NewNullableAsNull[int32]()}},
				{Op: types.Op_In},
				// 9
				{Op: types.Op_LoadCol, Col: 1},
				{Op: types.Op_LoadImmediate, Param: []any{types.NewNullable(float32(61)), types.NewNullable(float32(67))}},
				{Op: types.Op_NotIn},
				// 8 and 9 -> 10
				{Op: types.Op_And},
				// 7 and 10 -> 11
				{Op: types.Op_And},
				// 12
				{Op: types.Op_LoadCol, Col: 5},
				{Op: types.Op_LoadImmediate, Param: []any{types.NewNullable("foo;qux"), types.NewNullable("aaa;bbb;ccc"), types.NewNullableAsNull[string]()}},
				{Op: types.Op_Includes},
				// 13
				{Op: types.Op_LoadCol, Col: 5},
				{Op: types.Op_LoadImmediate, Param: []any{types.NewNullable("fooo;qux"), types.NewNullable("aaa;bbb;ccc")}},
				{Op: types.Op_Excludes},
				// 12 and 13 -> 14
				{Op: types.Op_And},
				// 11 and 14 -> 15
				{Op: types.Op_And},
			}...)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if dt.Len() == 0 {
				t.Errorf("filter result len = %v", dt.Len())
				return
			}
			// dt.ForEach(func(i int, row datatable.DataRow) {
			// 	fmt.Printf("%v\n", row.Values(buf))
			// })
			// fmt.Printf("==================================\n")

			err = dt.Sort([]types.SortInfo{
				{Col: 1, Desc: false, NullsLast: true},
				{Col: 2, Desc: false},
				//{Col: 0},
			}...)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			// buf := make([]interface{}, dt.ColLen())
			// dt.ForEach(func(i int, row datatable.DataRow) {
			// 	fmt.Printf("%v\n", row.Values(buf))
			// })
			// fmt.Printf("==================================\n")
		})
	}
}

func TestSortAndFilter2(t *testing.T) {
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
			datatable.DbgNoFastFilter = true

			ctx := runtime.NewRuntimeContext()

			ctx.Funcs["concat"] = scalar.Concat

			dt := datatable.NewDataTableWithSize(
				ctx,
				1000, 1000,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)
			dt.SetHeader([][]string{{"a"}, {"a"}, {"expr1"}})

			dt.ForEach(func(i int, row datatable.DataRow) bool {
				row.Set(0, types.NewNullable("foobar"))
				row.Set(1, types.NewNullable(uint16(63)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			err := dt.Filter([]types.FilterInfo{
				{Op: types.Op_LoadCol, Col: 0},
				{Op: types.Op_LoadImmediate, Param: "baz"},
				{Op: types.Op_Call, Param: "concat", NArgs: 2},
				{Op: types.Op_LoadImmediate, Param: "foobarbaz"},
				{Op: types.Op_Eq},
			}...)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if dt.Len() == 0 {
				t.Errorf("filter result len = %v", dt.Len())
				return
			}
		})
	}
}

func TestSortAndFilter3(t *testing.T) {
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
			datatable.DbgNoFastFilter = true

			ctx := runtime.NewRuntimeContext()

			dt := datatable.NewDataTableWithSize(
				ctx,
				1000, 1000,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)

			dt.ForEach(func(i int, row datatable.DataRow) bool {
				row.Set(0, types.NewNullable("foobar"))
				row.Set(1, types.NewNullable(uint16(63)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dt2, err := dt.Select([]types.SelectInfo{
				{Col: 0},
				{Col: 1},
				{Col: 2},
				{Col: 3},
			}...)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if dt2.Len() == 0 {
				t.Errorf("filter result len = %v", dt.Len())
				return
			}
		})
	}
}
