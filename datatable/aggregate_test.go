package datatable_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/function/aggregation"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func TestAgg1(t *testing.T) {
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

			ctx.Funcs["count"] = aggregation.Count

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
				if i%2 == 0 {
					row.Set(0, types.NewNullableAsNull[string]())
				} else {
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa(i%10)))
				}
				row.Set(1, types.NewNullable(1+i%10))
				row.Set(2, types.NewNullable(rand.Float64()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dt2, err := dt.Aggregate([]int{0, 1}, nil, []types.SelectInfo{
				{Col: 0},
				{Col: 1},
				{Formula: []types.FormulaInfo{
					{Op: types.Op_Call, Param: "count", NArgs: 0},
				}},
			}...)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if dt2.Len() != 505 {
				t.Errorf("filter result len = %v", dt.Len())
				return
			}
		})
	}
}

func TestAgg2(t *testing.T) {
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

			ctx.Funcs["count"] = aggregation.Count

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
				if i%2 == 0 {
					row.Set(0, types.NewNullableAsNull[string]())
				} else {
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa(i%10)))
				}
				row.Set(1, types.NewNullable(1+i%10))
				row.Set(2, types.NewNullable(rand.Float64()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dt2, err := dt.Aggregate([]int{0, 1}, nil, []types.SelectInfo{
				{Col: 0},
				{Col: 1},
				{Formula: []types.FormulaInfo{
					{Op: types.Op_LoadCol, Col: 0},
					{Op: types.Op_Call, Param: "count", NArgs: 1},
				}},
			}...)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if dt2.Len() != 505 {
				t.Errorf("filter result len = %v", dt.Len())
				return
			}
		})
	}
}

func TestAgg3(t *testing.T) {
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

			ctx.Funcs["count"] = aggregation.Count

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
				if i%2 == 0 {
					row.Set(0, types.NewNullableAsNull[string]())
				} else {
					row.Set(0, types.NewNullable("foobar"+strconv.Itoa(i%10)))
				}
				row.Set(1, types.NewNullable(1+i%10))
				row.Set(2, types.NewNullable(rand.Float64()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dt2, err := dt.Aggregate([]int{0, 1}, []types.FilterInfo{
				{Op: types.Op_LoadCol, Col: 0},
				{Op: types.Op_Call, Param: "count", NArgs: 1},
				{Op: types.Op_LoadImmediate, Param: int(1)},
				{Op: types.Op_Gt},
			}, []types.SelectInfo{
				{Col: 0},
				{Col: 1},
				{Formula: []types.FormulaInfo{
					{Op: types.Op_LoadCol, Col: 0},
					{Op: types.Op_Call, Param: "count", NArgs: 1},
				}},
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
