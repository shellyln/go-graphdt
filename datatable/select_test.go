package datatable_test

import (
	"math/rand"
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/function/scalar"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func TestSelect1(t *testing.T) {
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

			dt.ForEach(func(i int, row datatable.DataRow) bool {
				row.Set(0, types.NewNullable("foobar"))
				row.Set(1, types.NewNullable(uint16(63)))
				row.Set(2, types.NewNullable(rand.Float32()))
				row.Set(5, types.NewNullable("foo;bar;baz;qux;quux"))
				return false
			})

			dt2, err := dt.Select([]types.SelectInfo{
				{Col: 0},
				{Formula: []types.FormulaInfo{
					{Op: types.Op_LoadCol, Col: 0},
					{Op: types.Op_LoadImmediate, Param: "baz"},
					{Op: types.Op_Call, Param: "concat", NArgs: 2},
				}},
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
