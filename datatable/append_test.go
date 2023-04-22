package datatable_test

import (
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func TestAppend1(t *testing.T) {
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

			dt := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_I64,
				types.Type_Nullable_F64,
				types.Type_Nullable_I64,
				types.Type_Nullable_I64,
				types.Type_Nullable_String,
			)

			dt.Append(func(i int, row datatable.DataRow) bool {
				if i >= 2048 {
					return true
				}

				row.Set(1, i)
				return false
			})
			if dt.Len() != 2048 {
				t.Errorf("filter result len = %v", dt.Len())
				return
			}
		})
	}
}
