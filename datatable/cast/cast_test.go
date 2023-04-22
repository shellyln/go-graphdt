package cast_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func Test0(t *testing.T) {
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
			if r := cast.GetType(reflect.TypeOf(types.Nullable[any]{})); r != types.Type_Nullable_Any {
				t.Errorf("GetType(Nullable[any]{}) failed: %v", r)
				return
			}
		})
	}
}

func Test1(t *testing.T) {
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
			{
				fn := cast.GetCastFunc(types.Type_Nullable_I64, types.Type_F64)
				r, ok := fn(types.NewNullable(1234))
				fmt.Printf("0a: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_F64)
				r, ok := fn(types.NewNullable(1234))
				fmt.Printf("0b: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Nullable_Any, types.Type_Nullable_F64)
				r, ok := fn(types.NewNullable(1234))
				fmt.Printf("0c: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Nullable_I64, types.Type_F64)
				r, ok := fn(types.NewNullable(int64(1234)))
				fmt.Printf("0d: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Nullable_I64, types.Type_F64)
				r, ok := fn(types.NewNullableAsNull[int64]())
				fmt.Printf("0e: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_I64, types.Type_Nullable_F64)
				r, ok := fn(int64(1234))
				fmt.Printf("0f: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Nullable_I64, types.Type_String)
				r, ok := fn(types.NewNullable(int64(1234)))
				fmt.Printf("1: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_DateTimeRange, types.Type_String)
				r, ok := fn(types.TimeRange{Start: time.Now(), End: time.Now()})
				fmt.Printf("2: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Nullable_DateTimeRange, types.Type_String)
				r, ok := fn(types.NewNullableAsNull[types.TimeRange]())
				fmt.Printf("3: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Nullable_DateTimeRange|types.Type_Flag_Array, types.Type_String|types.Type_Flag_Array)
				r, ok := fn([]types.Nullable[types.TimeRange]{types.NewNullable(types.TimeRange{Start: time.Now(), End: time.Now()}), types.NewNullableAsNull[types.TimeRange]()})
				fmt.Printf("4: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_String|types.Type_Flag_Array)
				r, ok := fn([]types.Nullable[types.TimeRange]{types.NewNullable(types.TimeRange{Start: time.Now(), End: time.Now()})})
				fmt.Printf("5: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_Nullable_String|types.Type_Flag_Array)
				r, ok := fn([]any{types.NewNullable(types.TimeRange{Start: time.Now(), End: time.Now()}), types.NewNullableAsNull[int64](), int32(1234), 5678})
				fmt.Printf("6a: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_Nullable_String|types.Type_Flag_Array)
				r, ok := fn([]any{types.NewNullable(types.TimeRange{Start: time.Now(), End: time.Now()}), nil, int32(1234)})
				fmt.Printf("6b: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_Any|types.Type_Flag_Array)
				r, ok := fn([]any{types.NewNullable(types.TimeRange{Start: time.Now(), End: time.Now()}), types.NewNullableAsNull[int64](), types.NewNullable[any](8765),
					nil, types.NewNullableAsNull[any](), int32(1234), 5678})
				fmt.Printf("7: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_Nullable_Any|types.Type_Flag_Array)
				r, ok := fn([]any{types.NewNullable(types.TimeRange{Start: time.Now(), End: time.Now()}), types.NewNullableAsNull[int64](), types.NewNullable[any](8765),
					nil, types.NewNullableAsNull[any](), int32(1234), 5678})
				fmt.Printf("8: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_String)
				r, ok := fn(nil)
				fmt.Printf("9: %v,%v\n", r, ok)
			}
			{
				fn := cast.GetCastFunc(types.Type_Any, types.Type_Nullable_String)
				r, ok := fn(nil)
				fmt.Printf("10: %v,%v\n", r, ok)
			}
		})
	}
}
