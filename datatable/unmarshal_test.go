package datatable_test

import (
	"bytes"
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

func TestUnmarshal1(t *testing.T) {
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

	type tableGraults struct {
		FieldF1 string `json:"fieldF1"`
		FieldF2 string `json:"fieldF2"`
		FieldF3 string `json:"fieldF3"`
		FieldF4 string `json:"fieldF4"`
	}
	type tableCorge struct {
		FieldE1 string `json:"fieldE1"`
		FieldE2 string `json:"fieldE2"`
		FieldE3 string `json:"fieldE3"`
		FieldE4 string `json:"fieldE4"`
	}
	type tableQuux struct {
		FieldD1 string         `json:"fieldD1"`
		FieldD2 string         `json:"fieldD2"`
		FieldD3 string         `json:"fieldD3"`
		FieldD4 string         `json:"fieldD4"`
		Corges  []tableCorge   `json:"corges"`
		Graults []tableGraults `json:"graults"`
	}
	type tableQux struct {
		FieldC1 string `json:"fieldC1"`
		FieldC2 string `json:"fieldC2"`
		FieldC3 string `json:"fieldC3"`
		FieldC4 string `json:"fieldC4"`
	}
	type tableBar struct {
		FieldB1 string     `json:"fieldB1"`
		FieldB2 string     `json:"fieldB2"`
		FieldB3 string     `json:"fieldB3"`
		FieldB4 string     `json:"fieldB4"`
		Quxes   []tableQux `json:"quxes"`
	}
	type tableFoo struct {
		FieldA1 *string     `json:"fieldA1"`
		FieldA2 *string     `json:"fieldA2"`
		FieldA3 *string     `json:"fieldA3"`
		FieldA4 *string     `json:"fieldA4"`
		Bar     []tableBar  `json:"bar"`
		Quuxes  []tableQuux `json:"quuxes"`
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(test1JsonBytes)
			ctx := runtime.NewRuntimeContext()

			dtFoo := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
			)
			dtFoo.SetHeader([][]string{
				{"Foo", "fieldA1"},
				{"Foo", "fieldA2"},
				{"Foo", "fieldA3"},
				{"Foo", "fieldA4"},
			})

			dtBar := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
			)
			dtBar.SetHeader([][]string{
				{"Bar", "fieldB1"},
				{"Bar", "fieldB2"},
				{"Bar", "fieldB3"},
				{"Bar", "fieldB4"},
			})

			dtQux := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
			)
			dtQux.SetHeader([][]string{
				{"Qux", "fieldC1"},
				{"Qux", "fieldC2"},
				{"Qux", "fieldC3"},
				{"Qux", "fieldC4"},
			})

			dtQuux := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
			)
			dtQuux.SetHeader([][]string{
				{"Quux", "fieldD1"},
				{"Quux", "fieldD2"},
				{"Quux", "fieldD3"},
				{"Quux", "fieldD4"},
			})

			dtCorge := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
			)
			dtCorge.SetHeader([][]string{
				{"Corge", "fieldE1"},
				{"Corge", "fieldE2"},
				{"Corge", "fieldE3"},
				{"Corge", "fieldE4"},
			})

			dtGrault := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
			)
			dtGrault.SetHeader([][]string{
				{"Grault", "fieldF1"},
				{"Grault", "fieldF2"},
				{"Grault", "fieldF3"},
				{"Grault", "fieldF4"},
			})

			dtFoo2, err := dtFoo.LeftJoin(0, dtBar, 0, "bar", datatable.JoinOptions{}) // foo | bar
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}

			dtFoo3, err := dtFoo2.LeftJoin(4, dtQux, 0, "quxes", datatable.JoinOptions{}) // foo | bar | qux
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}

			dtFoo4, err := dtFoo3.LeftJoin(0, dtQuux, 0, "quuxes", datatable.JoinOptions{}) // foo | bar | qux | quux
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}

			dtFoo5, err := dtFoo4.LeftJoin(12, dtCorge, 0, "corges", datatable.JoinOptions{}) // foo | bar | qux | quux | corge
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}

			dtFoo6, err := dtFoo5.LeftJoin(12, dtGrault, 0, "graults", datatable.JoinOptions{}) // foo | bar | qux | quux | corge | grault
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}

			if err := dtFoo6.AppendFromJSONLines(buf); err != nil {
				t.Errorf("error = %v", err)
				return
			}

			var dstArray []tableFoo
			if err := dtFoo6.Unmarshal(&dstArray); err != nil {
				t.Errorf("error = %v", err)
				return
			}

			t.Log(dstArray)
		})
	}
}
