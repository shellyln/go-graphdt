package datatable_test

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/shellyln/go-graphdt/datatable"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	"github.com/shellyln/go-graphdt/datatable/types"
)

//go:embed _testdata/test1.csv
var test1CsvBytes []byte

func TestReadEmbedCsv1(t *testing.T) {
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
			buf := bytes.NewReader(test1CsvBytes)
			reader := csv.NewReader(buf)

			ctx := runtime.NewRuntimeContext()
			dt := datatable.NewDataTableWithSize(
				ctx,
				0, 0,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
				types.Type_Nullable_String,
			)
			dt.SetHeader(nil)
			if err := dt.AppendFromCSV(reader, datatable.CSVOptions{HasHeader: true}); err != nil {
				t.Errorf("error = %v", err)
				return
			}

			w := bufio.NewWriter(os.Stdout)
			defer w.Flush()

			names := make([][]string, dt.ColLen())
			values := make([]interface{}, dt.ColLen())
			dt.ForEach(func(i int, row datatable.DataRow) bool {
				fmt.Fprintln(w, row.Names(names))
				return true
			})
			dt.ForEach(func(i int, row datatable.DataRow) bool {
				// t.Logf("row = %v", row)
				fmt.Fprintf(w, "row[%d] = %v\n", i, row.Values(values))
				return false
			})

			var wbuf bytes.Buffer
			writer := csv.NewWriter(&wbuf)
			if err := dt.WriteToCSV(writer, datatable.CSVOptions{HasHeader: true}); err != nil {
				t.Errorf("error = %v", err)
				return
			}
			writer.Flush()

			csvout := strings.Split(wbuf.String(), "\n")
			for i, s := range csvout {
				fmt.Fprintln(w, i, s)
			}
		})
	}
}

func TestReadEmbedCsv2(t *testing.T) {
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
			buf := bytes.NewReader(test1CsvBytes)
			reader := csv.NewReader(buf)

			ctx := runtime.NewRuntimeContext()

			dt, err := datatable.NewDataTableFromCSV(ctx, reader, datatable.CSVOptions{HasHeader: true, Namespace: []string{"Asdf"}})
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}

			w := bufio.NewWriter(os.Stdout)
			defer w.Flush()

			names := make([][]string, dt.ColLen())
			values := make([]interface{}, dt.ColLen())
			dt.ForEach(func(i int, row datatable.DataRow) bool {
				fmt.Fprintln(w, row.Names(names))
				return true
			})
			dt.ForEach(func(i int, row datatable.DataRow) bool {
				// t.Logf("row = %v", row)
				fmt.Fprintf(w, "row[%d] = %v\n", i, row.Values(values))
				return false
			})
		})
	}
}
