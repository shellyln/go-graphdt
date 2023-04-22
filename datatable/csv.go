package datatable

import (
	"encoding/csv"
	"errors"
	"io"

	"github.com/shellyln/go-graphdt/datatable/cast"
	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/runtime"
	. "github.com/shellyln/go-graphdt/datatable/types"
)

type CSVOptions struct {
	HasHeader        bool
	MapColumnsByName bool
	Namespace        []string
	// TODO: null treating
}

// NewDataTableFromCSV creates a new data table from the CSV reader.
func NewDataTableFromCSV(rtCtx *runtime.RuntimeContext, reader *csv.Reader, options CSVOptions) (*DataTable, error) {
	record, err := reader.Read()
	if err == io.EOF {
		return nil, errors.New("NewDataTableFromCSV: No data")
	} else if err != nil {
		return nil, err
	}
	if len(record) == 0 {
		return nil, errors.New("NewDataTableFromCSV: First row is empty")
	}

	types := make([]DataColumnType, len(record))
	for i := 0; i < len(types); i++ {
		types[i] = Type_String
	}

	dt := NewDataTableWithSize(rtCtx, 0, datacolimpl.DataColumnImpl_DefaultSize, types...)
	if options.HasHeader {
		if options.Namespace != nil {
			complexNames := make([][]string, len(record))
			for i := 0; i < len(complexNames); i++ {
				complexNames[i] = make([]string, len(options.Namespace)+1)
				copy(complexNames[i], options.Namespace)
				complexNames[i][len(options.Namespace)] = record[i]
			}
			dt.SetHeader(complexNames)
		} else {
			dt.SetSimpleHeader(record)
		}
	} else {
		dt.SetHeader(nil)
	}

	opt2 := options
	opt2.HasHeader = false

	err = dt.AppendFromCSV(reader, opt2)
	if err != nil {
		return nil, err
	}

	return dt, nil
}

// AppendFromCSV appends the data from the CSV reader.
// TODO: null treating
func (p *DataTable) AppendFromCSV(reader *csv.Reader, options CSVOptions) error {
	var errRet error
	var header []string
	var err error

	colLen := p.ColLen()

	if options.HasHeader {
		header, err = reader.Read()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}

	if options.HasHeader && options.MapColumnsByName {
		dtNameMap := make(map[string]int)
		for i := range p.header {
			if len(p.header[i].name) > 0 {
				nm := p.header[i].name[len(p.header[i].name)-1]
				if _, ok := dtNameMap[nm]; !ok {
					dtNameMap[nm] = i
				}
			}
		}

		csvCols := make([]int, colLen)
		for i := 0; i < colLen; i++ {
			if i >= len(header) {
				csvCols[i] = -1
			}
			if c, ok := dtNameMap[header[i]]; ok {
				csvCols[i] = c
			} else {
				csvCols[i] = -1
			}
		}

		p.Append(func(i int, row DataRow) bool {
			record, err := reader.Read()
			if err == io.EOF {
				return true
			} else if err != nil {
				errRet = err
				return true
			}

			recLen := len(record)
			for c := 0; c < colLen && c < recLen; c++ {
				col := csvCols[c]
				if col >= 0 {
					row.Set(col, record[c])
				}
			}
			return false
		})
	} else {
		p.Append(func(i int, row DataRow) bool {
			record, err := reader.Read()
			if err == io.EOF {
				return true
			} else if err != nil {
				errRet = err
				return true
			}

			recLen := len(record)
			for c := 0; c < colLen && c < recLen; c++ {
				row.Set(c, record[c])
			}
			return false
		})
	}

	return errRet
}

// WriteToCSV writes the data table to the CSV writer.
// TODO: null treating
func (p *DataTable) WriteToCSV(writer *csv.Writer, options CSVOptions) error {
	length := p.index.Len()
	colLen := p.ColLen()

	values := make([]interface{}, colLen)
	record := make([]string, colLen)

	ctx := NewDataIterContextWithCache(p)
	rel := DataRelation{DataIterContext: ctx, cols: nil, relIndex: -1}
	rr := DataRowRange{DataRelation: &rel, start: 0, end: 0}

	if options.HasHeader {
		names := make([]string, colLen)
		names = rel.SimpleNames(names)
		writer.Write(names)
	}

	castFuncs := make([]CastFunc, colLen)
	for c := 0; c < colLen; c++ {
		castFuncs[c] = cast.GetCastFunc(p.cols[c].GetType(), Type_String)
	}

	for i := 0; i < length; i++ {
		row := DataRow{DataRowRange: rr, row: p.index.Get(i)}
		vals := row.Values(values)
		for c, v := range vals {
			if cv, ok := castFuncs[c](v); ok {
				record[c] = cv.(string)
			}
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}
