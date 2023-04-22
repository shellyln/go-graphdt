package datatable

import (
	"github.com/shellyln/go-loose-json-parser/marshal"
)

// Unmarshal the DataTable to the specified type array.
func (p *DataTable) Unmarshal(to interface{}) error {
	ar := make([]interface{}, 0)

	p.ToUntyped(func(i int, record map[string]interface{}) bool {
		ar = append(ar, record)
		return false
	})

	return marshal.Unmarshal(ar, to, nil)
}
