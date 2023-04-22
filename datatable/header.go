package datatable

import (
	"fmt"
	"strconv"

	"github.com/shellyln/go-graphdt/nameutil"
)

// Name the colums with namespaces.
func (p *DataTable) SetHeader(header [][]string) error {
	dict := make(map[string]int)
	for i, hdr := range header {
		if len(hdr) == 0 || len(hdr) == 1 && hdr[0] == "" {
			continue
		}
		key := nameutil.MakeDottedKeyIgnoreCase(hdr, len(hdr))
		if _, ok := dict[key]; !ok {
			dict[key] = i
		}
	}

	z := make([][]string, len(p.cols))
	for i, c := 0, 0; i < len(p.cols); i++ {
		var name []string
		var isUnnamed bool
		if i < len(header) && len(header[i]) > 0 {
			if len(header[i]) == 1 && header[i][0] == "" {
				isUnnamed = true
			} else {
				name = make([]string, len(header[i]))
				copy(name, header[i])
			}
		} else {
			isUnnamed = true
		}

		if isUnnamed {
			name = []string{fmt.Sprintf("expr%d", c)}
			c++
		}

		key := nameutil.MakeDottedKeyIgnoreCase(name, len(name))
		if p, ok := dict[key]; p != i && ok {
			if isUnnamed {
				for ok {
					name = []string{fmt.Sprintf("expr%d", c)}
					c++
					key = nameutil.MakeDottedKeyIgnoreCase(name, len(name))
					_, ok = dict[key]
				}
			} else {
				basename := make([]string, len(name))
				copy(basename, name)
				for j := 1; ok; j++ {
					name[len(basename)-1] = basename[len(basename)-1] + "_" + strconv.Itoa(j)
					key = nameutil.MakeDottedKeyIgnoreCase(name, len(name))
					_, ok = dict[key]
				}
			}
		}
		dict[key] = i
		z[i] = name
	}

	for i := 0; i < len(z); i++ {
		p.header[i].name = z[i]
	}
	return nil
}

// Name the columns.
func (p *DataTable) SetSimpleHeader(header []string) error {
	z := make([][]string, len(p.cols))
	for i := 0; i < len(p.cols); i++ {
		if i < len(header) {
			z[i] = []string{header[i]}
		} else {
			z[i] = []string{""}
		}
	}
	return p.SetHeader(z)
}
