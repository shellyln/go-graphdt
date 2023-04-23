package datatable

import (
	"github.com/shellyln/go-nameutil/nameutil"
)

func (p *DataTable) GetDescribe() *DescribeObject {
	names := make([]string, p.ColLen())
	rootRel := p.RootObjectsRowRange()
	names = rootRel.SimpleNames(names)

	objFromRel := func(rel DataRelation) *DescribeObject {
		names = rel.SimpleNames(names)
		fields := make([]DescribeField, len(rel.cols))

		for i := range fields {
			fields[i].Name = names[i]
			fields[i].Type = p.cols[rel.cols[i]].GetType()
		}

		return &DescribeObject{
			Fields:    fields,
			Relations: []DescribeRelation{},
		}
	}

	var fn func(parent *DescribeObject) func(i int, rel DataRelation) bool
	fn = func(parent *DescribeObject) func(i int, rel DataRelation) bool {
		return func(i int, rel DataRelation) bool {
			obj := objFromRel(rel)

			relName := ""
			if nameutil.IsValidName(p.relations[rel.relIndex].namespace) {
				relName = p.relations[rel.relIndex].namespace[len(p.relations[rel.relIndex].namespace)-1]
			}

			parent.Relations = append(parent.Relations, DescribeRelation{
				Name:     relName,
				Target:   obj,
				OneToOne: p.relations[rel.relIndex].OneToOne,
				Required: p.relations[rel.relIndex].Required,
			})

			return rel.ForEachChildRelationDescribes(fn(obj))
		}
	}

	rootObj := objFromRel(*rootRel.DataRelation)
	rootRel.ForEachChildRelationDescribes(fn(rootObj))

	return rootObj
}
