package types

// DescribeField is a struct for describing a field (or column).
type DescribeField struct {
	Name string
	Type DataColumnType
}

// DescribeRelation is a struct for describing a relationship.
type DescribeRelation struct {
	Name     string
	Target   *DescribeObject
	OneToOne bool
	Required bool
}

// DescribeObject is a struct for describing a object (or table).
type DescribeObject struct {
	Name      string
	Fields    []DescribeField
	Relations []DescribeRelation
}
