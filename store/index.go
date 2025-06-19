package store

type IndexType int
type IndexDataType int

const (
	UniqueIndex IndexType = iota
	NonUniqueIndex
)

// IndexDataType specifies the underlying data type of the indexed property.
const (
	StringIndex IndexDataType = iota
	Int64Index
	Float64Index
	BoolIndex
	DateTimeIndex
)

//var _ Index = (*IndexDefinition)(nil)

type IndexDefinition struct {
	PropertyName string        `json:"propertyName"`
	Type         IndexType     `json:"type"`
	DataType     IndexDataType `json:"dataType"`
}

func (it IndexType) String() string {
	return [...]string{"Unique", "NonUnique"}[it]
}

/*
func (id *IndexDefinition) GetPropertyName() string {
	return id.PropertyName
}

func (id *IndexDefinition) GetIndexType() IndexType {
	return id.Type
}*/

// String returns the string representation of IndexDataType.
func (idt IndexDataType) String() string {
	return [...]string{"String", "Int64", "Float64", "Bool", "DateTime"}[idt]
}
