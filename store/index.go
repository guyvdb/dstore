package store

type IndexType int

const (
	UniqueIndex IndexType = iota
	NonUniqueIndex
)

//var _ Index = (*IndexDefinition)(nil)

type IndexDefinition struct {
	PropertyName string    `json:"propertyName"`
	Type         IndexType `json:"type"`
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
