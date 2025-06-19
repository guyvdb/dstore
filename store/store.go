package store

type Storable interface {
	GetId() *Id
	SetId(id *Id)
	GetTypeName() string
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

type StoreTypeManager interface {
	CreateInstance(typeId int64) (Storable, error)
	GetTypeId(typeName string) (int64, error)
	GetTypeName(typeId int64) (string, error)
	AllocateId(item Storable) error
	Indexes(typeId uint64) []*IndexDefinition
}

type Store interface {
	Put(m Storable) error
	PutAll(m []Storable) error
	Exists(id *Id) (bool, error)
	Get(id *Id) (Storable, error)
	GetAll(typeId int64) ([]Storable, error)
	GetAllByTypeName(typeName string) ([]Storable, error)
	Delete(id *Id) error
	AllocateId(item Storable) error
	AllocateBucketIfNeeded(typeName string) error

	// Indexed Searches
	// indexName is in the form of TypeName.PropertyName (e.g., "Product.BarCode").

	// Match finds storables where an indexed property exactly matches the given value.
	// The type of 'value' should correspond to the IndexDefinition.DataType of the indexed property.
	// indexName is in the form of TypeName.PropertyName.
	Match(indexName string, value interface{}) ([]Storable, error)

	// WildcardMatch finds storables where an indexed string property matches the given wildcard pattern.
	// indexName is in the form of TypeName.PropertyName.
	// The property must be indexed and of type StringIndex.
	WildcardMatch(indexName string, pattern string) ([]Storable, error)

	Close() error
}
