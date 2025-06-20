package dyno

import (
	"encoding/json"

	"github.com/guyvdb/dstore/store"
	"github.com/guyvdb/dstore/types"
)

const DYNAMIC_OBJECT_TYPE_NAME string = "DynamicObject"

var _ store.Storable = (*DynamicObject)(nil)

type DynamicObject struct {
	Id          *store.Id              `json:"id"`
	DynamicType string                 `json:"dynamicType"`
	Properties  map[string]interface{} `json:"properties"`
}

func init() {
	types.GetRegistry().Register(DYNAMIC_OBJECT_TYPE_NAME, dynamicObjectFactory)
}

func dynamicObjectFactory() store.Storable {
	return &DynamicObject{}
}

func NewDynamicObject(dynamicTypeName string) *DynamicObject {
	return &DynamicObject{
		DynamicType: dynamicTypeName,
		Properties:  make(map[string]interface{}),
	}
}

// GetId returns the Id of the DynamicObject.
func (do *DynamicObject) GetId() *store.Id {
	return do.Id
}

// SetId sets the Id of the DynamicObject.
func (do *DynamicObject) SetId(id *store.Id) {
	do.Id = id
}

// GetTypeName returns the type name of the DynamicObject.
func (do *DynamicObject) GetTypeName() string {
	return DYNAMIC_OBJECT_TYPE_NAME
}

// Marshal serializes the DynamicObject to a byte slice.
func (do *DynamicObject) Marshal() ([]byte, error) {
	return json.Marshal(do)
}

// Unmarshal deserializes a byte slice into the DynamicObject.
func (do *DynamicObject) Unmarshal(data []byte) error {
	return json.Unmarshal(data, do)
}

func (do *DynamicObject) SetProperty(name string, value interface{}) {
	do.Properties[name] = value
}

func (do *DynamicObject) GetProperty(name string) interface{} {
	return do.Properties[name]
}
