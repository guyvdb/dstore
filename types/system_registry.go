package types

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/guyvdb/dstore/fault"
	"github.com/guyvdb/dstore/store"
)

const REGISTRY_INFO_TYPE_NAME string = "RegistryInfo"
const REGISTRY_ITEM_TYPE_NAME string = "RegistryItem"

var _ Registry = (*SystemRegistry)(nil)
var _ store.StoreTypeManager = (*SystemRegistry)(nil)
var _ store.Storable = (*RegistryItem)(nil)
var _ store.Storable = (*RegistryInfo)(nil)

type RegistryInfo struct {
	Id           *store.Id `json:"id"`
	NextTypeId   int64     `json:"nextTypeId"`
	NextObjectId int64     `json:"nextObjectId"`
}

// Hardcoded type and object id's
const REGISTRY_INFO_TYPE_ID int64 = 1
const REGISTRY_INFO_OBJECT_ID int64 = 1
const REGISTRY_ITEM_TYPE_ID int64 = 2

type RegistryItem struct {
	Id           *store.Id                `json:"id"`           // The id of this RegistryItem
	TypeName     string                   `json:"typeName"`     // The string name of the type that this item represents
	TypeId       int64                    `json:"typeId"`       // The typeid of the type that this item represents
	NextObjectId int64                    `json:"nextObjectId"` // The next object id for this typeid
	Indexes      []*store.IndexDefinition `json:"indexes"`
	Factory      TypeFactory              `json:"-"`
}

// Registry implements the store.Registry interface.
// It provides a way to register types, create instances of those types,
// and manage their persistence.
type SystemRegistry struct {
	mu            sync.RWMutex // lock
	info          *RegistryInfo
	store         store.Store
	items         []*RegistryItem // all the types that we know about
	typeIdIndex   map[int64]*RegistryItem
	typeNameIndex map[string]*RegistryItem
}

// NewRegistry creates and returns a new Registry instance.
func NewSystemRegistry() *SystemRegistry {
	slog.Debug("NewSystemRegistry - create registry")
	return &SystemRegistry{
		items: make([]*RegistryItem, 0),
		//index: make(map[int64]*RegistryItem),
	}
}

func NewRegistryItem(typeName string, factory TypeFactory) *RegistryItem {
	return &RegistryItem{
		TypeName: typeName,
		Factory:  factory,
		Indexes:  make([]*store.IndexDefinition, 0),
	}
}

// Register registers a new type with the registry.
// It adds the typename and the factory during this phase of registation
// later, on Load() it will assign the typeId assigned to this typename
// or create a new typeid, if it is the first time seeing this type
func (r *SystemRegistry) Register(typename string, factory TypeFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()

	item := NewRegistryItem(typename, factory)
	r.items = append(r.items, item)
}

func (r *SystemRegistry) Index(typeName string, propertyName string, indexType store.IndexType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, item := range r.items {
		if item.TypeName == typeName {
			item.AddIndex(propertyName, indexType)
		}
	}
}

func (r *SystemRegistry) AllocateId(item store.Storable) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, found := r.typeNameIndex[item.GetTypeName()]
	if !found {
		return fault.ErrTypeNotFound
	}

	id := store.NewId(info.TypeId, info.NextObjectId)
	info.NextObjectId++

	slog.Debug("SystemRegistry.AllocateId() - allocate id", "typeName", item.GetTypeName(), "typeId", info.TypeId, "objectId", id.ObjectId, "id", id.String())
	item.SetId(id)

	err := r.store.Put(info)
	if err != nil {
		slog.Debug("error saving type information", "err", err)
		return err
	}

	return nil
}

// Instance creates a new instance of a Storable type given its typeId.
// It returns the created Storable instance or an error if the typeId is not found.
// A map[int64]*RegistryItem should be created at the end of the Load() method
// to for an index for typeId->factory
func (r *SystemRegistry) Instance(typeId int64) (store.Storable, error) {
	// special instance for RegistryInfo and RegistryItem
	if typeId == REGISTRY_INFO_TYPE_ID {
		return &RegistryInfo{}, nil
	}

	if typeId == REGISTRY_ITEM_TYPE_ID {
		return &RegistryItem{}, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Otherwise it is a user defined types
	info, found := r.typeIdIndex[typeId]
	if !found {
		return nil, fault.ErrTypeNotFound
	}

	return info.Factory(), nil
}

func (r *SystemRegistry) CreateInstance(typeId int64) (store.Storable, error) {
	return r.Instance(typeId)
}

func (r *SystemRegistry) GetTypeId(typeName string) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, found := r.typeNameIndex[typeName]
	if !found {
		return 0, fault.ErrTypeNotFound
	}
	return info.TypeId, nil
}

func (r *SystemRegistry) GetTypeName(typeId int64) (string, error) {

	// special instance for RegistryInfo and RegistryItem
	if typeId == REGISTRY_INFO_TYPE_ID {
		return "RegistryInfo", nil
	}

	if typeId == REGISTRY_ITEM_TYPE_ID {
		return "RegistryItem", nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	info, found := r.typeIdIndex[typeId]
	if !found {
		return "", fault.ErrTypeNotFound
	}
	return info.TypeName, nil
}

func (r *SystemRegistry) Indexes(typeId uint64) []*store.IndexDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, found := r.typeIdIndex[int64(typeId)]
	if !found {
		return []*store.IndexDefinition{}
	}

	return info.Indexes
}

// Load loads registry information from a store.
// The nextTypeId should be loaded first. Then for each
// RegistryItem the following should occur
//
//	Any previous typeId should be assigned back to this typename.
//	If no typeId exists, one should be allocated and nextTypeId should
//	be incremented.
//	The nextObjectId for this typeId should be retreived.
func (r *SystemRegistry) Load(s store.Store) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var info *RegistryInfo

	// save a reference to the store
	r.store = s

	// Allocate our buckets for RegistryInfo and RegistryItem
	s.AllocateBucketIfNeeded(REGISTRY_INFO_TYPE_NAME)
	s.AllocateBucketIfNeeded(REGISTRY_ITEM_TYPE_NAME)

	// The registry info
	infoId := store.NewId(REGISTRY_INFO_TYPE_ID, REGISTRY_INFO_OBJECT_ID)
	item, err := s.Get(infoId)

	if err != nil {
		slog.Debug("SystemRegistry.Load() - error getting registry info", "err", err)

		if err == fault.ErrKeyNotFound {
			// Create
			info = &RegistryInfo{
				Id:           infoId,
				NextTypeId:   1001,
				NextObjectId: 1001,
			}
			s.Put(info)
		} else {
			return err
		}
	} else {
		info = item.(*RegistryInfo)
	}

	r.info = info

	// All type info that we know about
	types, err := s.GetAll(REGISTRY_ITEM_TYPE_ID)
	if err != nil {
		return err
	}
	for _, t := range types {
		ri := t.(*RegistryItem)
		r.updateTypeInfo(ri)
	}

	// Check any types that may be new types
	for _, ri := range r.items {
		if ri.TypeId == 0 {
			slog.Debug("SystemRegistry.allocateNewType() - allocate new type", "typeName", ri.TypeName)
			r.allocateNewType(s, ri)
		}
	}

	// build an index of typeId -> *RegistryItem
	r.typeIdIndex = make(map[int64]*RegistryItem)
	r.typeNameIndex = make(map[string]*RegistryItem)
	for _, ri := range r.items {
		r.typeIdIndex[ri.TypeId] = ri
		r.typeNameIndex[ri.TypeName] = ri
	}

	return nil
}

func (r *SystemRegistry) allocateNewType(s store.Store, item *RegistryItem) {

	// assign a type id
	item.TypeId = r.info.NextTypeId
	r.info.NextTypeId++

	// assign an registry info id
	item.Id = store.NewId(REGISTRY_ITEM_TYPE_ID, r.info.NextObjectId)
	item.NextObjectId = 1
	r.info.NextObjectId++

	slog.Debug("Allocate indexes: ", "typeName", item.TypeName, "indexes", item.Indexes)

	// save the item to the store
	err := s.Put(item)
	if err != nil {
		panic(err)
	}

	// save the registry info to the store
	err = s.Put(r.info)
	if err != nil {
		panic(err)
	}

}

func (r *SystemRegistry) updateTypeInfo(item *RegistryItem) {
	for _, ri := range r.items {
		if ri.TypeName == item.TypeName {
			ri.Id = item.Id
			ri.TypeId = item.TypeId
			ri.NextObjectId = item.NextObjectId
			ri.Indexes = make([]*store.IndexDefinition, len(item.Indexes))
			copy(ri.Indexes, item.Indexes)
			if len(ri.Indexes) > 0 {
				for _, idx := range ri.Indexes {
					slog.Debug("Index: ", "typeName", ri.TypeName, "propertyName", idx.PropertyName, "type", idx.Type)
				}
			}
		}
	}
}

// GetId returns the Id of the RegistryItem.
func (ri *RegistryItem) GetId() *store.Id {
	return ri.Id
}

func (ri *RegistryItem) SetId(id *store.Id) {
	ri.Id = id
}

func (ri *RegistryItem) GetTypeName() string {
	return REGISTRY_ITEM_TYPE_NAME
}

func (ri *RegistryItem) AddIndex(propertyName string, indexType store.IndexType) {
	ri.Indexes = append(ri.Indexes, &store.IndexDefinition{
		PropertyName: propertyName,
		Type:         indexType,
	})
}

// Marshal serializes the RegistryItem to a byte slice.
func (ri *RegistryItem) Marshal() ([]byte, error) {
	return json.Marshal(ri)
}

// Unmarshal deserializes a byte slice into the RegistryItem.
func (ri *RegistryItem) Unmarshal(data []byte) error {
	return json.Unmarshal(data, ri)
}

func (ri *RegistryInfo) GetId() *store.Id {
	return store.NewId(REGISTRY_INFO_TYPE_ID, REGISTRY_INFO_OBJECT_ID)
}

func (ri *RegistryInfo) SetId(id *store.Id) {
	// noop
}

func (ri *RegistryInfo) GetTypeName() string {
	return REGISTRY_INFO_TYPE_NAME
}

func (ri *RegistryInfo) Marshal() ([]byte, error) {
	return json.Marshal(ri)
}

func (ri *RegistryInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, ri)
}

//
