package types

import "github.com/guyvdb/dstore/store"

type TypeFactory func() store.Storable

// The registy is a type registry that records known types,
// tracks their namespaces and allocates Id's
type Registry interface {
	store.StoreTypeManager

	// Register with a Storable with the registry
	Register(typename string, factory TypeFactory)

	// Index a property
	Index(typeName string, propertyName string, indexType store.IndexType)

	// Create a concrete type of a Storable
	Instance(typeId int64) (store.Storable, error)

	// Allocate a new instance
	AllocateId(item store.Storable) error

	// Load additional information from a store
	Load(store store.Store) error

	// Save additional information about our types to a store
	//Store(store store.Store) error
}

// Global function to return the one and only registry
var registry Registry = nil

func GetRegistry() Registry {
	if registry == nil {
		registry = NewSystemRegistry()
	}
	return registry
}
