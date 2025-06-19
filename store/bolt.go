package store

import (
	"fmt"
	"log/slog"

	"reflect"

	"github.com/guyvdb/dstore/fault"

	"go.etcd.io/bbolt"
)

// BoltStore implements the store.Store interface using BoltDB.
var _ Store = (*BoltStore)(nil)

type BoltStore struct {
	db          *bbolt.DB
	typeManager StoreTypeManager
}

// NewBoltStore creates and returns a new BoltStore.
// It takes the path to the BoltDB file.
func NewBoltStore(path string, typeManager StoreTypeManager) (Store, error) {

	slog.Debug("NewBoltStore - create bolt store", "path", path)

	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt db: %w", err)
	}

	// Buckets for types will be created on demand.
	return &BoltStore{db: db, typeManager: typeManager}, nil
}

// Put stores a Storable model.
func (bs *BoltStore) Put(m Storable) error {
	if m == nil {
		return fault.ErrNilStoreable
	}

	id := m.GetId()
	if id == nil {
		return fault.ErrStorableHasNilId
	}

	bucketNameBytes, err := bs.typeBucketKey(id.TypeId)
	if err != nil {
		return err
	}

	data, err := m.Marshal()
	if err != nil {
		return fault.ErrMarshalFailed
	}

	keyBytes := []byte(id.String())

	return bs.db.Update(func(tx *bbolt.Tx) error {

		// Put the storeable
		bucket, err := tx.CreateBucketIfNotExists(bucketNameBytes)
		if err != nil {
			return fault.ErrBucketCreateFailed
		}

		err = bucket.Put(keyBytes, data)
		if err != nil {
			return fault.ErrPutFailed
		}

		// Update any indexes
		for _, index := range bs.typeManager.Indexes(uint64(id.TypeId)) {
			indexBucketNameBytes, err := bs.mkIndexBucketName(id.TypeId, index.PropertyName)
			if err != nil {
				return err
			}
			slog.Debug("BoltStore.Put()", "indexName", string(indexBucketNameBytes))
			idxbucket, err := tx.CreateBucketIfNotExists(indexBucketNameBytes)
			if err != nil {
				return fault.ErrBucketCreateFailed
			}

			// Use reflection to get the value of the PropertyName from m (Storable)
			v := reflect.ValueOf(m)
			// If m is a pointer, dereference it
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			// Ensure we are dealing with a struct
			if v.Kind() != reflect.Struct {
				//slog.Warn("Indexed item is not a struct", "typeName", typeName, "index", index.PropertyName)
				// Decide how to handle this - maybe skip indexing for this item?
				continue
			}

			field := v.FieldByName(index.PropertyName)

			// Check if the field exists and is exportable
			if !field.IsValid() || !field.CanInterface() {
				//slog.Warn("Indexed property not found or not exportable", "typeName", typeName, "index", index.PropertyName)
				// Decide how to handle this - maybe skip indexing for this property?
				continue
			}

			// Check if the field is a string (as per current index methods)
			if field.Kind() != reflect.String {
				//slog.Warn("Indexed property is not a string", "typeName", typeName, "index", index.PropertyName, "kind", field.Kind())
				// Decide how to handle this - maybe skip indexing for this property?
				continue
			}

			propertyValue := field.String()
			idBytes := []byte(id.String())

			// Store in index bucket
			// For non-unique, use a composite key propertyValue_objectId to ensure key uniqueness in BoltDB
			// For unique, use propertyValue as key and check for conflicts
			indexKey := buildIndexKey(index.Type, propertyValue, id)

			// TODO: Add uniqueness check for UniqueIndex before putting
			idxbucket.Put(indexKey, idBytes)
		}

		return nil
	})
}

// PutAll stores multiple Storable models.
func (bs *BoltStore) PutAll(m []Storable) error {
	if len(m) == 0 {
		return nil // Nothing to do
	}

	return bs.db.Update(func(tx *bbolt.Tx) error {
		for _, item := range m {
			if item == nil {
				return fault.ErrNilStoreable
			}

			id := item.GetId()
			if id == nil {
				return fault.ErrStorableHasNilId
			}

			// typeName, err := bs.typeManager.GetTypeName(id.TypeId)
			// if err != nil {
			// 	return fault.ErrTypeNotFound
			// }
			// bucketNameBytes := []byte(typeName)
			bucketNameBytes, err := bs.typeBucketKey(id.TypeId)
			if err != nil {
				return err
			}

			data, err := item.Marshal()
			if err != nil {
				return fault.ErrMarshalFailed
			}

			bucket, err := tx.CreateBucketIfNotExists(bucketNameBytes)
			if err != nil {
				return fault.ErrBucketCreateFailed
			}

			keyBytes := []byte(id.String())
			if err := bucket.Put(keyBytes, data); err != nil {
				return fault.ErrPutFailed
			}
		}
		return nil
	})
}

// Exists checks if a model with the given Id exists.
func (bs *BoltStore) Exists(id *Id) (bool, error) {
	if id == nil {
		return false, fault.ErrIdIsNil
	}

	// typeName, err := bs.typeManager.GetTypeName(id.TypeId)
	// if err != nil {
	// 	// If typeId is unknown, we can't determine the bucket.
	// 	return false, fault.ErrTypeNotFound
	// }
	// bucketNameBytes := []byte(typeName)
	bucketNameBytes, err := bs.typeBucketKey(id.TypeId)
	if err != nil {
		return false, err
	}

	keyBytes := []byte(id.String())
	var exists bool

	err = bs.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketNameBytes)
		if bucket == nil {
			// Bucket for this type does not exist, so key cannot exist.
			exists = false
			return nil // Not an error for Exists operation
		}
		val := bucket.Get(keyBytes)
		exists = (val != nil)
		return nil
	})

	// err here would be a DB-level error from bs.db.View, not from bucket/key not found.
	// If err is not nil, 'exists' value is indeterminate, so return error.
	return exists, err
}

// Get retrieves a Storable model by its key.
func (bs *BoltStore) Get(id *Id) (Storable, error) {
	var result Storable

	if id == nil {
		return nil, fault.ErrIdIsNil
	}

	// typeName, err := bs.typeManager.GetTypeName(id.TypeId)
	// if err != nil {
	// 	return nil, fault.ErrTypeNotFound
	// }
	// bucketNameBytes := []byte(typeName)
	bucketNameBytes, err := bs.typeBucketKey(id.TypeId)
	if err != nil {
		return nil, err
	}

	slog.Debug("BoltStore.Get() - get item", "id", id, "bucketName", string(bucketNameBytes))

	keyBytes := []byte(id.String())

	err = bs.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketNameBytes)
		if bucket == nil {
			return fault.ErrBucketNotFound
		}

		val := bucket.Get(keyBytes)
		if val == nil {
			return fault.ErrKeyNotFound
		}

		instance, createErr := bs.typeManager.CreateInstance(id.TypeId)
		if createErr != nil {
			return fault.ErrTypeNotCreated
		}

		if unmarshalErr := instance.Unmarshal(val); unmarshalErr != nil {
			return fault.ErrUnmarshalFailed
		}
		result = instance
		return nil
	})

	if err != nil {
		// err could be from db.View, bucket not found, key not found, create instance, or unmarshal.
		return nil, err
	}

	return result, nil
}

// GetAllByTypeName retrieves all Storable models of a given typeName.
func (bs *BoltStore) GetAllByTypeName(typeName string) ([]Storable, error) {
	typeId, err := bs.typeManager.GetTypeId(typeName)
	if err != nil {
		return nil, fault.ErrTypeNotFound
	}
	// Use typeName directly as the bucket name. Pass typeId for unmarshalling.
	return bs.getAllFromBucket(typeId)
}

// GetAll retrieves all Storable models of a given typeId.
func (bs *BoltStore) GetAll(typeId int64) ([]Storable, error) {
	// typeName, err := bs.typeManager.GetTypeName(typeId)
	// if err != nil {
	// 	return nil, fault.ErrTypeNotFound
	// }
	// Use typeName as the bucket name. Pass typeId for unmarshalling.
	return bs.getAllFromBucket(typeId)
}

// getAllFromBucket is a helper to retrieve all items from a named bucket,
// unmarshalling them as the given typeId.
func (bs *BoltStore) getAllFromBucket( /*bucketName string,*/ typeId int64) ([]Storable, error) {
	var results []Storable
	//bucketNameBytes := []byte(bucketName)

	bucketNameBytes, err := bs.typeBucketKey(typeId)
	if err != nil {
		return nil, err
	}

	err = bs.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketNameBytes)
		if bucket == nil {
			// If the bucket doesn't exist, there are no items of this type.
			// This is not an error condition for GetAll operations.
			return nil
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			instance, err := bs.typeManager.CreateInstance(typeId)
			if err != nil {
				// This error means the factory doesn't know how to create this typeId.
				return fault.ErrTypeNotCreated
			}

			// Value 'v' is only valid for the lifetime of the transaction.
			// We must copy it before Unmarshal if Unmarshal might hold onto the slice.
			// json.Unmarshal typically copies data, but being explicit is safer.
			valueBytes := make([]byte, len(v))
			copy(valueBytes, v)

			if err := instance.Unmarshal(valueBytes); err != nil {
				return fault.ErrUnmarshalFailed
			}
			results = append(results, instance)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Ensure empty slice instead of nil if no items found and no error.
	if results == nil {
		results = make([]Storable, 0)
	}
	return results, nil
}

// buildIndexKey creates the key for the index bucket based on index type.
// For UniqueIndex, the key is the property value.
// For NonUniqueIndex, the key is propertyValue_objectId to allow multiple items
// with the same property value while maintaining unique keys in BoltDB.
func buildIndexKey(indexType IndexType, propertyValue string, id *Id) []byte {
	switch indexType {
	case UniqueIndex:
		return []byte(propertyValue)
	case NonUniqueIndex:
		return []byte(fmt.Sprintf("%s_%s", propertyValue, id.String()))
	}
	return nil // Should not happen with current IndexType values
}

// Delete removes a model by its key.
func (bs *BoltStore) Delete(id *Id) error {
	panic("not implemented")
}

func (bs *BoltStore) AllocateId(item Storable) error {
	return bs.typeManager.AllocateId(item)
}

func (bs *BoltStore) StringExactMatch(indexName string, value string) (Storable, error) {
	panic("not implemented")
}

func (bs *BoltStore) StringWildcardMatch(indexName string, value string) ([]Storable, error) {
	panic("not implemented")
}

func (bs *BoltStore) AllocateBucketIfNeeded(typeName string) error {
	var bucketNameBytes []byte

	if typeName == "RegistryInfo" || typeName == "RegistryItem" {
		// hardcode these values
		bucketNameBytes = []byte("Type." + typeName)
	} else {
		// lookup the name
		typeId, err := bs.typeManager.GetTypeId(typeName)
		if err != nil {
			fmt.Printf("AllocateBucketIfNeeded() - type not found: typeName=%s\n", typeName)
			return fault.ErrTypeNotFound
		}

		bucketNameBytes, err = bs.typeBucketKey(typeId)
		if err != nil {
			fmt.Printf("AllocateBucketIfNeeded() - error creating bucket name\n")
			return err
		}

	}

	return bs.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketNameBytes)
		return err
	})
}

func (bs *BoltStore) typeBucketKey(typeId int64) ([]byte, error) {
	typeName, err := bs.typeManager.GetTypeName(typeId)
	if err != nil {
		return []byte{}, fault.ErrTypeNotFound
	}

	return []byte("Type." + typeName), nil
}

func (bs *BoltStore) mkIndexBucketName(typeId int64, propertyName string) ([]byte, error) {
	typeName, err := bs.typeManager.GetTypeName(typeId)
	if err != nil {
		return []byte{}, fault.ErrTypeNotFound
	}
	return []byte("Index." + typeName + "." + propertyName), nil
}

// Close closes the BoltDB database.
func (bs *BoltStore) Close() error {
	slog.Debug("BoltStore.Close() - close db")
	if bs.db != nil {
		return bs.db.Close()
	}
	return nil
}
