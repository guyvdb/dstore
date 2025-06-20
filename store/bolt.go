package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

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

func (bs *BoltStore) updateIndexes(tx *bbolt.Tx, m Storable) error {

	id := m.GetId()

	// Update any indexes
	for _, index := range bs.typeManager.Indexes(id.TypeId) {
		indexBucketNameBytes, err := bs.mkIndexBucketName(id.TypeId, index.PropertyName)
		if err != nil {
			return err
		}
		slog.Debug("BoltStore.Put() indexing property", "indexBucketName", string(indexBucketNameBytes), "propertyName", index.PropertyName, "dataType", index.DataType.String())

		typeNameForLog, _ := bs.typeManager.GetTypeName(id.TypeId) // Best effort for logging

		idxbucket, err := tx.CreateBucketIfNotExists(indexBucketNameBytes)
		if err != nil {
			return fmt.Errorf("failed to create index bucket %s: %w", string(indexBucketNameBytes), fault.ErrBucketCreateFailed)
		}

		var propertyValueBytes []byte
		var ok bool

		switch index.DataType {
		case StringIndex:
			stringValue, success := GetIndexableStringValue(m, typeNameForLog, index.PropertyName)
			if success {
				propertyValueBytes = []byte(stringValue)
			}
			ok = success
		case Int64Index:
			intValue, success := GetIndexableIntValue(m, typeNameForLog, index.PropertyName)
			if success {
				// XOR with (1 << 63) to make signed int64 lexicographically sortable
				// Negative numbers become 0..., positive numbers become 1...
				uint64Val := uint64(intValue) ^ (1 << 63)
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, uint64Val)
				propertyValueBytes = buf
			}
			ok = success
		case Float64Index:
			floatValue, success := GetIndexableFloatValue(m, typeNameForLog, index.PropertyName)
			if success {
				bits := math.Float64bits(floatValue)
				// For lexicographical sort of IEEE 754 floats:
				// If positive (sign bit is 0), flip sign bit to 1.
				// If negative (sign bit is 1), flip all bits.
				if bits&(1<<63) == 0 { // Positive or +0
					bits |= (1 << 63)
				} else { // Negative or -0
					bits = ^bits
				}
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, bits)
				propertyValueBytes = buf
			}
			ok = success
		case BoolIndex:
			boolValue, success := GetIndexableBoolValue(m, typeNameForLog, index.PropertyName)
			if success {
				if boolValue {
					propertyValueBytes = []byte{1} // True
				} else {
					propertyValueBytes = []byte{0} // False
				}
			}
			ok = success
		case DateTimeIndex:
			timeValue, success := GetIndexableDateTimeValue(m, typeNameForLog, index.PropertyName)
			if success {
				// RFC3339Nano is lexicographically sortable and human-readable.
				// Pre-allocate buffer for efficiency. Max length of RFC3339Nano is 35.
				propertyValueBytes = timeValue.AppendFormat(make([]byte, 0, 35), time.RFC3339Nano)
			}
			ok = success
		default:
			slog.Warn("BoltStore.Put: Unknown or unsupported index data type", "dataType", index.DataType.String(), "typeName", typeNameForLog, "property", index.PropertyName)
			continue // Skip this index
		}

		if !ok {
			// The GetIndexable<Type>Value function already logs the reason.
			continue
		}

		idBytes := []byte(id.String())
		indexKey := buildIndexKey(index.Type, propertyValueBytes, id)

		if index.Type == UniqueIndex {
			existingIdBytes := idxbucket.Get(indexKey)
			if existingIdBytes != nil && !bytes.Equal(existingIdBytes, idBytes) {
				// Value already exists for a different Storable ID, uniqueness constraint violation.
				return fmt.Errorf("uniqueness constraint violation for index '%s' on property '%s': value already mapped to ID %s : %w",
					index.PropertyName, string(indexBucketNameBytes), string(existingIdBytes), fault.ErrUniqueIndexConstraintViolation)
			}
		}

		if err := idxbucket.Put(indexKey, idBytes); err != nil {
			return fmt.Errorf("failed to put index entry for %s: %w", index.PropertyName, err)
		}
	}

	return nil
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

		return bs.updateIndexes(tx, m)
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

			err = bs.updateIndexes(tx, item)
			if err != nil {
				return fault.ErrIndexUpdateFailed
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
	// Use typeName as the bucket name. Pass typeId for unmarshalling.
	return bs.getAllFromBucket(typeId)
}

// getAllFromBucket is a helper to retrieve all items from a named bucket,
// unmarshalling them as the given typeId.
func (bs *BoltStore) getAllFromBucket(typeId int64) ([]Storable, error) {
	var results []Storable

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
// with the same property value while maintaining unique keys in BoltDB. The
// propertyValueBytes are joined with the ID bytes using a null byte separator.
func buildIndexKey(indexType IndexType, propertyValueBytes []byte, id *Id) []byte {
	switch indexType {
	case UniqueIndex:
		return propertyValueBytes
	case NonUniqueIndex:
		idBytes := []byte(id.String())
		// Use a null byte as a separator. This is generally safe as propertyValueBytes
		// from structured data (like numbers, specific string formats for time) are unlikely
		// to naturally form sequences that would collide after appending a null byte and an ID.
		key := make([]byte, 0, len(propertyValueBytes)+1+len(idBytes))
		key = append(key, propertyValueBytes...)
		key = append(key, 0) // Null byte separator
		key = append(key, idBytes...)
		return key
	}
	return nil // Should not happen
}

// Delete removes a model by its key.
func (bs *BoltStore) Delete(id *Id) error {
	if id == nil {
		return fault.ErrIdIsNil
	}

	// Step 1: Retrieve the storable. We need its actual data to correctly
	// form the index keys that need to be deleted.
	itemToDelete, err := bs.Get(id)
	if err != nil {
		if errors.Is(err, fault.ErrKeyNotFound) || errors.Is(err, fault.ErrBucketNotFound) {
			// Item or its containing bucket doesn't exist, so it's effectively already "deleted".
			slog.Debug("BoltStore.Delete: Item not found, considering delete successful", "id", id.String())
			return nil
		}
		// Another error occurred during Get (e.g., unmarshal failed, type not created).
		return fmt.Errorf("failed to retrieve item %s for deletion: %w", id.String(), err)
	}

	// If Get succeeded, itemToDelete should not be nil.
	// This check is mostly for defensive programming.
	if itemToDelete == nil {
		slog.Warn("BoltStore.Delete: Get succeeded but returned nil item, considering delete successful", "id", id.String())
		return nil
	}

	return bs.db.Update(func(tx *bbolt.Tx) error {
		// Step 2: Delete the item from its primary type bucket.
		bucketNameBytes, typeErr := bs.typeBucketKey(id.TypeId)
		if typeErr != nil {
			// This should ideally not happen if Get(id) succeeded, as Get also calls typeBucketKey.
			return fmt.Errorf("failed to get type bucket key for deleting item %s: %w", id.String(), typeErr)
		}

		bucket := tx.Bucket(bucketNameBytes)
		if bucket == nil {
			// This is an inconsistent state if Get(id) succeeded, as Get would have returned ErrBucketNotFound.
			// Log a warning and treat as if the item is already gone.
			slog.Warn("BoltStore.Delete: Type bucket disappeared during transaction", "id", id.String(), "bucketName", string(bucketNameBytes))
			return fault.ErrBucketNotFound // Or return nil if we consider it "already deleted"
		}

		keyBytes := []byte(id.String())
		if err := bucket.Delete(keyBytes); err != nil {
			// bbolt's Delete doesn't return an error if the key is not found.
			// This would be for other underlying BoltDB errors.
			return fmt.Errorf("failed to delete item %s from primary bucket %s: %w", id.String(), string(bucketNameBytes), err)
		}
		slog.Debug("BoltStore.Delete: Deleted item from primary bucket", "id", id.String(), "bucketName", string(bucketNameBytes))

		// Step 3: Delete entries from all relevant index buckets.
		// Use itemToDelete (which is 'Storable') to get indexed property values.
		typeNameForLog, getTypeNameErr := bs.typeManager.GetTypeName(id.TypeId)
		if getTypeNameErr != nil {
			slog.Warn("BoltStore.Delete: Could not get type name for logging index cleanup", "typeId", id.TypeId, "error", getTypeNameErr)
			typeNameForLog = fmt.Sprintf("typeId_%d", id.TypeId) // Fallback for logging context
		}

		for _, indexDef := range bs.typeManager.Indexes(id.TypeId) {
			indexBucketNameBytes, err := bs.mkIndexBucketName(id.TypeId, indexDef.PropertyName)
			if err != nil {
				return fmt.Errorf("failed to create index bucket name for property '%s' of item %s: %w", indexDef.PropertyName, id.String(), err)
			}

			idxBucket := tx.Bucket(indexBucketNameBytes)
			if idxBucket == nil {
				// Index bucket doesn't exist, so no entry to delete for this index.
				continue
			}

			var propertyValueBytes []byte
			var ok bool

			// This switch logic is similar to updateIndexes to get the value of the indexed property.
			switch indexDef.DataType {
			case StringIndex:
				val, success := GetIndexableStringValue(itemToDelete, typeNameForLog, indexDef.PropertyName)
				if success {
					propertyValueBytes = []byte(val)
				}
				ok = success
			case Int64Index:
				val, success := GetIndexableIntValue(itemToDelete, typeNameForLog, indexDef.PropertyName)
				if success {
					uint64Val := uint64(val) ^ (1 << 63)
					buf := make([]byte, 8)
					binary.BigEndian.PutUint64(buf, uint64Val)
					propertyValueBytes = buf
				}
				ok = success
			case Float64Index:
				val, success := GetIndexableFloatValue(itemToDelete, typeNameForLog, indexDef.PropertyName)
				if success {
					bits := math.Float64bits(val)
					if bits&(1<<63) == 0 {
						bits |= (1 << 63)
					} else {
						bits = ^bits
					}
					buf := make([]byte, 8)
					binary.BigEndian.PutUint64(buf, bits)
					propertyValueBytes = buf
				}
				ok = success
			case BoolIndex:
				val, success := GetIndexableBoolValue(itemToDelete, typeNameForLog, indexDef.PropertyName)
				if success {
					if val {
						propertyValueBytes = []byte{1}
					} else {
						propertyValueBytes = []byte{0}
					}
				}
				ok = success
			case DateTimeIndex:
				val, success := GetIndexableDateTimeValue(itemToDelete, typeNameForLog, indexDef.PropertyName)
				if success {
					propertyValueBytes = val.AppendFormat(make([]byte, 0, 35), time.RFC3339Nano)
				}
				ok = success
			default:
				slog.Warn("BoltStore.Delete: Unknown or unsupported index data type during index cleanup", "dataType", indexDef.DataType.String(), "typeName", typeNameForLog, "property", indexDef.PropertyName)
				continue
			}

			if !ok {
				// GetIndexable<Type>Value functions already log specific reasons for failure.
				// If we couldn't get the value, we can't form the key to delete.
				slog.Debug("BoltStore.Delete: Skipping index cleanup for property as value was not retrievable", "id", id.String(), "typeName", typeNameForLog, "property", indexDef.PropertyName)
				continue
			}

			indexKey := buildIndexKey(indexDef.Type, propertyValueBytes, id)
			if err := idxBucket.Delete(indexKey); err != nil {
				// bbolt's Delete doesn't error if key not found. This would be for other DB errors.
				return fmt.Errorf("failed to delete index entry for property '%s' from bucket '%s' (item %s): %w", indexDef.PropertyName, string(indexBucketNameBytes), id.String(), err)
			}
			slog.Debug("BoltStore.Delete: Deleted index entry", "id", id.String(), "property", indexDef.PropertyName, "indexBucketName", string(indexBucketNameBytes))
		}
		return nil
	})
}

func (bs *BoltStore) AllocateId(item Storable) error {
	return bs.typeManager.AllocateId(item)
}

func (bs *BoltStore) Match(indexName string, value interface{}) ([]Storable, error) {
	panic("not implemented")
}

func (bs *BoltStore) WildcardMatch(indexName string, pattern string) ([]Storable, error) {
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
