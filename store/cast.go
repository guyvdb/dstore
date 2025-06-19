package store

import (
	"fmt"
	"log/slog"
)

// GetAs retrieves an item from the KVStore by its key and attempts to cast it
// to the specified generic type T. T must be a type that implements the Storable interface.
// Typically, T will be a pointer to your model struct (e.g., *model.Nursery).
//
// It returns the typed item and an error. If the item is not found, it returns
// the zero value for T and nil error (or you could define a specific ErrNotFound).
// If the item is found but is not of the expected type T, it returns an error.
// func GetAs[T Storable](store Store, key string) (T, error) {
// 	var zeroT T // Zero value of type T (e.g., nil for pointer types)

// 	item, err := store.Get(key)
// 	if err != nil {
// 		return zeroT, fmt.Errorf("store.GetAs: failed to get item with key '%s': %w", key, err)
// 	}

// 	if item == nil { // Item not found
// 		return zeroT, nil // Or return a custom ErrNotFound
// 	}

// 	typedItem, ok := item.(T)
// 	if !ok {
// 		return zeroT, fmt.Errorf("store.GetAs: item with key '%s' is of type %T, not the expected type %T", key, item, zeroT)
// 	}
// 	return typedItem, nil
// }

func As[T Storable](item Storable) T {
	var zeroT T
	if item == nil {
		slog.Warn("store.As: item is nil")
		return zeroT
	}
	typedItem, ok := item.(T)
	if !ok {
		slog.Warn("store.As: item is not of the expected type", "expected", fmt.Sprintf("%T", zeroT), "actual", fmt.Sprintf("%T", item))
		return zeroT
	}
	return typedItem
}

func GetAs[T Storable](store Store, id *Id) (T, error) {
	var zeroT T
	item, err := store.Get(id)
	if err != nil {
		return zeroT, err
	}
	return As[T](item), nil
}

func AllAs[T Storable](items []Storable) ([]T, error) {
	if items == nil {
		// Return nil, nil if the input slice is nil.
		// Alternatively, one could return make([]T, 0), nil for an empty slice.
		return []T{}, nil
	}

	typedItems := make([]T, 0, len(items))
	var zeroT T // Used for type information in error messages and for nil items.

	for i, item := range items {
		if item == nil {
			// If the Storable item itself is nil, append the zero value of T.
			// This is consistent with how As(nil) behaves.
			typedItems = append(typedItems, zeroT)
			continue
		}

		// Attempt to cast the non-nil item to type T.
		typedItem, ok := item.(T)
		if !ok {
			return nil, fmt.Errorf("store.AllAs: item at index %d (type %T) cannot be cast to target type %T", i, item, zeroT)
		}
		typedItems = append(typedItems, typedItem)
	}
	return typedItems, nil
}

// func AllAs[T Storable](items []Storable) ([]T, error) {

// }

func GetAllAs[T Storable](store Store, typeName string) ([]T, error) {
	storables, err := store.GetAllByTypeName(typeName)
	if err != nil {
		// Return nil for the slice part when an error occurs.
		return nil, fmt.Errorf("store.GetAllAs: failed to get all items of type '%s' from store: %w", typeName, err)
	}

	// AllAs will handle nil or empty storables slice appropriately.
	// It will also propagate any casting errors.
	return AllAs[T](storables)
}
