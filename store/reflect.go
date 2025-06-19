package store

import (
	"log/slog"
	"reflect"
	"time"
)

// GetIndexableStringValue uses reflection to extract the string value of a specified property
// from a Storable item. It's intended for use in indexing.
//
// Parameters:
//   - item: The Storable item from which to extract the value.
//   - typeName: The type name of the item, used for logging purposes.
//   - propertyName: The name of the property (struct field) to extract.
//     This is passed in because item.GetTypeName() might be too generic if 'item'
//     is an interface type at the call site.
//
// Returns:
//   - string: The string value of the property if found, accessible, and of string type.
//   - bool: True if the property was successfully extracted and is suitable for string indexing,
//     false otherwise. If false, a warning will be logged.
func GetIndexableStringValue(item Storable, typeName, propertyName string) (string, bool) {

	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		slog.Warn("GetIndexableStringValue: Item is not a struct, skipping indexable property", "typeName", typeName, "property", propertyName)
		return "", false
	}

	field := v.FieldByName(propertyName)

	if !field.IsValid() {
		slog.Warn("GetIndexableStringValue: Property not found in struct", "typeName", typeName, "property", propertyName)
		return "", false
	}
	if !field.CanInterface() {
		slog.Warn("GetIndexableStringValue: Property not exportable", "typeName", typeName, "property", propertyName)
		return "", false
	}

	if field.Kind() != reflect.String {
		slog.Warn("GetIndexableStringValue: Property is not a string", "typeName", typeName, "property", propertyName, "kind", field.Kind().String())
		return "", false
	}

	return field.String(), true
}

// GetIndexableIntValue uses reflection to extract the int64 value of a specified property
// from a Storable item. It's intended for use in indexing.
//
// Parameters:
//   - item: The Storable item from which to extract the value.
//   - typeName: The type name of the item, used for logging purposes.
//   - propertyName: The name of the property (struct field) to extract.
//
// Returns:
//   - int64: The int64 value of the property if found, accessible, and of int64 type.
//   - bool: True if the property was successfully extracted and is suitable for integer indexing,
//     false otherwise. If false, a warning will be logged.
func GetIndexableIntValue(item Storable, typeName, propertyName string) (int64, bool) {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		slog.Warn("GetIndexableIntValue: Item is not a struct, skipping indexable property", "typeName", typeName, "property", propertyName)
		return 0, false
	}

	field := v.FieldByName(propertyName)

	if !field.IsValid() || !field.CanInterface() || field.Kind() != reflect.Int64 {
		slog.Warn("GetIndexableIntValue: Property not found, not exportable, or not an int64", "typeName", typeName, "property", propertyName, "kind", field.Kind().String())
		return 0, false
	}
	return field.Int(), true
}

// GetIndexableFloatValue uses reflection to extract the float64 value of a specified property
// from a Storable item. It's intended for use in indexing.
//
// Parameters:
//   - item: The Storable item from which to extract the value.
//   - typeName: The type name of the item, used for logging purposes.
//   - propertyName: The name of the property (struct field) to extract.
//
// Returns:
//   - float64: The float64 value of the property if found, accessible, and of float64 type.
//   - bool: True if the property was successfully extracted and is suitable for float indexing,
//     false otherwise. If false, a warning will be logged.
func GetIndexableFloatValue(item Storable, typeName, propertyName string) (float64, bool) {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		slog.Warn("GetIndexableFloatValue: Item is not a struct, skipping indexable property", "typeName", typeName, "property", propertyName)
		return 0.0, false
	}

	field := v.FieldByName(propertyName)

	if !field.IsValid() || !field.CanInterface() || field.Kind() != reflect.Float64 {
		slog.Warn("GetIndexableFloatValue: Property not found, not exportable, or not a float64", "typeName", typeName, "property", propertyName, "kind", field.Kind().String())
		return 0.0, false
	}
	return field.Float(), true
}

// GetIndexableBoolValue uses reflection to extract the bool value of a specified property
// from a Storable item. It's intended for use in indexing.
//
// Parameters:
//   - item: The Storable item from which to extract the value.
//   - typeName: The type name of the item, used for logging purposes.
//   - propertyName: The name of the property (struct field) to extract.
//
// Returns:
//   - bool: The bool value of the property if found, accessible, and of bool type.
//   - bool: True if the property was successfully extracted and is suitable for boolean indexing,
//     false otherwise. If false, a warning will be logged.
func GetIndexableBoolValue(item Storable, typeName, propertyName string) (bool, bool) {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		slog.Warn("GetIndexableBoolValue: Item is not a struct, skipping indexable property", "typeName", typeName, "property", propertyName)
		return false, false
	}

	field := v.FieldByName(propertyName)

	if !field.IsValid() || !field.CanInterface() || field.Kind() != reflect.Bool {
		slog.Warn("GetIndexableBoolValue: Property not found, not exportable, or not a bool", "typeName", typeName, "property", propertyName, "kind", field.Kind().String())
		return false, false
	}
	return field.Bool(), true
}

// GetIndexableDateTimeValue uses reflection to extract the time.Time value of a specified property
// from a Storable item. It's intended for use in indexing.
//
// Parameters:
//   - item: The Storable item from which to extract the value.
//   - typeName: The type name of the item, used for logging purposes.
//   - propertyName: The name of the property (struct field) to extract.
//
// Returns:
//   - time.Time: The time.Time value of the property if found, accessible, and of time.Time type.
//   - bool: True if the property was successfully extracted and is suitable for date/time indexing,
//     false otherwise. If false, a warning will be logged.
func GetIndexableDateTimeValue(item Storable, typeName, propertyName string) (time.Time, bool) {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		slog.Warn("GetIndexableDateTimeValue: Item is not a struct, skipping indexable property", "typeName", typeName, "property", propertyName)
		return time.Time{}, false
	}

	field := v.FieldByName(propertyName)

	// Check if the field is of type time.Time
	if !field.IsValid() || !field.CanInterface() || field.Type() != reflect.TypeOf(time.Time{}) {
		slog.Warn("GetIndexableDateTimeValue: Property not found, not exportable, or not a time.Time", "typeName", typeName, "property", propertyName, "kind", field.Kind().String(), "actualType", field.Type().String())
		return time.Time{}, false
	}

	// Get the interface value and type assert to time.Time
	val, ok := field.Interface().(time.Time)
	if !ok {
		// This should ideally not happen if the previous check passed, but it's a safeguard.
		slog.Warn("GetIndexableDateTimeValue: Failed to assert property to time.Time", "typeName", typeName, "property", propertyName)
		return time.Time{}, false
	}
	return val, true
}
