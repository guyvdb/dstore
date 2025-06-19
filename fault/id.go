package fault

import "errors"

// Predefined errors for Id parsing.
var (
	// ErrInvalidIdFormat indicates that the string representation of an Id
	// does not conform to the expected "<typeid>-<objectid>" format.
	ErrInvalidIdFormat = errors.New("invalid id format")

	// ErrInvalidTypeId indicates that the type_id part of an Id string
	// could not be parsed as a hexadecimal integer.
	ErrInvalidTypeId = errors.New("invalid type_id in id string")

	// ErrInvalidObjectId indicates that the object_id part of an Id string
	// could not be parsed as a hexadecimal integer.
	ErrInvalidObjectId = errors.New("invalid object_id in id string")
)
