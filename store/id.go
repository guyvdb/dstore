package store

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/guyvdb/dstore/fault"
)

// Every object has an Id.
type Id struct {
	TypeId   int64 `json:"type_id"`
	ObjectId int64 `json:"object_id"`
}

func NewId(typeId, objectId int64) *Id {
	return &Id{
		TypeId:   typeId,
		ObjectId: objectId,
	}
}

func IdFromString(s string) (*Id, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("%w: expected <typeid>-<objectid>, got '%s'", fault.ErrInvalidIdFormat, s)
	}

	typeIdStr := parts[0]
	objectIdStr := parts[1]

	typeId, err := strconv.ParseInt(typeIdStr, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("%w '%s': %w", fault.ErrInvalidTypeId, typeIdStr, err)
	}

	objectId, err := strconv.ParseInt(objectIdStr, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("%w '%s': %w", fault.ErrInvalidObjectId, objectIdStr, err)
	}

	return NewId(typeId, objectId), nil
}

func (id *Id) String() string {
	return fmt.Sprintf("%x-%x", id.TypeId, id.ObjectId)
}
