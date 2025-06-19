package fault

import "errors"

var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketCreateFailed = errors.New("bucket create failed")
	ErrUnmarshalFailed    = errors.New("unmarshal failed")
	ErrMarshalFailed      = errors.New("marshal failed")
	ErrNilStoreable       = errors.New("nil storeable")
	ErrStorableHasNilId   = errors.New("storable has a nil Id")
	ErrIdIsNil            = errors.New("id is nil")
	ErrPutFailed          = errors.New("put failed")
)
