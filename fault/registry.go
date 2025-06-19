package fault

import "errors"

var (
	ErrTypeNotCreated = errors.New("type not created")
	ErrTypeNotFound   = errors.New("type not found")
)
