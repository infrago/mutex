package mutex

import "errors"

const (
	NAME    = "MUTEX"
	DEFAULT = "default"
)

var (
	ErrInvalidConnection = errors.New("Invalid mutex connection.")
)
