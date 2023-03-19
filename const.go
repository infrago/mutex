package mutex

import "errors"

const (
	NAME    = "MUTEX"
	DEFAULT = "default"
)

var (
	//
	errInvalidMutexConnection = errors.New("Invalid mutex connection.")
)
