package mutex

import (
	"time"

	base "github.com/infrago/base"
)

type (
	// Driver defines a mutex driver.
	Driver interface {
		Connect(*Instance) (Connection, error)
	}

	// Connection defines a mutex connection.
	Connection interface {
		Open() error
		Close() error

		Lock(key string, expires time.Duration) error
		Unlock(key string) error
	}

	// Instance is the driver instance context.
	Instance struct {
		conn Connection
		Name    string
		Config  Config
		Setting base.Map
	}
)
