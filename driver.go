package mutex

import (
	"time"

	. "github.com/infrago/base"
)

type (
	// Driver 驱动
	Driver interface {
		Connect(*Instance) (Connect, error)
	}

	// Connect 连接
	Connect interface {
		//打开、关闭
		Open() error
		Close() error

		Lock(key string, expires time.Duration) error
		Unlock(key string) error
	}

	Instance struct {
		connect Connect
		Name    string
		Config  Config
		Setting Map
	}
)
