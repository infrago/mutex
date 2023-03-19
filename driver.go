package mutex

import (
	"time"
)

type (
	// Driver 数据驱动
	Driver interface {
		Connect(name string, config Config) (Connect, error)
	}

	// Connect 会话连接
	Connect interface {
		//打开、关闭
		Open() error
		Close() error

		Lock(key string, expiry time.Duration) error
		Unlock(key string) error
	}
)
