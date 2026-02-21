package mutex

import (
	"errors"
	"sync"
	"time"
)

type defaultDriver struct{}

type defaultConnect struct {
	mutex    sync.Mutex
	instance *Instance
	locks    map[string]time.Time
}

func (d *defaultDriver) Connect(inst *Instance) (Connection, error) {
	return &defaultConnect{
		instance: inst,
		locks:    make(map[string]time.Time, 0),
	}, nil
}

func (c *defaultConnect) Open() error {
	if c.locks == nil {
		c.locks = make(map[string]time.Time, 0)
	}
	return nil
}

func (c *defaultConnect) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.locks = make(map[string]time.Time, 0)
	return nil
}

func (c *defaultConnect) Lock(key string, expire time.Duration) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if expire <= 0 {
		expire = c.instance.Config.Expire
	}
	if expire <= 0 {
		expire = time.Second
	}

	if until, ok := c.locks[key]; ok {
		if time.Now().Before(until) {
			return errors.New("existed")
		}
		delete(c.locks, key)
	}

	c.locks[key] = time.Now().Add(expire)
	return nil
}

func (c *defaultConnect) Unlock(key string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.locks == nil {
		return nil
	}
	delete(c.locks, key)
	return nil
}
