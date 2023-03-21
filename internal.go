package mutex

import "time"

// . "github.com/infrago/base"

// Lock 加锁
func (this *Module) Lock(key string, expiries ...time.Duration) error {
	locate := this.hashring.Locate(key)

	if inst, ok := this.instances[locate]; ok {

		expiry := inst.Config.Expiry
		if len(expiries) > 0 {
			expiry = expiries[0]
		}

		// 加上前缀
		key := inst.Config.Prefix + key

		return inst.connect.Lock(key, expiry)
	}

	return errInvalidMutexConnection
}

// LockTo 加锁到指定的连接
func (this *Module) LockTo(conn string, key string, expiries ...time.Duration) error {
	if inst, ok := this.instances[conn]; ok {

		//默认过期时间
		expiry := inst.Config.Expiry
		if len(expiries) > 0 {
			expiry = expiries[0]
		}

		// 加上前缀
		key := inst.Config.Prefix + key

		return inst.connect.Lock(key, expiry)
	}

	return errInvalidMutexConnection
}

// Unlock 解锁
func (this *Module) Unlock(key string) error {
	locate := this.hashring.Locate(key)

	if inst, ok := this.instances[locate]; ok {
		key := inst.Config.Prefix + key //加上前缀
		return inst.connect.Unlock(key)
	}

	return errInvalidMutexConnection
}

// UnlockFrom 从指定的连接解锁
func (this *Module) UnlockFrom(locate string, key string) error {
	if inst, ok := this.instances[locate]; ok {
		key := inst.Config.Prefix + key //加上前缀
		return inst.connect.Unlock(key)
	}

	return errInvalidMutexConnection
}
