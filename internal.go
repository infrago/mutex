package mutex

import "time"

// . "github.com/infrago/base"

// Lock 加锁
func (this *Module) getInst(conn, key string) (*Instance, error) {
	if conn != "" {
		conn = this.hashring.Locate(key)
	}

	if inst, ok := this.instances[conn]; ok {
		return inst, nil
	}

	return nil, ErrInvalidConnection
}

// LockTo 加锁到指定的连接
func (this *Module) LockOn(conn string, key string, expiries ...time.Duration) error {
	inst, err := this.getInst(conn, key)
	if err != nil {
		return err
	}

	expiry := inst.Config.Expiry
	if len(expiries) > 0 {
		expiry = expiries[0]
	}

	// 加上前缀
	realKey := inst.Config.Prefix + key
	return inst.connect.Lock(realKey, expiry)
}

// Lock 加锁
func (this *Module) Lock(key string, expiries ...time.Duration) error {
	return this.LockOn("", key, expiries...)
}

// UnlockOn 指定库解锁
func (this *Module) UnlockOn(conn, key string) error {
	inst, err := this.getInst(conn, key)
	if err != nil {
		return err
	}
	//加上前缀
	realKey := inst.Config.Prefix + key
	return inst.connect.Unlock(realKey)
}

// Unlock 解锁
func (this *Module) Unlock(conn, key string) error {
	return this.UnlockOn("", key)
}
