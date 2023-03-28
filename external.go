package mutex

import (
	"fmt"
	"strings"
	"time"

	. "github.com/infrago/base"
)

type (
	locker struct {
		conn, key string
	}
)

func Key(args ...Any) string {
	keys := []string{}
	for _, arg := range args {
		keys = append(keys, fmt.Sprintf("%v", arg))
	}

	return strings.Join(keys, "-")
}

func LockOn(conn string, args ...Any) (*locker, error) {
	keys := []Any{}
	exps := []time.Duration{}

	for _, arg := range args {
		if exp, ok := arg.(time.Duration); ok {
			exps = append(exps, exp)
		} else {
			keys = append(keys, fmt.Sprintf("%v", arg))
		}
	}

	key := Key(keys...)
	err := module.LockOn(conn, key, exps...)
	if err != nil {
		return nil, err
	}

	return &locker{conn, key}, nil
}
func UnlockOn(conn string, args ...Any) error {
	key := Key(args...)
	return module.UnlockOn(conn, key)
}

func LockedOn(conn string, args ...Any) bool {
	_, err := LockOn(conn, args...)
	return err != nil
}

func Lock(args ...Any) (*locker, error) {
	return LockOn("", args...)
}
func Unlock(args ...Any) error {
	return UnlockOn("", args...)
}
func Locked(args ...Any) bool {
	_, err := Lock(args...)
	return err != nil
}

// 解锁方法
func (this *locker) Unlock() error {
	return UnlockOn(this.conn, this.key)
}
