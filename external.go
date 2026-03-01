package mutex

import (
	"fmt"
	"strings"
	"time"

	base "github.com/infrago/base"
)

type (
	locker struct {
		conn string
		key  string
	}
)

func Key(args ...base.Any) string {
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, fmt.Sprintf("%v", arg))
	}
	return strings.Join(keys, "-")
}

func LockOn(conn string, args ...base.Any) (*locker, error) {
	keys := make([]base.Any, 0, len(args))
	exps := make([]time.Duration, 0, 1)

	for _, arg := range args {
		if exp, ok := arg.(time.Duration); ok {
			exps = append(exps, exp)
		} else {
			keys = append(keys, arg)
		}
	}

	key := Key(keys...)
	if err := module.LockOn(conn, key, exps...); err != nil {
		return nil, err
	}
	return &locker{conn: conn, key: key}, nil
}

func UnlockOn(conn string, args ...base.Any) error {
	key := Key(args...)
	return module.UnlockOn(conn, key)
}

func LockedOn(conn string, args ...base.Any) bool {
	lok, err := LockOn(conn, args...)
	if err != nil {
		return true
	}
	_ = lok.Unlock()
	return false
}

func Lock(args ...base.Any) (*locker, error) {
	return LockOn("", args...)
}

func Unlock(args ...base.Any) error {
	return UnlockOn("", args...)
}

func Locked(args ...base.Any) bool {
	return LockedOn("", args...)
}

func (l *locker) Unlock() error {
	return UnlockOn(l.conn, l.key)
}
