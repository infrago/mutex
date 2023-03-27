package mutex

import (
	"fmt"
	"strings"
	"time"

	. "github.com/infrago/base"
)

func Key(args ...Any) string {
	keys := []string{}
	for _, arg := range args {
		keys = append(keys, fmt.Sprintf("%v", arg))
	}

	return strings.Join(keys, "-")
}

func LockOn(conn string, args ...Any) error {
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
	return module.LockOn(conn, key, exps...)
}
func UnlockOn(conn string, args ...Any) error {
	key := Key(args...)
	return module.UnlockOn(conn, key)
}

func LockedOn(conn string, args ...Any) bool {
	return LockOn(conn, args...) != nil
}

func Lock(args ...Any) error {
	return LockOn("", args...)
}
func Unlock(args ...Any) error {
	return UnlockOn("", args...)
}
func Locked(args ...Any) bool {
	return Lock(args...) != nil
}
