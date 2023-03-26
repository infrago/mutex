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

func Lock(args ...Any) error {
	keys := []Any{}
	exps := []time.Duration{}

	for _, arg := range args {
		if exp, ok := arg.(time.Duration); ok {
			exps = append(exps, exp)
		} else {
			keys = append(keys, arg)
		}
	}

	key := Key(keys...)

	return module.Lock(key, exps...)
}
func Unlock(args ...Any) error {
	return module.Unlock(Key(args))
}
func LockTo(conn string, args ...Any) error {
	keys := []string{}
	exps := []time.Duration{}

	for _, arg := range args {
		if exp, ok := arg.(time.Duration); ok {
			exps = append(exps, exp)
		} else {
			keys = append(keys, fmt.Sprintf("%v", arg))
		}
	}

	return module.LockTo(conn, Key(keys), exps...)
}
func UnlockFrom(conn string, args ...Any) error {
	keys := []string{}
	for _, arg := range args {
		keys = append(keys, fmt.Sprintf("%v", arg))
	}
	return module.UnlockFrom(conn, Key(keys))
}

func Locked(args ...Any) bool {
	return Lock(args...) != nil
}
