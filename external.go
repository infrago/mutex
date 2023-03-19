package mutex

import (
	"fmt"
	"strings"
	"time"

	. "github.com/infrago/base"
)

func makeKey(args []string) string {
	return strings.Join(args, "-")
}

func Lock(args ...Any) error {
	keys := []string{}
	exps := []time.Duration{}

	for _, arg := range args {
		if exp, ok := arg.(time.Duration); ok {
			exps = append(exps, exp)
		} else {
			keys = append(keys, fmt.Sprintf("%v", arg))
		}
	}

	return module.Lock(makeKey(keys), exps...)
}
func Unlock(args ...Any) error {
	keys := []string{}
	for _, arg := range args {
		keys = append(keys, fmt.Sprintf("%v", arg))
	}
	return module.Unlock(makeKey(keys))
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

	return module.LockTo(conn, makeKey(keys), exps...)
}
func UnlockFrom(conn string, args ...Any) error {
	keys := []string{}
	for _, arg := range args {
		keys = append(keys, fmt.Sprintf("%v", arg))
	}
	return module.UnlockFrom(conn, makeKey(keys))
}

func Locked(args ...Any) bool {
	return Lock(args...) != nil
}
