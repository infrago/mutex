package mutex

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	base "github.com/infrago/base"
)

type (
	locker struct {
		conn   string
		key    string
		token  string
		expire time.Duration

		mutex         sync.Mutex
		keepAliveStop chan struct{}
		keepAliveDone chan struct{}
	}
)

func Key(args ...base.Any) string {
	return KeyWith("-", args...)
}

func KeyWith(sep string, args ...base.Any) string {
	if sep == "" {
		sep = "-"
	}
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, fmt.Sprintf("%v", arg))
	}
	return strings.Join(keys, sep)
}

func LockOn(conn string, args ...base.Any) (*locker, error) {
	keys, exps := splitArgs(args...)
	key := Key(keys...)
	token, expire, err := module.lockTokenOn(conn, key, exps...)
	if err != nil {
		return nil, err
	}
	return &locker{conn: conn, key: key, token: token, expire: expire}, nil
}

func TryLockOn(conn string, args ...base.Any) (*locker, error) {
	return LockOn(conn, args...)
}

func WaitLockOn(conn string, timeout, interval time.Duration, args ...base.Any) (*locker, error) {
	ctx, cancel := context.WithTimeout(context.Background(), normalizeWaitTimeout(timeout))
	defer cancel()
	locker, err := waitLockOnContext(conn, ctx, interval, args...)
	if err == nil {
		return locker, nil
	}
	if err == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, err
}

func WaitLockOnContext(conn string, ctx context.Context, interval time.Duration, args ...base.Any) (*locker, error) {
	return waitLockOnContext(conn, ctx, interval, args...)
}

func UnlockOn(conn string, args ...base.Any) error {
	keys, _ := splitArgs(args...)
	key := Key(keys...)
	return module.UnlockOn(conn, key)
}

func RefreshOn(conn string, args ...base.Any) error {
	keys, exps := splitArgs(args...)
	key := Key(keys...)
	return module.RefreshOn(conn, key, exps...)
}

func LockedOn(conn string, args ...base.Any) bool {
	locked, err := CheckOn(conn, args...)
	if err != nil {
		return true
	}
	return locked
}

func Lock(args ...base.Any) (*locker, error) {
	return LockOn("", args...)
}

func TryLock(args ...base.Any) (*locker, error) {
	return TryLockOn("", args...)
}

func WaitLock(timeout, interval time.Duration, args ...base.Any) (*locker, error) {
	return WaitLockOn("", timeout, interval, args...)
}

func WaitLockContext(ctx context.Context, interval time.Duration, args ...base.Any) (*locker, error) {
	return WaitLockOnContext("", ctx, interval, args...)
}

func Unlock(args ...base.Any) error {
	return UnlockOn("", args...)
}

func Refresh(args ...base.Any) error {
	return RefreshOn("", args...)
}

func Locked(args ...base.Any) bool {
	return LockedOn("", args...)
}

func CheckOn(conn string, args ...base.Any) (bool, error) {
	keys, _ := splitArgs(args...)
	key := Key(keys...)
	return module.LockedOn(conn, key)
}

func Check(args ...base.Any) (bool, error) {
	return CheckOn("", args...)
}

func (l *locker) Unlock() error {
	l.stopKeepAlive()
	return module.unlockTokenOn(l.conn, l.key, l.token)
}

func (l *locker) Refresh(expires ...time.Duration) error {
	return module.refreshTokenOn(l.conn, l.key, l.token, expires...)
}

func (l *locker) KeepAlive(intervals ...time.Duration) error {
	interval := l.expire / 2
	if len(intervals) > 0 && intervals[0] > 0 {
		interval = intervals[0]
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	l.stopKeepAlive()

	l.mutex.Lock()
	stop := make(chan struct{})
	done := make(chan struct{})
	l.keepAliveStop = stop
	l.keepAliveDone = done
	l.mutex.Unlock()

	go l.keepAliveLoop(stop, done, interval)
	return nil
}

func Stats() Statistics {
	return module.Stats()
}

func StatsFrom(conn string) (Statistics, error) {
	return module.StatsFrom(conn)
}

func ResetStats() {
	module.ResetStats()
}

func Capabilities() map[string]Capability {
	return module.Capabilities()
}

func CapabilityFrom(conn string) (Capability, error) {
	return module.CapabilityFrom(conn)
}

func Debug() DebugInfo {
	return module.Debug()
}

func DebugTokens() []DebugToken {
	return module.DebugTokens()
}

func waitLockOnContext(conn string, ctx context.Context, interval time.Duration, args ...base.Any) (*locker, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}

	for {
		locker, err := TryLockOn(conn, args...)
		if err == nil {
			return locker, nil
		}
		if !isLockedError(err) {
			return nil, err
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func normalizeWaitTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return time.Nanosecond
	}
	return timeout
}

func (l *locker) stopKeepAlive() {
	l.mutex.Lock()
	stop := l.keepAliveStop
	done := l.keepAliveDone
	l.keepAliveStop = nil
	l.keepAliveDone = nil
	l.mutex.Unlock()

	if stop != nil {
		close(stop)
		<-done
	}
}

func (l *locker) keepAliveLoop(stop <-chan struct{}, done chan<- struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(done)

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := module.refreshTokenOn(l.conn, l.key, l.token, l.expire); err != nil {
				return
			}
		}
	}
}

func splitArgs(args ...base.Any) ([]base.Any, []time.Duration) {
	keys := make([]base.Any, 0, len(args))
	exps := make([]time.Duration, 0, 1)

	for _, arg := range args {
		if exp, ok := arg.(time.Duration); ok {
			exps = append(exps, exp)
		} else {
			keys = append(keys, arg)
		}
	}

	return keys, exps
}
