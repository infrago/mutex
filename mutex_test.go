package mutex

import (
	"context"
	"errors"
	"testing"
	"time"

	base "github.com/infrago/base"
	"github.com/infrago/infra"
)

func resetModuleForTest(t *testing.T) {
	t.Helper()
	module.Close()
	module.mutex.Lock()
	defer module.mutex.Unlock()
	module.configs = map[string]Config{}
	module.instances = map[string]*Instance{}
	module.weights = map[string]int{}
	module.ring = nil
	module.opened = false
	module.closed = false
	module.started = false
	module.tokens = map[string][]tokenEntry{}
	module.cleanupInterval = 0
	module.stats.reset()
}

func TestMutexConfigFromPlainMap(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Config(map[string]any{
		"mutex": map[string]any{
			"driver":           "default",
			"prefix":           "app:",
			"expire":           "2s",
			"cleanup_interval": "7s",
			"token_grace":      "9s",
			"setting": map[string]any{
				"note": "ok",
			},
		},
	})

	cfg := module.configs[infra.DEFAULT]
	if cfg.Prefix != "app:" {
		t.Fatalf("unexpected prefix: %q", cfg.Prefix)
	}
	if cfg.Expire != 2*time.Second {
		t.Fatalf("unexpected expire: %s", cfg.Expire)
	}
	if cfg.CleanupInterval != 7*time.Second {
		t.Fatalf("unexpected cleanup interval: %s", cfg.CleanupInterval)
	}
	if cfg.TokenGrace != 9*time.Second {
		t.Fatalf("unexpected token grace: %s", cfg.TokenGrace)
	}
	if cfg.Setting["note"] != "ok" {
		t.Fatalf("unexpected setting: %v", cfg.Setting)
	}
}

func TestMutexKeyWith(t *testing.T) {
	if got := KeyWith(":", "order", 12, "pay"); got != "order:12:pay" {
		t.Fatalf("unexpected key: %q", got)
	}
}

func TestMutexLockedIsNonDestructive(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	lk, err := Lock("user", 1, time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	locked := Locked("user", 1, time.Second)
	if !locked {
		t.Fatal("expected locked to be true")
	}
	if _, err := Lock("user", 1, time.Second); err == nil {
		t.Fatal("locked check released the lock")
	}
	if err := lk.Unlock(); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if Locked("user", 1, time.Second) {
		t.Fatal("expected unlocked state")
	}
}

func TestMutexStaleLockerDoesNotUnlockNewOwner(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	first, err := Lock("job", 1, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	second, err := Lock("job", 1, time.Second)
	if err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if err := first.Unlock(); err != nil {
		t.Fatalf("stale unlock: %v", err)
	}
	if !Locked("job", 1, time.Second) {
		t.Fatal("stale locker removed new owner's lock")
	}
	if err := second.Unlock(); err != nil {
		t.Fatalf("second unlock: %v", err)
	}
}

func TestMutexStaleHelperUnlockDoesNotUnlockNewOwner(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	if _, err := Lock("job", 2, 30*time.Millisecond); err != nil {
		t.Fatalf("first lock: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := Lock("job", 2, time.Second); err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if err := Unlock("job", 2); err != nil {
		t.Fatalf("helper unlock: %v", err)
	}
	locked, err := Check("job", 2)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !locked {
		t.Fatal("stale helper unlock removed new owner's lock")
	}
}

func TestMutexRefreshExtendsLease(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	locker, err := Lock("job", 4, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	time.Sleep(25 * time.Millisecond)
	if err := locker.Refresh(90 * time.Millisecond); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := Lock("job", 4, time.Second); err == nil {
		t.Fatal("refresh did not extend lease")
	}
	if err := locker.Unlock(); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestMutexKeepAliveExtendsLease(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	locker, err := Lock("job", 10, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := locker.KeepAlive(10 * time.Millisecond); err != nil {
		t.Fatalf("keepalive: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := TryLock("job", 10, time.Second); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected lock to stay alive, got %v", err)
	}
	if err := locker.Unlock(); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestMutexInvalidLease(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	if _, err := Lock("job", 7, -time.Second); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("expected ErrInvalidLease, got %v", err)
	}
	locker, err := Lock("job", 8, time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := locker.Refresh(-time.Second); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("expected ErrInvalidLease on refresh, got %v", err)
	}
}

func TestMutexTryLockAndWaitLock(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	locker, err := TryLock("job", 11, time.Second)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	defer locker.Unlock()

	if _, err := TryLock("job", 11, time.Second); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = locker.Unlock()
	}()

	waited, err := WaitLock(300*time.Millisecond, 10*time.Millisecond, "job", 11, time.Second)
	if err != nil {
		t.Fatalf("wait lock: %v", err)
	}
	if err := waited.Unlock(); err != nil {
		t.Fatalf("wait unlock: %v", err)
	}
}

func TestMutexWaitLockTimeoutAndCancel(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	locker, err := Lock("job", 12, time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer locker.Unlock()

	if _, err := WaitLock(40*time.Millisecond, 10*time.Millisecond, "job", 12, time.Second); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WaitLockContext(ctx, 10*time.Millisecond, "job", 12, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestMutexCleanupExpiredTokens(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	module.token.Lock()
	module.tokens[tokenQueueKey(infra.DEFAULT, "job-5")] = []tokenEntry{{
		token: "expired",
		until: time.Now().Add(-2 * time.Minute),
		grace: time.Minute,
	}}
	module.token.Unlock()

	if removed := module.cleanupExpiredTokens(); removed == 0 {
		t.Fatal("expected expired tokens to be cleaned")
	}
	stats := Stats()
	if stats.ActiveTokens != 0 {
		t.Fatalf("expected no active tokens, got %d", stats.ActiveTokens)
	}
}

func TestMutexStats(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	ResetStats()
	locker, err := Lock("order:6", time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if _, err := Lock("order:6", time.Second); err == nil {
		t.Fatal("expected contention")
	}
	locker2, err := Lock("order:7", time.Second)
	if err != nil {
		t.Fatalf("lock order 7: %v", err)
	}
	if _, err := TryLock("order:7", time.Second); err == nil {
		t.Fatal("expected second contention")
	}
	if _, err := Check("order:6"); err != nil {
		t.Fatalf("check: %v", err)
	}
	if err := locker.Refresh(time.Second); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if err := locker.Unlock(); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := locker2.Unlock(); err != nil {
		t.Fatalf("unlock2: %v", err)
	}

	stats := Stats()
	if stats.Lock != 2 {
		t.Fatalf("unexpected lock stats: %+v", stats)
	}
	if stats.Contention != 2 {
		t.Fatalf("unexpected contention stats: %+v", stats)
	}
	if stats.Check != 1 || stats.Refresh != 1 || stats.Unlock != 2 {
		t.Fatalf("unexpected op stats: %+v", stats)
	}
	if stats.ContentionByPrefix["order"] != 2 {
		t.Fatalf("unexpected contention prefix stats: %+v", stats.ContentionByPrefix)
	}
	if stats.ContentionByKey["order:6"] != 1 || stats.ContentionByKey["order:7"] != 1 {
		t.Fatalf("unexpected contention key stats: %+v", stats.ContentionByKey)
	}
	if len(stats.HotKeys) == 0 || stats.HotKeys[0].Name == "" {
		t.Fatalf("expected hot keys, got %+v", stats.HotKeys)
	}
	if stats.ActiveTokens != 0 {
		t.Fatalf("unexpected active tokens: %+v", stats)
	}
}

func TestMutexCapabilitiesAndDebug(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	defer module.Close()

	caps := Capabilities()
	capability, ok := caps[infra.DEFAULT]
	if !ok {
		t.Fatalf("missing default capability: %+v", caps)
	}
	if !capability.Check || !capability.Refresh || !capability.Token || !capability.TokenRefresh {
		t.Fatalf("unexpected default capability: %+v", capability)
	}
	if _, err := CapabilityFrom("missing"); !errors.Is(err, ErrInvalidConnection) {
		t.Fatalf("expected invalid connection, got %v", err)
	}

	locker, err := Lock("debug", 1, time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer locker.Unlock()

	debug := Debug()
	if !debug.Opened {
		t.Fatal("expected debug opened state")
	}
	inst, ok := debug.Instances[infra.DEFAULT]
	if !ok {
		t.Fatalf("missing default debug instance: %+v", debug.Instances)
	}
	if inst.Driver != infra.DEFAULT || !inst.Capability.TokenRefresh {
		t.Fatalf("unexpected debug instance: %+v", inst)
	}
	if debug.Stats.ActiveTokens != 1 {
		t.Fatalf("unexpected debug stats: %+v", debug.Stats)
	}

	tokens := DebugTokens()
	if len(tokens) != 1 {
		t.Fatalf("expected one debug token group, got %+v", tokens)
	}
	if tokens[0].Conn != infra.DEFAULT || tokens[0].Key != "debug-1" {
		t.Fatalf("unexpected debug token group: %+v", tokens[0])
	}
	if tokens[0].Active != 1 || tokens[0].Count != 1 || len(tokens[0].Entries) != 1 {
		t.Fatalf("unexpected debug token entry: %+v", tokens[0])
	}
	if !tokens[0].Entries[0].Active || tokens[0].Entries[0].Remaining <= 0 {
		t.Fatalf("unexpected debug token timing: %+v", tokens[0].Entries[0])
	}
}

func TestMutexResolveCleanupInterval(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.RegisterConfig("fast", Config{
		Driver:          infra.DEFAULT,
		Weight:          1,
		Expire:          time.Second,
		CleanupInterval: 5 * time.Second,
		TokenGrace:      time.Second,
		Setting:         base.Map{},
	})
	module.RegisterConfig("slow", Config{
		Driver:          infra.DEFAULT,
		Weight:          1,
		Expire:          time.Second,
		CleanupInterval: 12 * time.Second,
		TokenGrace:      time.Second,
		Setting:         base.Map{},
	})
	module.Setup()
	if got := module.resolveCleanupIntervalLocked(); got != 5*time.Second {
		t.Fatalf("unexpected cleanup interval: %s", got)
	}
}

func TestMutexClosedError(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.Setup()
	module.Open()
	module.Close()

	if _, err := Check("job", 9); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestMutexCheckReturnsError(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	locked, err := Check("job", 3)
	if err == nil {
		t.Fatalf("expected error, got locked=%v", locked)
	}
}

func TestMutexNegativeWeightExcludedFromRing(t *testing.T) {
	resetModuleForTest(t)
	t.Cleanup(func() { resetModuleForTest(t) })

	module.RegisterConfig("active", Config{
		Driver:  infra.DEFAULT,
		Weight:  1,
		Expire:  time.Second,
		Setting: base.Map{},
	})
	module.RegisterConfig("standby", Config{
		Driver:  infra.DEFAULT,
		Weight:  -1,
		Expire:  time.Second,
		Setting: base.Map{},
	})
	module.Open()
	defer module.Close()

	if module.configs["standby"].Weight != -1 {
		t.Fatalf("unexpected standby weight: %d", module.configs["standby"].Weight)
	}
	for i := 0; i < 10; i++ {
		if got := module.ring.Locate("user:1"); got != "active" {
			t.Fatalf("unexpected ring target: %q", got)
		}
	}
}
