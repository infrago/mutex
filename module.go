package mutex

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	base "github.com/infrago/base"
	"github.com/infrago/infra"
)

func init() {
	infra.Mount(module)
	module.RegisterDriver(infra.DEFAULT, &defaultDriver{})
}

var module = &Module{
	drivers:   make(map[string]Driver, 0),
	configs:   make(map[string]Config, 0),
	instances: make(map[string]*Instance, 0),
	weights:   make(map[string]int, 0),
	tokens:    make(map[string][]tokenEntry, 0),
}

type (
	Module struct {
		mutex sync.RWMutex
		token sync.Mutex

		opened  bool
		closed  bool
		started bool

		drivers   map[string]Driver
		configs   map[string]Config
		instances map[string]*Instance
		weights   map[string]int
		ring      *hashRing
		tokens    map[string][]tokenEntry
		stats     mutexStats

		cleanupInterval time.Duration

		cleanerStop chan struct{}
		cleanerDone chan struct{}
	}

	tokenEntry struct {
		token string
		until time.Time
		grace time.Duration
	}

	Config struct {
		Driver          string
		Weight          int
		Prefix          string
		Expire          time.Duration
		CleanupInterval time.Duration
		TokenGrace      time.Duration
		Setting         base.Map
	}

	Configs map[string]Config
)

// Register dispatches registrations.
func (m *Module) Register(name string, value base.Any) {
	switch v := value.(type) {
	case Driver:
		m.RegisterDriver(name, v)
	case Config:
		m.RegisterConfig(name, v)
	case Configs:
		m.RegisterConfigs(v)
	}
}

// RegisterDriver registers a mutex driver.
func (m *Module) RegisterDriver(name string, driver Driver) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}
	if driver == nil {
		panic("Invalid mutex driver: " + name)
	}
	if infra.Override() {
		m.drivers[name] = driver
	} else {
		if _, ok := m.drivers[name]; !ok {
			m.drivers[name] = driver
		}
	}
}

// RegisterConfig registers a named mutex config.
func (m *Module) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	if name == "" {
		name = infra.DEFAULT
	}
	if infra.Override() {
		m.configs[name] = cfg
	} else {
		if _, ok := m.configs[name]; !ok {
			m.configs[name] = cfg
		}
	}
}

// RegisterConfigs registers multiple named mutex configs.
func (m *Module) RegisterConfigs(configs Configs) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	for name, cfg := range configs {
		if name == "" {
			name = infra.DEFAULT
		}
		if infra.Override() {
			m.configs[name] = cfg
		} else {
			if _, ok := m.configs[name]; !ok {
				m.configs[name] = cfg
			}
		}
	}
}

// Config parses global config for mutex.
func (m *Module) Config(global base.Map) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	cfgAny, ok := global["mutex"]
	if !ok {
		return
	}
	cfgMap, ok := castMap(cfgAny)
	if !ok || cfgMap == nil {
		return
	}

	rootConfig := base.Map{}
	for key, val := range cfgMap {
		if conf, ok := castMap(val); ok && key != "setting" {
			m.configure(key, conf)
		} else {
			rootConfig[key] = val
		}
	}
	if len(rootConfig) > 0 {
		m.configure(infra.DEFAULT, rootConfig)
	}
}

func (m *Module) configure(name string, conf base.Map) {
	cfg := Config{
		Driver:          infra.DEFAULT,
		Weight:          1,
		Expire:          time.Second,
		CleanupInterval: 30 * time.Second,
		TokenGrace:      time.Minute,
	}
	if existing, ok := m.configs[name]; ok {
		cfg = existing
	}

	if v, ok := conf["driver"].(string); ok && v != "" {
		cfg.Driver = v
	}
	if v, ok := conf["weight"].(int); ok {
		cfg.Weight = v
	}
	if v, ok := conf["weight"].(int64); ok {
		cfg.Weight = int(v)
	}
	if v, ok := conf["weight"].(float64); ok {
		cfg.Weight = int(v)
	}
	if v, ok := conf["prefix"].(string); ok {
		cfg.Prefix = v
	}
	if v, ok := conf["expire"]; ok {
		if d := parseDuration(v); d > 0 {
			cfg.Expire = d
		}
	}
	if v, ok := conf["cleanup_interval"]; ok {
		if d := parseDuration(v); d > 0 {
			cfg.CleanupInterval = d
		}
	}
	if v, ok := conf["token_grace"]; ok {
		if d := parseDuration(v); d > 0 {
			cfg.TokenGrace = d
		}
	}
	if v, ok := castMap(conf["setting"]); ok {
		cfg.Setting = v
	}

	m.configs[name] = cfg
}

// Setup initializes defaults.
func (m *Module) Setup() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	if len(m.configs) == 0 {
		m.configs[infra.DEFAULT] = Config{
			Driver:          infra.DEFAULT,
			Weight:          1,
			Expire:          time.Second,
			CleanupInterval: 30 * time.Second,
			TokenGrace:      time.Minute,
		}
	}

	for name, cfg := range m.configs {
		if name == "" {
			name = infra.DEFAULT
		}
		if cfg.Driver == "" {
			cfg.Driver = infra.DEFAULT
		}
		if cfg.Weight == 0 {
			cfg.Weight = 1
		}
		if cfg.Expire <= 0 {
			cfg.Expire = time.Second
		}
		if cfg.CleanupInterval <= 0 {
			cfg.CleanupInterval = 30 * time.Second
		}
		if cfg.TokenGrace <= 0 {
			cfg.TokenGrace = time.Minute
		}
		m.configs[name] = cfg
	}
}

// Open connects all mutex drivers.
func (m *Module) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	if len(m.configs) == 0 {
		panic("Missing mutex config")
	}

	for name, cfg := range m.configs {
		drv, ok := m.drivers[cfg.Driver]
		if !ok || drv == nil {
			panic("Missing mutex driver: " + cfg.Driver)
		}

		inst := &Instance{Name: name, Config: cfg, Setting: cfg.Setting}
		conn, err := drv.Connect(inst)
		if err != nil {
			panic("Failed to conn to mutex: " + err.Error())
		}
		if err := conn.Open(); err != nil {
			panic("Failed to open mutex: " + err.Error())
		}

		inst.conn = conn
		m.instances[name] = inst
		if cfg.Weight > 0 {
			m.weights[name] = cfg.Weight
		}
	}

	m.ring = newHashRing(m.weights)
	m.cleanupInterval = m.resolveCleanupIntervalLocked()
	m.startCleanerLocked()
	m.opened = true
	m.closed = false
}

// Start launches module (no-op).
func (m *Module) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.started {
		return
	}
	m.started = true
	fmt.Printf("infrago mutex module is running with %d connections.\n", len(m.instances))
}

// Stop stops module (no-op).
func (m *Module) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.started {
		return
	}
	m.started = false
}

// Close closes connections.
func (m *Module) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.opened {
		return
	}

	for _, inst := range m.instances {
		if inst.conn != nil {
			_ = inst.conn.Close()
		}
	}
	m.stopCleanerLocked()

	m.instances = make(map[string]*Instance, 0)
	m.weights = make(map[string]int, 0)
	m.ring = nil
	m.cleanupInterval = 0
	m.token.Lock()
	m.tokens = make(map[string][]tokenEntry, 0)
	m.token.Unlock()
	m.opened = false
	m.closed = true
}

func (m *Module) Stats() Statistics {
	m.cleanupExpiredTokens()

	activeTokens := m.activeTokenCounts()
	out := m.stats.snapshot(activeTokens[""])
	out.Instances = map[string]Statistics{}

	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for name, inst := range m.instances {
		out.Instances[name] = inst.stats.snapshot(activeTokens[name])
	}
	return out
}

func (m *Module) StatsFrom(conn string) (Statistics, error) {
	m.cleanupExpiredTokens()
	activeTokens := m.activeTokenCounts()

	m.mutex.RLock()
	defer m.mutex.RUnlock()
	inst, ok := m.instances[conn]
	if !ok {
		return Statistics{}, ErrInvalidConnection
	}
	return inst.stats.snapshot(activeTokens[conn]), nil
}

func (m *Module) ResetStats() {
	m.stats.reset()

	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, inst := range m.instances {
		inst.stats.reset()
	}
}

func (m *Module) Capabilities() map[string]Capability {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	out := make(map[string]Capability, len(m.instances))
	for name, inst := range m.instances {
		out[name] = capabilityOf(inst.conn)
	}
	return out
}

func (m *Module) CapabilityFrom(conn string) (Capability, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	inst, ok := m.instances[conn]
	if !ok {
		return Capability{}, ErrInvalidConnection
	}
	return capabilityOf(inst.conn), nil
}

func (m *Module) Debug() DebugInfo {
	m.cleanupExpiredTokens()
	activeTokens := m.activeTokenCounts()

	info := DebugInfo{
		Tokens:    m.DebugTokens(),
		Stats:     m.stats.snapshot(activeTokens[""]),
		Instances: map[string]DebugInstance{},
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()
	info.Opened = m.opened
	info.Started = m.started
	info.CleanupInterval = m.cleanupInterval

	for name, inst := range m.instances {
		info.Instances[name] = DebugInstance{
			Name:            name,
			Driver:          inst.Config.Driver,
			Weight:          inst.Config.Weight,
			Prefix:          inst.Config.Prefix,
			Expire:          inst.Config.Expire,
			CleanupInterval: inst.Config.CleanupInterval,
			TokenGrace:      inst.Config.TokenGrace,
			Setting:         cloneMap(inst.Config.Setting),
			Capability:      capabilityOf(inst.conn),
			Stats:           inst.stats.snapshot(activeTokens[name]),
		}
	}
	return info
}

func (m *Module) DebugTokens() []DebugToken {
	m.token.Lock()
	defer m.token.Unlock()

	now := time.Now()
	out := make([]DebugToken, 0, len(m.tokens))
	for queueKey, queue := range m.tokens {
		compact := m.compactQueue(queue, now)
		if len(compact) == 0 {
			delete(m.tokens, queueKey)
			continue
		}
		m.tokens[queueKey] = compact

		item := DebugToken{
			Conn:    tokenQueueConn(queueKey),
			Key:     tokenQueueKeyName(queueKey),
			Count:   len(compact),
			Entries: make([]DebugTokenEntry, 0, len(compact)),
		}
		for _, token := range compact {
			active := token.until.After(now)
			if active {
				item.Active++
			} else {
				item.Expired++
			}
			entry := DebugTokenEntry{
				Active:    active,
				Remaining: durationRemaining(now, token.until),
			}
			if !active {
				entry.GraceRemaining = durationRemaining(now, token.until.Add(token.grace))
			}
			item.Entries = append(item.Entries, entry)
		}
		out = append(out, item)
	}
	return out
}

func (m *Module) activeTokenCounts() map[string]int {
	m.token.Lock()
	defer m.token.Unlock()

	now := time.Now()
	counts := map[string]int{"": 0}
	for key, queue := range m.tokens {
		compact := m.compactQueue(queue, now)
		if len(compact) == 0 {
			delete(m.tokens, key)
			continue
		}
		m.tokens[key] = compact
		conn := tokenQueueConn(key)
		for _, item := range compact {
			if item.until.After(now) {
				counts[""]++
				counts[conn]++
			}
		}
	}
	return counts
}

func parseDuration(val base.Any) time.Duration {
	switch v := val.(type) {
	case time.Duration:
		return v
	case int:
		return time.Second * time.Duration(v)
	case int64:
		return time.Second * time.Duration(v)
	case float64:
		return time.Second * time.Duration(v)
	case string:
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 0
}

func castMap(value base.Any) (base.Map, bool) {
	if value == nil {
		return nil, false
	}
	if vv, ok := value.(base.Map); ok {
		return vv, true
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	out := base.Map{}
	iter := rv.MapRange()
	for iter.Next() {
		out[iter.Key().String()] = iter.Value().Interface()
	}
	return out, true
}

func tokenQueueKey(conn, key string) string {
	return conn + "\x00" + key
}

func tokenQueueKeyName(queueKey string) string {
	for i := 0; i < len(queueKey); i++ {
		if queueKey[i] == 0 {
			if i+1 >= len(queueKey) {
				return ""
			}
			return queueKey[i+1:]
		}
	}
	return queueKey
}

func durationRemaining(now, until time.Time) time.Duration {
	if !until.After(now) {
		return 0
	}
	return until.Sub(now)
}

func cloneMap(src base.Map) base.Map {
	if len(src) == 0 {
		return base.Map{}
	}
	dst := make(base.Map, len(src))
	for key, val := range src {
		dst[key] = val
	}
	return dst
}

func (m *Module) startCleanerLocked() {
	if m.cleanerStop != nil {
		return
	}
	if m.cleanupInterval <= 0 {
		m.cleanupInterval = 30 * time.Second
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	m.cleanerStop = stop
	m.cleanerDone = done
	go m.runCleaner(stop, done, m.cleanupInterval)
}

func (m *Module) stopCleanerLocked() {
	if m.cleanerStop == nil {
		return
	}
	close(m.cleanerStop)
	done := m.cleanerDone
	m.cleanerStop = nil
	m.cleanerDone = nil
	<-done
}

func (m *Module) runCleaner(stop <-chan struct{}, done chan<- struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(done)

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			m.cleanupExpiredTokens()
		}
	}
}

func (m *Module) resolveCleanupIntervalLocked() time.Duration {
	interval := time.Duration(0)
	for _, cfg := range m.configs {
		if cfg.CleanupInterval <= 0 {
			continue
		}
		if interval == 0 || cfg.CleanupInterval < interval {
			interval = cfg.CleanupInterval
		}
	}
	if interval <= 0 {
		return 30 * time.Second
	}
	return interval
}
