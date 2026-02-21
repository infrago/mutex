package mutex

import (
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	base "github.com/bamgoo/base"
)

func init() {
	bamgoo.Mount(module)
	bamgoo.Register(bamgoo.DEFAULT, &defaultDriver{})
}

var module = &Module{
	drivers:   make(map[string]Driver, 0),
	configs:   make(map[string]Config, 0),
	instances: make(map[string]*Instance, 0),
	weights:   make(map[string]int, 0),
}

type (
	Module struct {
		mutex sync.RWMutex

		opened  bool
		started bool

		drivers   map[string]Driver
		configs   map[string]Config
		instances map[string]*Instance
		weights   map[string]int
		ring      *hashRing
	}

	Config struct {
		Driver  string
		Weight  int
		Prefix  string
		Expire  time.Duration
		Setting base.Map
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
		name = bamgoo.DEFAULT
	}
	if driver == nil {
		panic("Invalid mutex driver: " + name)
	}
	if _, ok := m.drivers[name]; ok {
		panic("Mutex driver already registered: " + name)
	}
	m.drivers[name] = driver
}

// RegisterConfig registers a named mutex config.
func (m *Module) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	if name == "" {
		name = bamgoo.DEFAULT
	}
	if _, ok := m.configs[name]; ok {
		panic("Mutex config already registered: " + name)
	}
	m.configs[name] = cfg
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
			name = bamgoo.DEFAULT
		}
		if _, ok := m.configs[name]; ok {
			panic("Mutex config already registered: " + name)
		}
		m.configs[name] = cfg
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
	cfgMap, ok := cfgAny.(base.Map)
	if !ok || cfgMap == nil {
		return
	}

	rootConfig := base.Map{}
	for key, val := range cfgMap {
		if conf, ok := val.(base.Map); ok && key != "setting" {
			m.configure(key, conf)
		} else {
			rootConfig[key] = val
		}
	}
	if len(rootConfig) > 0 {
		m.configure(bamgoo.DEFAULT, rootConfig)
	}
}

func (m *Module) configure(name string, conf base.Map) {
	cfg := Config{Driver: bamgoo.DEFAULT, Weight: 1, Expire: time.Second}
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
	if v, ok := conf["setting"].(base.Map); ok {
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
		m.configs[bamgoo.DEFAULT] = Config{Driver: bamgoo.DEFAULT, Weight: 1, Expire: time.Second}
	}

	for name, cfg := range m.configs {
		if name == "" {
			name = bamgoo.DEFAULT
		}
		if cfg.Driver == "" {
			cfg.Driver = bamgoo.DEFAULT
		}
		if cfg.Weight == 0 {
			cfg.Weight = 1
		}
		if cfg.Expire == 0 {
			cfg.Expire = time.Second
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
	m.opened = true
}

// Start launches module (no-op).
func (m *Module) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.started {
		return
	}
	m.started = true
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

	m.instances = make(map[string]*Instance, 0)
	m.weights = make(map[string]int, 0)
	m.ring = nil
	m.opened = false
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
