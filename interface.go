package mutex

import (
	"fmt"
	"log"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/util"
)

func (this *Module) Register(name string, value Any) {
	switch config := value.(type) {
	case Driver:
		this.Driver(name, config)
	case Config:
		this.Config(name, config)
	case Configs:
		this.Configs(config)
	}
}

func (this *Module) configure(name string, config Map) {
	cfg := Config{
		Driver: DEFAULT, Weight: 1, Expiry: time.Second,
	}
	//如果已经存在了，用现成的改写
	if vv, ok := this.configs[name]; ok {
		cfg = vv
	}

	if driver, ok := config["driver"].(string); ok {
		cfg.Driver = driver
	}

	//分配权重
	if weight, ok := config["weight"].(int); ok {
		cfg.Weight = weight
	}
	if weight, ok := config["weight"].(int64); ok {
		cfg.Weight = int(weight)
	}
	if weight, ok := config["weight"].(float64); ok {
		cfg.Weight = int(weight)
	}

	//默认过期时间，单位秒
	if expiry, ok := config["expiry"].(string); ok {
		dur, err := util.ParseDuration(expiry)
		if err == nil {
			cfg.Expiry = dur
		}
	}
	if expiry, ok := config["expiry"].(int); ok {
		cfg.Expiry = time.Second * time.Duration(expiry)
	}
	if expiry, ok := config["expiry"].(float64); ok {
		cfg.Expiry = time.Second * time.Duration(expiry)
	}

	if setting, ok := config["setting"].(Map); ok {
		cfg.Setting = setting
	}

	//保存配置
	this.configs[name] = cfg
}
func (this *Module) Configure(global Map) {
	var config Map
	if vvv, ok := global["mutex"].(Map); ok {
		config = vvv
	}
	if config == nil {
		return
	}

	//记录上一层的配置，如果有的话
	rootConfig := Map{}

	for key, val := range config {
		if conf, ok := val.(Map); ok && key != "setting" {
			this.configure(key, conf)
		} else {
			rootConfig[key] = val
		}
	}

	if len(rootConfig) > 0 {
		this.configure(infra.DEFAULT, rootConfig)
	}
}
func (this *Module) Initialize() {
	if this.initialized {
		return
	}

	// 如果没有配置任何连接时，默认一个
	if len(this.configs) == 0 {
		this.configs[infra.DEFAULT] = Config{
			Driver: infra.DEFAULT, Weight: 1, Expiry: time.Second,
		}
	} else {
		//session全部分布，不留单独外部连接
		for key, config := range this.configs {
			if config.Weight == 0 {
				config.Weight = 1
			}
			if config.Expiry == 0 {
				config.Expiry = time.Second
			}
			this.configs[key] = config
		}

	}

	this.initialized = true
}
func (this *Module) Connect() {
	if this.connected {
		return
	}

	//记录要参与分布的连接和权重
	weights := make(map[string]int)

	for name, config := range this.configs {
		driver, ok := this.drivers[config.Driver]
		if ok == false {
			panic("Invalid mutex driver: " + config.Driver)
		}

		inst := &Instance{
			nil, name, config, config.Setting,
		}

		// 建立连接
		connect, err := driver.Connect(inst)
		if err != nil {
			panic("Failed to connect to mutex: " + err.Error())
		}

		// 打开连接
		err = connect.Open()
		if err != nil {
			panic("Failed to open mutex connect: " + err.Error())
		}

		inst.connect = connect

		//保存实例
		this.instances[name] = inst

		//只有设置了权重的才参与分布
		if config.Weight > 0 {
			weights[name] = config.Weight
		}
	}

	//hashring分片
	this.weights = weights
	this.hashring = util.NewHashRing(weights)

	this.connected = true
}
func (this *Module) Launch() {
	if this.launched {
		return
	}

	log.Println(fmt.Sprintf("%s MUTEX is running with %d connects.", infra.INFRAGO, len(this.instances)))

	this.launched = true
}
func (this *Module) Terminate() {
	for _, ins := range this.instances {
		ins.connect.Close()
	}

	this.launched = false
	this.connected = false
	this.initialized = false
}
