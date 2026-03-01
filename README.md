# mutex

`mutex` 是 infrago 的模块包。

## 安装

```bash
go get github.com/infrago/mutex@latest
```

## 最小接入

```go
package main

import (
    _ "github.com/infrago/mutex"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[mutex]
driver = "default"
```

## 公开 API（摘自源码）

- `func (m *Module) LockOn(conn string, key string, expires ...time.Duration) error`
- `func (m *Module) Lock(key string, expires ...time.Duration) error`
- `func (m *Module) UnlockOn(conn, key string) error`
- `func (m *Module) Unlock(key string) error`
- `func (d *defaultDriver) Connect(inst *Instance) (Connection, error)`
- `func (c *defaultConnect) Open() error`
- `func (c *defaultConnect) Close() error`
- `func (c *defaultConnect) Lock(key string, expire time.Duration) error`
- `func (c *defaultConnect) Unlock(key string) error`
- `func (r *hashRing) Locate(key string) string`
- `func Key(args ...base.Any) string`
- `func LockOn(conn string, args ...base.Any) (*locker, error)`
- `func UnlockOn(conn string, args ...base.Any) error`
- `func LockedOn(conn string, args ...base.Any) bool`
- `func Lock(args ...base.Any) (*locker, error)`
- `func Unlock(args ...base.Any) error`
- `func Locked(args ...base.Any) bool`
- `func (l *locker) Unlock() error`
- `func (m *Module) Register(name string, value base.Any)`
- `func (m *Module) RegisterDriver(name string, driver Driver)`
- `func (m *Module) RegisterConfig(name string, cfg Config)`
- `func (m *Module) RegisterConfigs(configs Configs)`
- `func (m *Module) Config(global base.Map)`
- `func (m *Module) Setup()`
- `func (m *Module) Open()`
- `func (m *Module) Start()`
- `func (m *Module) Stop()`
- `func (m *Module) Close()`

## 排错

- 模块未运行：确认空导入已存在
- driver 无效：确认驱动包已引入
- 配置不生效：检查配置段名是否为 `[mutex]`
