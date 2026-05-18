# mutex

`mutex` 是 infrago 的**模块**。

## 包定位

- 类型：模块
- 作用：分布式锁模块，负责锁获取/释放与并发互斥。

## 主要功能

- 对上提供统一模块接口
- 对下通过驱动接口接入具体后端
- 支持按配置切换驱动实现
- 支持非破坏式 `Locked()` 锁状态检查
- 支持 `Check()` / `CheckOn()` 返回明确错误，区分“锁已存在”和“后端检查失败”
- 支持 `TryLock()` / `WaitLock()` / `WaitLockContext()` 显式控制即时抢锁与等待抢锁
- 支持 `Refresh()` / `RefreshOn()` 续租，适合长任务按需延长租约
- 支持 `locker.KeepAlive()` 自动续租，适合长任务
- 支持 `Stats()` / `StatsFrom()` / `ResetStats()` 基础运行指标
- 支持 `Capabilities()` / `CapabilityFrom()` 显式查看实例能力
- 支持 `Debug()` / `DebugTokens()` 只读调试导出
- 支持 `KeyWith()` 统一构造锁 key
- `Lock()` 返回的 locker 具备 token-aware 解锁语义，过期旧锁不会误删新锁

## 快速接入

```go
import _ "github.com/infrago/mutex"
```

```toml
[mutex]
driver = "default"
expire = "3s"
cleanup_interval = "30s"
token_grace = "1m"
```

## 驱动实现接口列表

以下接口由驱动实现（来自模块 `driver.go`）：

### Driver

- `Connect(*Instance) (Connection, error)`

### Connection

- `Open() error`
- `Close() error`
- `Lock(key string, expires time.Duration) error`
- `Unlock(key string) error`

### 可选接口

- `Checker`：`Locked(key string) (bool, error)`
- `Refresher`：`Refresh(key string, expires time.Duration) error`
- `TokenConnection`：`LockToken(key string, expires time.Duration) (string, error)` / `UnlockToken(key, token string) error`
- `TokenRefresher`：`RefreshToken(key, token string, expires time.Duration) error`
- `CapabilityProvider`：`Capabilities() Capability`

## 全局配置项（所有配置键）

配置段：`[mutex]`

- `driver`
- `weight`
- `prefix`
- `expire`
- `cleanup_interval`
- `token_grace`
- `setting`

## 说明

- `setting` 一般用于向具体驱动透传专用参数
- 多实例配置请参考模块源码中的 Config/configure 处理逻辑
- `Locked()` 现在是只读检查，不再通过“试抢锁再释放”实现
- `Locked()` / `LockedOn()` 为兼容旧接口，遇到检查错误时仍会保守返回 `true`
- 需要区分“真的已加锁”还是“检查失败”时，请使用 `Check()` / `CheckOn()`
- `TryLock()` 语义与当前 `Lock()` 一致，都是“立即尝试，失败直接返回”
- `WaitLock()` 在超时前会按 interval 轮询抢锁，超时返回 `ErrTimeout`
- `WaitLockContext()` 支持外部 `context.Context` 取消；超时或取消会直接返回 `ctx.Err()`
- 长任务建议在持锁期间周期性 `Refresh()`，否则超过 `expire` 后锁可能被其它节点重新拿到
- 显式传入负数 lease 会返回 `ErrInvalidLease`
- 模块关闭后再操作会返回 `ErrClosed`
- 后端超时会尽量归一到 `ErrTimeout`
- 推荐使用 `lok, err := mutex.Lock(...)` 后调用 `lok.Unlock()`；这条路径会保留锁 token，能避免过期旧 locker 误删新锁
- `lok.Refresh(...)` 会沿用该 locker 的 token 做精确续租，比无 token helper 更安全
- `lok.KeepAlive(interval)` 会后台自动续租；`Unlock()` 时会自动停止
- `KeyWith(":", "order", id, "pay")` 可以生成更稳定的层级 key，便于 contention 统计聚合
- `mutex.Unlock(...)` / `mutex.UnlockOn(...)` 现在也会优先按本进程记录的 token 安全解锁；如果当前驱动要求 token 但本地没有对应 token，会返回 `ErrTokenRequired`
- `cleanup_interval` 控制主包后台 token 清理周期，默认 `30s`
- `token_grace` 控制过期 token 的兼容保留窗口，默认 `1m`；超出该窗口后会被后台清理
- 为了兼容旧 helper 的延迟释放场景，主包会短暂保留最近过期的本地 token；`Stats()` 中的 `ActiveTokens` 只统计仍然有效的 token
- `Stats()` 返回模块汇总和实例级统计，当前包含 `Lock/Unlock/Refresh/Check/Contention/Error/Cleanup/ActiveTokens`
- contention 统计细分到 `ContentionByKey`、`ContentionByPrefix` 和 `HotKeys`
- `Capabilities()` 会按实例返回 `Check/Refresh/Token/TokenRefresh` 能力；默认会从驱动实现的接口自动识别，驱动也可以实现 `CapabilityProvider` 覆盖
- `Debug()` 返回模块状态、实例配置、能力、统计和 token 调试快照；`DebugTokens()` 只返回 token 计数和剩余时间，不暴露 token 原值
- `Debug()` / `DebugTokens()` 只建议用于排查和观测，不建议作为业务逻辑依赖
