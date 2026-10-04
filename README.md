# rpcx

kitex 服务端 / 客户端 `Option` 的装配层，hubx 家族第 15 个 `*x` SDK。

本包不做 RPC 框架、不做传输，只回答一个问题：**一个 kitex server 或 client
到底该带哪些 Option**。`Addr`、`RegistryAddr`、`Timeout`、`TraceEnabled` 这些
装配期关注点被收进 `ServerOptions` / `ClientOptions` 两个结构，调用方填业务
字段，由本包翻译成 kitex 的 `server.Option` / `client.Option` 列表。

依赖面刻意很窄：kitex 官方 API + `kitex-contrib/obs-opentelemetry` 的 tracing
suite + `gospacex/coordx`（服务注册与发现）。

## 与其它 rpcx 的关系

1. **由 `micro_apps/pkg/rpcx` 移植而来，并是其收敛目标。** 服务端/客户端
   Option 的装配逻辑、`ClientSpanMiddleware` 的续链修复均来自该包；后续
   `micro_apps/pkg/rpcx` 与本包同时存在时，新改动落本包，那边逐步退化为本包
   的 alias。
2. **与 `gospacex/rpcx`（裸模块名、依赖 regdisx）无关，不得混用。** 两者模块
   路径相近但注册中心实现不同，混用会在同一进程里引出两套注册中心。本包的
   模块路径是 `github.com/gospacex/rpcx`，依赖面里没有 `regdisx`。
3. **发现层走 coordx，新增注册中心必须经 coordx 扩展。** 本包不含任何注册中心
   客户端：注册/发现的实际连接由 coordx 的驱动工厂按 `Config.Driver` 解析
   （当前 blank import `gospacex/coordx/consul`）。要支持新的注册中心，请在
   coordx 侧实现驱动并在本包 blank import，**不要**在本包直接依赖注册中心库。

## 用法

```go
// 服务端（launchx 的 srv 档）：服务名由 kitex 代码生成的 NewServer 传入，
// 本包只负责它之外的那些 Option。
svr := orderservice.NewServer(handler, rpcx.BuildServerOptions(rpcx.ServerOptions{
    ServiceName:      "srv-order",
    Addr:             "127.0.0.1:9013",
    RegistryAddr:     "127.0.0.1:8500", // 空则不注册
    TraceEnabled:     true,
    ExitWait:         5 * time.Second,  // 0 → 5s
    ReadWriteTimeout: 30 * time.Second, // 0 → 30s
})...)
svr.Run()

// 客户端（launchx 的 bff 档）
cli := orderservice.MustNewClient("srv-order", rpcx.BuildClientOptions(rpcx.ClientOptions{
    ServiceName:    "bff-web",
    RegistryAddr:   "127.0.0.1:8500", // 优先：经 coordx 发现
    Target:         "127.0.0.1:9013,127.0.0.1:9014", // 兜底：直连
    ConnType:       "short",
    TraceEnabled:   true,
    RPCTimeout:     3 * time.Second,      // 0 → 3s
    ConnectTimeout: 500 * time.Millisecond, // 0 → 500ms
})...)
```

缺省值：RPC 3s、建连 500ms、服务端读写 30s、优雅退出排空 5s。零值不会被当真
——「立即超时」和「无限等待」两种解释都不期望。

发现优先级：`RegistryAddr` 非空 → coordx resolver；否则 `Target` 非空 →
`WithHostPorts` 直连；两者都空则不产出目标选项。

## 链路追踪

全局 `TracerProvider` 必须**先**由宿主（launchx telemetry）装配好，本包只从
`otel.GetTracerProvider()` 取用，绝不自行装配。

客户端在 `TraceEnabled` 时挂**两**个选项，且顺序固定：`obs-opentelemetry` 的
ClientSuite 先，本包的 `ClientSpanMiddleware()` 后。这不是风格问题：

> 只挂 ClientSuite 时（2026-10-02 实测），suite 的 client span 由 kitex 的
> `stats.Tracer` 产出且**从不落地** —— 下游能收到正确的 traceparent（同
> trace_id），父 span id 却指向一个不存在的 span，**span 树在调用边界断成两棵
> 子树**。症状极具迷惑性：Jaeger 里三服务都能查到 trace、trace-id 也相同，
> 只是父子链是断的。

因此 `ClientSpanMiddleware` 不可省、注册顺序不可换（kitex endpoint chain 里
先注册的在内层、后执行，必须让本包的 span 先建好，注入出去的才是它）。

## 注意事项

- **coordx 装配失败会被静默跳过。** `BuildServerOptions` / `BuildClientOptions`
  为了保持「单返回值、直接塞进 kitex 构造器」的形状，不返回 error：
  `coordx.NewRegister` / `NewDiscover` 出错（例如漏了驱动的 blank import、
  地址非法）时，产出的选项列表里没有 `WithRegistry` / `WithResolver`，现象是
  **服务起了但不被发现**、或调用时才报「无可用实例」。排查时先看启动日志里
  是否有 `coordx/consul register initialized` 这类驱动初始化记录，或自行调用
  `coordx.NewRegister` 校验配置。
- **`coordx/consul` 有两处 `init()` 重复注册同名驱动**（`register.go` 与
  `consul.go` 尾部）。向同一个 map 赋同样的值是幂等的，无害；不要在未确认
  coordx 侧是否已清理的情况下依赖「只有一份」。
- consul 驱动当前用 TTL 检查 + 10s 心跳维持 passing，`DeregisterCriticalServiceAfter`
  为 90s；进程被 SIGKILL 时实例最长 90s 后自动摘除。
