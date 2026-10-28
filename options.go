// Package rpcx 收敛 kitex 服务端/客户端 Option 的装配，是 hubx 家族第 15 个 x SDK。
//
// 与 micro_apps/pkg/rpcx 的关系：本包由该包移植而来并作为其收敛目标；
// 与那个「裸模块名 + regdisx」的 rpcx 无关，不得混用。
//
// 服务发现经 coordx：本包不引入 regdisx 或任何其它注册中心实现，
// 否则 hubx 家族会出现两套注册中心。注册/发现的具体驱动由 coordx 的工厂
// 按 Config.Driver 解析（本包 blank import consul 驱动见 server.go），
// 新增注册中心请在 coordx 侧实现驱动，不要在本包直接依赖某个注册中心库。
//
// 调用链：套件挂上 obs-opentelemetry 的 Server/Client Suite 后，kitex 的
// 入站/出站调用会经 propagator 注入与提取 traceparent —— 这是跨服务续链落点。
// 全局 TracerProvider 必须**先**由宿主（launchx telemetry）装配好，
// 本套件只从 otel.GetTracerProvider() 取用，绝不自行装配。
package rpcx

import (
	"strings"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/server"
	kitextracing "github.com/kitex-contrib/obs-opentelemetry/tracing"
)

// 缺省超时。不填 ClientOptions/ServerOptions 的时长字段时用这些值，
// 避免出现「零值 = 立即超时」或「零值 = 无限等待」两种都不期望的行为。
const (
	// defaultRPCTimeout 单 RPC 调用超时（含重试）
	defaultRPCTimeout = 3 * time.Second
	// defaultConnectTimeout 建连超时
	defaultConnectTimeout = 500 * time.Millisecond
	// defaultReadWriteTimeout 服务端读写超时
	defaultReadWriteTimeout = 30 * time.Second
	// defaultExitWait 优雅退出时等待 in-flight 请求排空的上限
	defaultExitWait = 5 * time.Second
)

// ServerOptions kitex 服务端装配参数
type ServerOptions struct {
	// ServiceName 服务名（与 coordx 里的注册名一致）
	ServiceName string
	// Addr 监听地址，形如 "127.0.0.1:9013"
	Addr string
	// RegistryAddr 注册中心地址，形如 "127.0.0.1:8500"；空则不注册。
	// 注册经 coordx，见 newCoordxRegistry。
	RegistryAddr string
	// TraceEnabled 是否挂 obs-opentelemetry 服务端套件（跨服务续链）。
	// 关闭时不装任何中间件与 meta handler：非 recording span 的开销比
	// 中间件本身还小，没必要为一个 noop 链路付出 fixed cost。
	TraceEnabled bool
	// ExitWait 优雅退出排空等待时长，0 用 defaultExitWait
	ExitWait time.Duration
	// ReadWriteTimeout 连接读写超时，0 用 defaultReadWriteTimeout
	ReadWriteTimeout time.Duration
	// MetricsEnabled 是否挂 RPC 服务端指标中间件（见 metrics.go）。
	// 零值 false：与出向一致，默认不挂，既有行为不变。
	MetricsEnabled bool
}

// ClientOptions kitex 客户端装配参数
type ClientOptions struct {
	// ServiceName 调用方自身的服务名（用于 span 的 source peer 属性）
	ServiceName string
	// Target 目标地址，逗号分隔的多实例 "host:port,host:port"。
	// 仅在 RegistryAddr 为空时生效，作为不经注册中心的直连兜底。
	Target string
	// RegistryAddr 注册中心地址；非空则挂 coordx resolver 做服务发现，
	// Target 被忽略。详见 newCoordxResolver。
	RegistryAddr string
	// ConnType 连接形态："short" 短连接 / 其它值或空即长连接（kitex 默认）。
	// 演示环境实例少且 Leader 会漂移，默认短连接更稳：连接断了下一轮新建。
	ConnType string
	// TraceEnabled 同 ServerOptions.TraceEnabled
	TraceEnabled bool
	// RPCTimeout 单调用超时，0 用 defaultRPCTimeout
	RPCTimeout time.Duration
	// ConnectTimeout 建连超时，0 用 defaultConnectTimeout
	ConnectTimeout time.Duration
	// DownstreamService 被调服务名（与调用方 NewClient 时传的服务名一致）。
	// 用途：服务级熔断 key（见 governance.go）与后续诊断；为空时服务级熔断
	// 退到 kitex 自己的缺省阈值，实例级不受影响。
	DownstreamService string
	// MetricsEnabled 是否挂 RPC 指标中间件（见 metrics.go）。
	MetricsEnabled bool

	// 出向治理三件套。**零值全关**，产出的选项列表与引入治理面之前逐字相同。
	// 语义、边界与坑见 governance.go 的包注释。
	Retry          RetryOptions
	CircuitBreaker CircuitBreakerOptions
	LoadBalancer   LoadBalancer
}

func (o ServerOptions) exitWait() time.Duration {
	if o.ExitWait <= 0 {
		return defaultExitWait
	}
	return o.ExitWait
}

func (o ServerOptions) readWriteTimeout() time.Duration {
	if o.ReadWriteTimeout <= 0 {
		return defaultReadWriteTimeout
	}
	return o.ReadWriteTimeout
}

func (o ClientOptions) rpcTimeout() time.Duration {
	if o.RPCTimeout <= 0 {
		return defaultRPCTimeout
	}
	return o.RPCTimeout
}

func (o ClientOptions) connectTimeout() time.Duration {
	if o.ConnectTimeout <= 0 {
		return defaultConnectTimeout
	}
	return o.ConnectTimeout
}

// BuildServerOptions 生成 kitex server.NewServer 的选项列表。
//
// 注意顺序：ServiceAddr 必须在 tracing suite 之前注册 —— suite 内部也注册
// option，后注册的同位配置会覆盖前者，把地址放在前面可保证它不会被 suite
// 的传输设置冲掉。
//
// 注册失败会静默跳过：coordx.NewRegister 出错（驱动未注册、地址非法等）时
// 本函数不返回错误，产出的选项列表不含 WithRegistry，服务**不会被发现**。
// 保持单返回值签名是刻意的（与 kitex 的 Option 装配口一致），调用方若需要
// 感知注册可用性，请在启动日志里核对监听地址出现在注册中心，或自行调用
// coordx.NewRegister 校验。
func BuildServerOptions(o ServerOptions) []server.Option {
	opts := []server.Option{}
	if o.Addr != "" {
		opts = append(opts, server.WithServiceAddr(utils.NewNetAddr("tcp", o.Addr)))
	}
	opts = append(opts,
		server.WithExitWaitTime(o.exitWait()),
		server.WithReadWriteTimeout(o.readWriteTimeout()),
	)
	if o.RegistryAddr != "" {
		if r, err := newCoordxRegistry(o.RegistryAddr, o.TraceEnabled, o.ServiceName, o.Addr); err == nil {
			opts = append(opts, server.WithRegistry(r))
		}
	}
	if o.TraceEnabled {
		opts = append(opts, server.WithSuite(kitextracing.NewServerSuite()))
	}
	// 指标中间件放最后 = 链路最外层：量到的是「这一次入站调用的总代价」。
	if o.MetricsEnabled {
		opts = append(opts, server.WithMiddleware(ServerMetricMiddleware()))
	}
	return opts
}

// BuildClientOptions 生成 kitex client.NewClient 的选项列表。
//
// 发现优先级：RegistryAddr 非空走 coordx resolver；否则 Target 非空走
// WithHostPorts 直连；两者都空时不产出任何目标选项，由调用方自行保证目标
// 可达 —— 比给一个空列表报错更易于排查「哪个环节没传目标」。
//
// 跟踪相关的顺序有讲究：suite 必须先注册，本包的 ClientSpanMiddleware 后注册。
// kitex 的 endpoint chain 里「先注册的在内层、后执行」，而 suite 自带的
// ClientMiddleware 负责把 ctx 里的 span 注入给下游——只有本包的 span 先建好，
// 被注入的才是它（详见 middleware.go 的实测记录）。
//
// 与服务端同理：coordx.NewDiscover 出错时静默跳过，不产出 WithResolver。
//
// 除上面这些，本函数还会按配置挂出向治理三件套（熔断 / 失败重试 / 负载均衡，
// 见 governance.go）与 RPC 指标中间件（见 metrics.go）；三者开关默认全关。
func BuildClientOptions(o ClientOptions) []client.Option {
	opts := []client.Option{
		client.WithRPCTimeout(o.rpcTimeout()),
		client.WithConnectTimeout(o.connectTimeout()),
	}
	// 出向治理三件套（熔断 / 重试 / 负载均衡）：默认全关，见 governance.go。
	opts = append(opts, governanceOptions(o)...)
	if o.TraceEnabled {
		opts = append(opts, client.WithSuite(kitextracing.NewClientSuite()))
		opts = append(opts, client.WithMiddleware(ClientSpanMiddleware()))
	}
	// 指标中间件**最后**注册 = 链路最外层（先注册的在内层、后执行）：
	// 它量的是「这一次业务调用的总代价」，含被包在内层的重试。放内层会量成
	// "最后一次尝试的耗时" —— 排查超时时最容易看错的那个数。
	if o.MetricsEnabled {
		opts = append(opts, client.WithMiddleware(ClientMetricMiddleware()))
	}
	switch {
	case o.RegistryAddr != "":
		if r, err := newCoordxResolver(o.RegistryAddr, o.TraceEnabled, o.ServiceName); err == nil {
			opts = append(opts, client.WithResolver(r))
		}
	case o.Target != "":
		if addrs := splitTarget(o.Target); len(addrs) > 0 {
			opts = append(opts, client.WithHostPorts(addrs...))
		}
	}
	// 短连接才需要显式叠加（kitex 默认即长连接 + 默认 IdleConfig），
	// 因此非 short 分支刻意不加任何选项，避免硬编码一份 IdleConfig 后
	// 与框架默认值漂移。
	if strings.EqualFold(strings.TrimSpace(o.ConnType), "short") {
		opts = append(opts, client.WithShortConnection())
	}
	return opts
}

// splitTarget 拆分逗号分隔的目标串，丢弃空片段。
//
// coordx 侧拼出的目标是 joinComma(addrs)，ad-hoc 调用也可能传入带空格的
// "a:1, b:2"，逐段 TrimSpace 比整体 strings.Split 更健壮。
func splitTarget(target string) []string {
	if strings.TrimSpace(target) == "" {
		return nil
	}
	parts := strings.Split(target, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
