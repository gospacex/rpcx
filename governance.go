// 出向 RPC 治理配置面：失败重试 / 熔断 / 负载均衡。
//
// 为什么落在 rpcx 而不是业务侧：这三项都要 kitex 的 client.Option 才生效，
// 而业务侧（launchx 的库源码）被依赖面守卫禁止直引 kitex —— 与「组件一律经
// *_x SDK 取用」同一条纪律。故配置面在此收敛，由 launchx 从
// `xmiddleware.rpcx` 段读出后透传（见 launchx/internal/mode/rpc.go）。
//
// 三条口径（都是"别撒谎"）：
//
//  1. **零值 = 不挂**：三个开关默认全关，产出的选项列表与引入本文件之前逐字
//     相同。既有服务的行为不变，不搞「默认开一个温和值」。
//  2. **取值只认闭集**：负载均衡器取值经 ParseLoadBalancer 校验，非法值在
//     装配期即报错，而不是运行期静默退回默认（静默退回最难查：配置写错了却
//     一切正常，直到某天按默认策略打偏）。
//  3. **熔断是两道闸**：服务级（整条下游服务）+ 实例级（单个地址）。kitex 的
//     服务级 key 缺省是 `from/to/method`，逐方法配置在装配期取不到，故本包自带
//     key 函数降到 `from/to`（服务粒度），并在装配期把阈值**预置**进该 key ——
//     否则服务级会沿用它自己的缺省阈值，出现「配了 err_rate=0.1 却按 0.5 熔断」。
package rpcx

import (
	"fmt"
	"strings"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/circuitbreak"
	"github.com/cloudwego/kitex/pkg/loadbalance"
	"github.com/cloudwego/kitex/pkg/retry"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// RetryTimesLimit kitex 对失败重试次数的硬上限（不含首次调用）。
//
// kitex 的 FailurePolicy.WithMaxRetryTimes 对越界值**直接 panic**，
// 故取值必须在装配期校验（launchx 侧拿这个常量做上界判断），
// 核心装配函数里只做防崩收敛，不把 panic 带给调用方。
const RetryTimesLimit = 5

// LoadBalancer 负载均衡器取值（闭集）。
//
// 不给 `consistent_hash`：它需要一个业务侧提供的 key 函数，没有默认值，
// 配了也只会把请求全打到同一个实例上——属于「看着配了、其实更糟」的那类。
type LoadBalancer string

// 合法取值。空串表示用 kitex 缺省（weighted_random）。
const (
	LoadBalanceDefault                       LoadBalancer = ""
	LoadBalanceWeightedRandom                LoadBalancer = "weighted_random"
	LoadBalanceWeightedRoundRobin            LoadBalancer = "weighted_round_robin"
	LoadBalanceInterleavedWeightedRoundRobin LoadBalancer = "interleaved_weighted_round_robin"
)

// LoadBalancerNames 返回合法取值（顺序固定：第 0 个是缺省）。
func LoadBalancerNames() []string {
	return []string{
		string(LoadBalanceDefault),
		string(LoadBalanceWeightedRandom),
		string(LoadBalanceWeightedRoundRobin),
		string(LoadBalanceInterleavedWeightedRoundRobin),
	}
}

// ParseLoadBalancer 校验并解析负载均衡器取值。
//
// 空串合法（= 框架缺省）；其余值必须命中闭集，否则返回带合法取值列表的 error。
// 调用方（launchx 装配层）应把它当**配置错误**即退，不要降级。
func ParseLoadBalancer(s string) (LoadBalancer, error) {
	v := LoadBalancer(strings.TrimSpace(s))
	switch v {
	case LoadBalanceDefault,
		LoadBalanceWeightedRandom,
		LoadBalanceWeightedRoundRobin,
		LoadBalanceInterleavedWeightedRoundRobin:
		return v, nil
	}
	return "", fmt.Errorf("rpcx: 未知负载均衡器 %q，合法取值：%s",
		s, strings.Join(quoteAll(LoadBalancerNames()), " / "))
}

// balancer 按取值建负载均衡器；空串返回 nil（= 不加选项，用框架缺省）。
func (l LoadBalancer) balancer() loadbalance.Loadbalancer {
	switch l {
	case LoadBalanceWeightedRandom:
		return loadbalance.NewWeightedRandomBalancer()
	case LoadBalanceWeightedRoundRobin:
		return loadbalance.NewWeightedRoundRobinBalancer()
	case LoadBalanceInterleavedWeightedRoundRobin:
		return loadbalance.NewInterleavedWeightedRoundRobinBalancer()
	}
	return nil
}

// RetryOptions 失败重试（kitex failure retry）。
//
// 只对**传输层失败**（超时 / 不可达 / 连接重置）重试：kitex 的缺省判据是
// err != nil，业务拒绝（biz error）由 kitex 归到 error 之外的通道，不会重试。
type RetryOptions struct {
	// Enabled 是否开启失败重试；false 时其余字段被忽略。
	Enabled bool
	// MaxRetryTimes 重试次数（不含首次调用）。0 用 kitex 缺省 2；
	// 上限 RetryTimesLimit（kitex 越界即 panic，装配期已校验）。
	MaxRetryTimes int
	// MaxDurationMS 首次调用 + 全部重试的总时长上限（毫秒）。0 = 不设。
	MaxDurationMS uint32
	// BackoffMS 固定退避毫秒。0 = 不退避（kitex NoneBackOffType）。
	BackoffMS int
}

// failurePolicy 产出 kitex 的失败重试策略。
//
// 刻意**不动** StopPolicy.DisableChainStop：kitex 的失败重试自带一条
// 「上游错误率过高就停止重试」的链式刹车（CBPolicy，缺省 error_rate=0.1），
// 这正是重试放大（retry storm）的兜底。把它关掉再叠加本包的熔断，
// 等于把「故障期的放大器」亲手装回去。
func (r RetryOptions) failurePolicy() *retry.FailurePolicy {
	p := retry.NewFailurePolicy()
	if n := r.MaxRetryTimes; n > 0 && n <= RetryTimesLimit {
		p.WithMaxRetryTimes(n)
	}
	if r.MaxDurationMS > 0 {
		p.WithMaxDurationMS(r.MaxDurationMS)
	}
	if r.BackoffMS > 0 {
		p.WithFixedBackOff(r.BackoffMS)
	}
	return p
}

// CircuitBreakerOptions 熔断（kitex 服务级 + 实例级 CBSuite）。
type CircuitBreakerOptions struct {
	// Enabled 是否挂熔断；false 时其余字段被忽略。
	Enabled bool
	// ErrRate 触发熔断的错误率阈值，取值 (0,1]。0 用 kitex 缺省 0.5。
	ErrRate float64
	// MinSample 触发熔断所需的最小样本数。0 用 kitex 缺省 200。
	MinSample int64
}

// cbConfig 产出 CBSuite 的阈值配置（缺省值取 kitex 的，不自造一套）。
func (c CircuitBreakerOptions) cbConfig() circuitbreak.CBConfig {
	cfg := circuitbreak.GetDefaultCBConfig()
	if c.ErrRate > 0 {
		cfg.ErrRate = c.ErrRate
	}
	if c.MinSample > 0 {
		cfg.MinSample = c.MinSample
	}
	return cfg
}

// newCBSuite 建一个熔断套件并**预置**服务级阈值。
//
// 每个 client 必须建一个自己的套件（kitex 文档明确：CBSuite 有实例状态，
// 跨 client 复用会让 A 服务的熔断状态影响 B 服务）。
//
// key 语义：`<调用方服务名>/<被调服务名>`。比 kitex 缺省（再多一段 method）
// 粗一档 —— 换来的是「阈值在装配期就配得上」；逐方法的阈值差异本轮不需要，
// 真需要时应由 kitex 的配置中心接口下发，而不是在装配期猜方法名。
//
// self/downstream 任一为空时无法预置服务级阈值（key 拼不出来），此时只有
// 实例级生效；调用方（launchx）总会传 DownstreamService，故正常路径不会踩到。
func newCBSuite(self, downstream string, cfg circuitbreak.CBConfig) *circuitbreak.CBSuite {
	cbs := circuitbreak.NewCBSuite(serviceCBKeyFunc(self))
	cbs.UpdateInstanceCBConfig(cfg)
	if key := serviceCBKey(self, downstream); key != "" {
		cbs.UpdateServiceCBConfig(key, cfg)
	}
	return cbs
}

// serviceCBKeyFunc 生成服务级熔断 key 的函数（kitex 在每次调用时回调）。
func serviceCBKeyFunc(self string) circuitbreak.GenServiceCBKeyFunc {
	return func(ri rpcinfo.RPCInfo) string {
		if ri == nil || ri.To() == nil {
			return ""
		}
		return serviceCBKey(self, ri.To().ServiceName())
	}
}

// serviceCBKey 拼服务级熔断 key；坐标不全时返回 ""（kitex 会退回其缺省配置）。
func serviceCBKey(self, downstream string) string {
	self, downstream = strings.TrimSpace(self), strings.TrimSpace(downstream)
	if self == "" || downstream == "" {
		return ""
	}
	return self + "/" + downstream
}

// governanceOptions 把治理三件套转成 kitex 客户端选项。
//
// 顺序：熔断 → 重试 → 负载均衡，与「先注册的在内层」的链式语义无关
// （这三者作用在不同环节：负载均衡选实例、重试包住调用、熔断判实例健康），
// 列在这里是为了让 diag 输出可读。
func governanceOptions(o ClientOptions) []client.Option {
	out := make([]client.Option, 0, 3)
	if o.CircuitBreaker.Enabled {
		out = append(out, client.WithCircuitBreaker(
			newCBSuite(o.ServiceName, o.DownstreamService, o.CircuitBreaker.cbConfig())))
	}
	if o.Retry.Enabled {
		out = append(out, client.WithFailureRetry(o.Retry.failurePolicy()))
	}
	if lb := o.LoadBalancer.balancer(); lb != nil {
		out = append(out, client.WithLoadBalancer(lb))
	}
	return out
}

// quoteAll 给取值加引号，空串显式写成 `""`（否则错误信息里会消失）。
func quoteAll(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = fmt.Sprintf("%q", s)
	}
	return out
}
