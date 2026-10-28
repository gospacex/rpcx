package rpcx

import (
	"context"
	"strings"
	"testing"

	"github.com/cloudwego/kitex/pkg/circuitbreak"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/retry"
)

// TestGovernanceZeroValueAddsNothing 是本文件最重要的一条：
// 治理三件套的**零值必须不挂任何东西** —— 加治理面不能顺手改变既有服务的行为。
//
// 反向探针：把 governanceOptions 里的任一开关判断去掉（改成无条件 append），
// 本用例立刻红。
func TestGovernanceZeroValueAddsNothing(t *testing.T) {
	base := len(BuildClientOptions(ClientOptions{ServiceName: "bff"}))
	// 只填**治理侧**才需要的坐标（被调服务名），不打开任何开关。
	//
	// 刻意不放 Target：它属于既有的发现路径（非空即产出一条 WithHostPorts），
	// 把两者混在一起量，会让本用例分辨不出「多出来的是发现还是治理」。
	withCoords := len(BuildClientOptions(ClientOptions{
		ServiceName:       "bff",
		DownstreamService: "demo-order",
	}))
	if base != withCoords {
		t.Fatalf("只填被调服务名就多出选项：base=%d withCoords=%d（治理面不得有隐式开关）", base, withCoords)
	}

	got := len(BuildClientOptions(ClientOptions{
		ServiceName:       "bff",
		DownstreamService: "demo-order",
		Retry:             RetryOptions{},          // 零值
		CircuitBreaker:    CircuitBreakerOptions{}, // 零值
		LoadBalancer:      LoadBalanceDefault,      // 零值
	}))
	if got != base {
		t.Fatalf("零值治理加了 %d 个选项，want 0（base=%d）", got-base, base)
	}
}

// TestGovernanceTogglesAddOneOptionEach 每个开关恰好加一条，互不吞并。
func TestGovernanceTogglesAddOneOptionEach(t *testing.T) {
	base := len(BuildClientOptions(ClientOptions{ServiceName: "bff"}))
	cases := []struct {
		name string
		in   ClientOptions
	}{
		{"retry", ClientOptions{ServiceName: "bff", Retry: RetryOptions{Enabled: true}}},
		{"circuit_breaker", ClientOptions{ServiceName: "bff", CircuitBreaker: CircuitBreakerOptions{Enabled: true}}},
		{"load_balancer", ClientOptions{ServiceName: "bff", LoadBalancer: LoadBalanceWeightedRoundRobin}},
		{"metrics", ClientOptions{ServiceName: "bff", MetricsEnabled: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := len(BuildClientOptions(c.in)); got != base+1 {
				t.Fatalf("%s 加了 %d 条选项，want 1", c.name, got-base)
			}
		})
	}
	all := len(BuildClientOptions(ClientOptions{
		ServiceName:       "bff",
		DownstreamService: "demo-order",
		Retry:             RetryOptions{Enabled: true},
		CircuitBreaker:    CircuitBreakerOptions{Enabled: true},
		LoadBalancer:      LoadBalanceWeightedRandom,
	}))
	if all != base+3 {
		t.Fatalf("三件套全开加了 %d 条，want 3", all-base)
	}
}

// TestParseLoadBalancer 取值只认闭集：非法值必须报错，且错误信息里带合法取值
// （否则配置写错时，人只能靠猜）。
func TestParseLoadBalancer(t *testing.T) {
	for _, ok := range LoadBalancerNames() {
		if _, err := ParseLoadBalancer(ok); err != nil {
			t.Fatalf("合法取值 %q 被拒：%v", ok, err)
		}
	}
	if got, err := ParseLoadBalancer("  weighted_round_robin  "); err != nil || got != LoadBalanceWeightedRoundRobin {
		t.Fatalf("两侧空白未 Trim：got=%q err=%v", got, err)
	}
	_, err := ParseLoadBalancer("round_robin")
	if err == nil {
		t.Fatal("未识别的负载均衡器必须报错（静默退回默认是最难查的故障形态）")
	}
	for _, want := range []string{"round_robin", "weighted_round_robin", "interleaved_weighted_round_robin"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误信息必须列出合法取值（缺 %q）：%v", want, err)
		}
	}
}

// TestRetryPolicyFields 重试参数真的落进了 kitex 策略，且链式刹车没被关掉。
func TestRetryPolicyFields(t *testing.T) {
	p := RetryOptions{Enabled: true, MaxRetryTimes: 3, MaxDurationMS: 400, BackoffMS: 20}.failurePolicy()
	if p.StopPolicy.MaxRetryTimes != 3 {
		t.Fatalf("MaxRetryTimes = %d, want 3", p.StopPolicy.MaxRetryTimes)
	}
	if p.StopPolicy.MaxDurationMS != 400 {
		t.Fatalf("MaxDurationMS = %d, want 400", p.StopPolicy.MaxDurationMS)
	}
	if p.BackOffPolicy == nil || p.BackOffPolicy.BackOffType != retry.FixedBackOffType {
		t.Fatalf("退避类型 = %+v, want fixed", p.BackOffPolicy)
	}
	if got := p.BackOffPolicy.CfgItems[retry.FixMSBackOffCfgKey]; got != 20 {
		t.Fatalf("固定退避 = %v ms, want 20", got)
	}
	// 链式刹车（上游错误率高即停止重试）是重试放大的兜底，MUST 保持开启。
	if p.StopPolicy.DisableChainStop {
		t.Fatal("DisableChainStop 被打开了：重试放大（retry storm）的兜底没了")
	}
	if p.StopPolicy.CBPolicy.ErrorRate != 0.1 {
		t.Fatalf("链式刹车的错误率 = %v, want kitex 缺省 0.1", p.StopPolicy.CBPolicy.ErrorRate)
	}

	// 越界次数不得 panic（kitex 的 WithMaxRetryTimes 对 >5 直接 panic）。
	over := RetryOptions{Enabled: true, MaxRetryTimes: RetryTimesLimit + 99}.failurePolicy()
	if over.StopPolicy.MaxRetryTimes != 2 {
		t.Fatalf("越界次数应回落到 kitex 缺省 2，实得 %d", over.StopPolicy.MaxRetryTimes)
	}
	// 零值其余字段 = 不动 kitex 缺省。
	def := RetryOptions{Enabled: true}.failurePolicy()
	if def.StopPolicy.MaxDurationMS != 0 || def.BackOffPolicy.BackOffType != retry.NoneBackOffType {
		t.Fatalf("零值字段污染了 kitex 缺省：%+v", def)
	}
}

// TestCircuitBreakerConfig 阈值映射：配了就覆盖，没配就用 kitex 缺省。
func TestCircuitBreakerConfig(t *testing.T) {
	cfg := CircuitBreakerOptions{Enabled: true, ErrRate: 0.2, MinSample: 30}.cbConfig()
	if cfg.ErrRate != 0.2 || cfg.MinSample != 30 || !cfg.Enable {
		t.Fatalf("配置未落到 CBConfig：%+v", cfg)
	}
	def := CircuitBreakerOptions{Enabled: true}.cbConfig()
	kitexDefault := circuitbreak.GetDefaultCBConfig()
	if def.ErrRate != kitexDefault.ErrRate || def.MinSample != kitexDefault.MinSample {
		t.Fatalf("零值字段应沿用 kitex 缺省 %+v，实得 %+v", kitexDefault, def)
	}
	if !def.Enable {
		t.Fatal("Enable 必须为真：开关已由 CircuitBreakerOptions.Enabled 裁决过一次")
	}
}

// TestServiceCBKey 服务级熔断 key 降到「服务粒度」，且坐标不全时不猜。
//
// 这条 key 与服务级 CBConfig 的预置必须严格一致 —— 拼错了不会报错，
// 只会静默回到 kitex 缺省阈值（"配了不生效"的典型形态）。
func TestServiceCBKey(t *testing.T) {
	if got := serviceCBKey("bff", "demo-order"); got != "bff/demo-order" {
		t.Fatalf("serviceCBKey = %q", got)
	}
	for _, c := range [][2]string{{"", "demo-order"}, {"bff", ""}, {"  ", "  "}} {
		if got := serviceCBKey(c[0], c[1]); got != "" {
			t.Fatalf("坐标不全（%q,%q）应返回空串，实得 %q", c[0], c[1], got)
		}
	}
	if got := serviceCBKeyFunc("bff")(nil); got != "" {
		t.Fatalf("rpcinfo 为 nil 时 key = %q, want 空串", got)
	}
}

// TestNewCBSuiteIsPerClient 每个 client 一个套件（kitex 要求），且允许无被调名。
func TestNewCBSuiteIsPerClient(t *testing.T) {
	cfg := circuitbreak.GetDefaultCBConfig()
	a := newCBSuite("bff", "demo-order", cfg)
	b := newCBSuite("bff", "demo-order", cfg)
	if a == b {
		t.Fatal("两次构造返回同一个 CBSuite：熔断状态会跨 client 串味")
	}
	if newCBSuite("bff", "", cfg) == nil {
		t.Fatal("被调名为空时也必须建出套件（实例级熔断不依赖它）")
	}
}

// ErrFake 一个与 kitex 无关的普通错误。
type ErrFake struct{}

func (ErrFake) Error() string { return "boom" }

// TestErrorType 错误分类必须是**基数受控**的闭集。
func TestErrorType(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"熔断", kerrors.ErrInstanceCircuitBreak, "circuit_break"},
		{"超时", kerrors.ErrRPCTimeout, "timeout"},
		{"业务拒绝", kerrors.NewBizStatusError(4004, "stock not enough"), "biz"},
		{"普通错误", ErrFake{}, "other"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := errorType(c.err); got != c.want {
				t.Fatalf("errorType(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
	// 业务码不参与分类：同一类错误、不同码，必须归到同一个 error.type
	//（否则 error.type 会随业务码爆炸）。
	a := errorType(kerrors.NewBizStatusError(4001, "x"))
	b := errorType(kerrors.NewBizStatusError(4004, "y"))
	if a != b {
		t.Fatalf("不同业务码被分成了不同 error.type：%q vs %q（基数不受控）", a, b)
	}
}

// TestMetricMiddlewaresDoNotPanic 未装配 MeterProvider（全局 noop meter）时，
// 指标中间件必须：不 panic、不吞错、照常调用 next。
func TestMetricMiddlewaresDoNotPanic(t *testing.T) {
	cases := map[string]endpoint.Middleware{
		"client": ClientMetricMiddleware(),
		"server": ServerMetricMiddleware(),
	}
	for name, mw := range cases {
		t.Run(name, func(t *testing.T) {
			called := 0
			ok := endpoint.Endpoint(func(context.Context, interface{}, interface{}) error {
				called++
				return nil
			})
			if err := mw(ok)(context.Background(), nil, nil); err != nil {
				t.Fatalf("noop meter 下不应产生错误：%v", err)
			}
			if called != 1 {
				t.Fatal("中间件必须调用 next")
			}

			want := ErrFake{}
			failing := endpoint.Endpoint(func(context.Context, interface{}, interface{}) error {
				return want
			})
			if err := mw(failing)(context.Background(), nil, nil); err != want {
				t.Fatalf("错误必须原样透传，实得 %v", err)
			}
		})
	}
}

// TestDurationBucketBoundsAreSecondsScale 防回归：耗时分桶必须是**秒**量级。
//
// 为什么值得单独立一条用例：这块出问题的症状是"面板上只有一个桶"，不报错、
// 不影响业务、单测也照过 —— 只有真去看 /metrics 的人才会发现。而缺省分桶
// [0,5,10,25,...,10000] 恰好长得像"毫秒"，最容易被顺手删掉显式分桶。
func TestDurationBucketBoundsAreSecondsScale(t *testing.T) {
	if len(durationBucketBounds) < 5 {
		t.Fatalf("分桶太少（%d 个），秒级耗时会全部落进 1-2 档", len(durationBucketBounds))
	}
	for i, b := range durationBucketBounds {
		if b <= 0 {
			t.Fatalf("分桶 %d = %v，必须为正", i, b)
		}
		if i > 0 && b <= durationBucketBounds[i-1] {
			t.Fatalf("分桶必须严格递增：%v 出现在 %v 之后", b, durationBucketBounds[i-1])
		}
	}
	last := durationBucketBounds[len(durationBucketBounds)-1]
	if last > 60 {
		t.Fatalf("最大分桶 = %v 秒，量级不对（看着像毫秒档被直接搬过来了）", last)
	}
	if durationBucketBounds[0] > 0.05 {
		t.Fatalf("最小分桶 = %v 秒，粒度太粗 —— 亚 50ms 的调用会被合并", durationBucketBounds[0])
	}
}
