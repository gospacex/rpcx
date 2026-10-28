// RPC 指标中间件：调用数 / 耗时 / 失败数，经 otel 全局 meter 取用。
//
// 为什么在这里而不是在业务侧：kitex 的 client/server 中间件签名要 kitex 的
// endpoint 类型，业务侧（launchx 库源码）被依赖面守卫禁止直引 kitex，
// 与 trace 中间件落点同理（见 middleware.go 的包注释）。
//
// 取用口径与全生态一致：**向 otel 全局要 meter**（`otel.GetMeterProvider()`），
// 不接收也不交接 provider。装配层没装 MeterProvider 时全局是 noop，
// instrument 是 noop 实现 —— 不产数据、不报错、零开关兜底。
package rpcx

import (
	"context"
	"errors"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metricsInstrumentationName 指标所属的 instrumentation scope。
const metricsInstrumentationName = "github.com/gospacex/rpcx"

// 指标名。前缀 rpcx. 与 webx 的 webx.* 保持同构，便于按 SDK 筛。
const (
	metricRequests = "rpcx.requests"
	metricDuration = "rpcx.request.duration"
	metricErrors   = "rpcx.errors"
)

// 角色标签取值，区分出向与入站。
const (
	roleClient = "client"
	roleServer = "server"
)

// durationBucketBounds 耗时分桶（单位：秒）。
//
// **必须显式给**：OTel SDK 的缺省分桶是 [0, 5, 10, 25, ..., 10000]，那是**毫秒**形状的
// 数值，而本直方图声明的单位是 `s`。两者错配的后果不是报错，而是**分桶全废**：
// 一次 12ms 的调用落在 `le="5"` 这一档里，和一次 4.9s 的调用同桶 —— 面板上看不出
// 快慢，p99 算出来也毫无意义。实测对比：改前 bff 的 GetOrder 全部落在 `le="5"` 一档。
//
// 取值按 RPC 的现实量级铺：亚毫秒到 5 秒，10 秒封顶（再慢已经超了 rpcx 的
// 单调用超时缺省 3s 与入口 webx.timeout，属于该看 errors 而不是 latency 的区间）。
var durationBucketBounds = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// rpcMetrics 一组 RPC instrument。
type rpcMetrics struct {
	requests metric.Int64Counter
	duration metric.Float64Histogram
	errors   metric.Int64Counter
}

// newRPCMetrics 建 instrument；任一失败即返回 nil（all-or-none）。
//
// 不返回半成品：只有 requests 建出来、errors 没有的话，面板上会呈现
// 「有流量、零错误」的假象 —— 比完全没有指标更危险。
func newRPCMetrics() *rpcMetrics {
	meter := otel.GetMeterProvider().Meter(metricsInstrumentationName)
	requests, err := meter.Int64Counter(metricRequests,
		metric.WithDescription("RPC 调用数（出向 + 入站）"),
		metric.WithUnit("{call}"))
	if err != nil {
		return nil
	}
	duration, err := meter.Float64Histogram(metricDuration,
		metric.WithDescription("RPC 调用耗时（秒）"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBucketBounds...))
	if err != nil {
		return nil
	}
	errs, err := meter.Int64Counter(metricErrors,
		metric.WithDescription("RPC 失败数（按 error.type 分类）"),
		metric.WithUnit("{call}"))
	if err != nil {
		return nil
	}
	return &rpcMetrics{requests: requests, duration: duration, errors: errs}
}

// ClientMetricMiddleware 记录出向 RPC 的调用数 / 耗时 / 失败数。
//
// 在 BuildClientOptions 里**最后**注册 = 链路最外层：量到的是「这一次业务调用
// 的总代价」（含被它包住的续链开销与重试）。放内层会量成"最后一次尝试的耗时"，
// 那正是排查超时最不该看错的数。
func ClientMetricMiddleware() endpoint.Middleware { return metricMiddleware(roleClient) }

// ServerMetricMiddleware 记录入站 RPC 的调用数 / 耗时 / 失败数。
func ServerMetricMiddleware() endpoint.Middleware { return metricMiddleware(roleServer) }

// metricMiddleware 产出指标中间件；instrument 建不出来时返回空中间件。
//
// 返回 dummy 而不是 panic / 返回 nil：指标是观测面，不能成为调用链的单点故障；
// 但也不挂一个"每请求白算一遍属性却什么都不记"的空壳。
func metricMiddleware(role string) endpoint.Middleware {
	m := newRPCMetrics()
	if m == nil {
		return endpoint.DummyMiddleware
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) (err error) {
			start := time.Now()
			err = next(ctx, req, resp)
			attrs := rpcAttrs(ctx, role)
			m.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
			m.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
			if err != nil {
				// error.type 只挂在 errors 上：把失败原因混进 requests/duration
				// 会让「总量」本身变成高基数序列。
				m.errors.Add(ctx, 1, metric.WithAttributes(append(attrs,
					attribute.String("error.type", errorType(err)))...))
			}
			return err
		}
	}
}

// rpcAttrs 取调用标签。坐标取不到时只给 role，绝不补一个"猜出来的"服务名
// —— 面板上的 `service="unknown"` 比缺标签更容易误导。
func rpcAttrs(ctx context.Context, role string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("rpc.system", "kitex"),
		attribute.String("rpc.role", role),
	}
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil {
		return attrs
	}
	if to := ri.To(); to != nil {
		if svc := to.ServiceName(); svc != "" {
			attrs = append(attrs, attribute.String("rpc.service", svc))
		}
		if m := to.Method(); m != "" {
			attrs = append(attrs, attribute.String("rpc.method", m))
		}
	}
	return attrs
}

// errorType 把错误归到**基数受控**的几类。
//
// 刻意不下钻业务码（如 4001/4004）：业务码是开放集合（新增一个码就多一条时间
// 序列），而 error.type 是要长期聚合的标签。业务码的区分留给 span 与日志。
//
// 分类顺序有意义：熔断本身就意味着上游在失败，先判它才不会被 IsTimeoutError
// 之类的宽判据吞掉。
func errorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, kerrors.ErrCircuitBreak):
		return "circuit_break"
	case kerrors.IsTimeoutError(err):
		return "timeout"
	}
	// 业务拒绝：跨进程回来的 biz error 在客户端是 *kerrors.BizStatusError，
	// 必须与传输层失败分开 —— 否则一次"订单不存在"会把错误率面板顶红。
	var bizErr *kerrors.BizStatusError
	if errors.As(err, &bizErr) {
		return "biz"
	}
	return "other"
}
