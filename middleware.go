// Package rpcx 的客户端链路中间件：显式产出 RPC client span 并注入下游。
//
// 为什么不能只依赖 obs-opentelemetry 的 ClientSuite（2026-10-02 实测）：
//
//  1. suite 自带的 client span 由 kitex 的 stats.Tracer 产出，实测从不落地——
//     下游能收到正确的 traceparent（同 trace_id），父 span id 却指向一个
//     从不存在的 span，span 树在调用边界断成两棵子树。
//  2. suite 的 ClientMiddleware 只「转发」ctx 里已有的 span，且它的注入发生在
//     本包 middleware 建 span 之前，注入出去的是上面那个不落地的 span。
//
// 因此这里把两件事都收回本包：span 的生命周期由 defer span.End() 钉死，
// traceparent 由本包自己写进 kitex 的 metainfo（后写覆盖，确保下游续的是
// 本 span）。命名沿用案例的分层约定 handler.* / service.* / client.*。
package rpcx

import (
	"context"
	"strings"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// rpcxInstrumentationName 本包创建的 span 所属的 instrumentation scope。
const rpcxInstrumentationName = "github.com/gospacex/rpcx"

// ClientSpanMiddleware 为每次出站 RPC 产出一个 client span 并完成续链注入。
//
// 命名 client.<目标服务>.<方法>，与服务端 span（由各服务自己的 service 层
// 产出）天然区分，Jaeger 上一眼能看出「谁调谁」。
func ClientSpanMiddleware() endpoint.Middleware {
	tracer := otel.GetTracerProvider().Tracer(rpcxInstrumentationName)
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) (err error) {
			ri := rpcinfo.GetRPCInfo(ctx)
			name := "client.rpc"
			if ri != nil && ri.To() != nil {
				name = "client." + ri.To().ServiceName() + "." + ri.To().Method()
			}

			// 剥离 ctx 中 kitex 那个不落地的 span，改以调用方的业务 span 为父。
			base := detachKitexSpan(ctx)
			base, span := tracer.Start(base, name,
				oteltrace.WithSpanKind(oteltrace.SpanKindClient))
			defer span.End()

			if ri != nil && ri.To() != nil {
				span.SetAttributes(
					attribute.String("rpc.system", "kitex"),
					attribute.String("rpc.service", ri.To().ServiceName()),
					attribute.String("rpc.method", ri.To().Method()),
				)
			}

			// 自己注入：suite 的 ClientMiddleware 已在更早的位置注入过 kitex
			// 那个不落地的 span，这里后写覆盖，下游 extract 到的才是本 span。
			base = injectSpanContext(base)

			if err = next(base, req, resp); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return err
		}
	}
}

// injectSpanContext 用全局传播器把 ctx 的 span context 写进 kitex metainfo。
//
// 只搬运传播器实际写出的键（traceparent / tracestate / baggage），不复制
// 业务头——metainfo 会随请求一并下发，塞无关键只会放大报文。
func injectSpanContext(ctx context.Context) context.Context {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for _, k := range carrier.Keys() {
		ctx = metainfo.WithValue(ctx, k, carrier.Get(k))
	}
	return ctx
}

// detachKitexSpan 把 ctx 当前的 span 换成「只保留其 SpanContext」的形态，
// 使后续 Start 以调用方的业务 span 为父，而不是 kitex 那个不落地的 span。
//
// 只有判定为 kitex span（见 isKitexSpan）才上跳一级到它的 Parent；判定不出
// 就沿用当前 span 自身的 SpanContext 做父——此时它多半就是调用方放进 ctx 的
// 业务 span，本来就是想要的父。
func detachKitexSpan(ctx context.Context) context.Context {
	// ctx 里没有 span 时 SpanFromContext 返回 noopSpan（非 nil），
	// 此时 SpanContext 无效、Parent 无效，会走「沿用」分支产出根 span。
	cur := oteltrace.SpanFromContext(ctx)
	parent := cur.SpanContext()
	if isKitexSpan(cur) {
		parent = cur.(sdktrace.ReadOnlySpan).Parent()
	}
	return oteltrace.ContextWithSpanContext(ctx, parent)
}

// kitexSpanScopeMarkers 判定 kitex span 用的 instrumentation scope 名标记。
//
// 2026-10-05 实测（obs-opentelemetry v0.3.0，走 NewClientSuite 真实装配、
// 用 tracetest 录制）：suite 产出的 span 其
// InstrumentationScope().Name == "github.com/kitex-contrib/obs-opentelemetry"、
// Version == "semver:0.39.0"。这里按「含标记串」而非全串相等来判定，上游
// 换版本号不会让判定失效（失效的后果是静默回到断链）。
var kitexSpanScopeMarkers = []string{"obs-opentelemetry", "kitex"}

// isKitexSpan 判断当前 span 是否 kitex suite 产出的那个不落地的 span。
//
// 判定依据是「身份」而不是「类」：只看 span 是不是 SDK ReadOnlySpan（旧写法）
// 等价于跳过任何 SDK span，一旦 ctx 里的当前 span 恰好就是调用方的业务 span
// （suite 升级后不在该位置建 span、或 tracer 关掉而中间件仍在链上），就会被
// 误当成 kitex span 上跳一级，client span 挂到祖父节点——正是本包要修的断链，
// 且静默发生。因此这里只认 kitex 自己的 scope 名，认不出就返回 false。
func isKitexSpan(span oteltrace.Span) bool {
	ro, ok := span.(sdktrace.ReadOnlySpan)
	if !ok || !ro.Parent().IsValid() {
		return false
	}
	name := ro.InstrumentationScope().Name
	for _, m := range kitexSpanScopeMarkers {
		if strings.Contains(name, m) {
			return true
		}
	}
	return false
}
