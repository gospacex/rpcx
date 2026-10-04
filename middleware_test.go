package rpcx

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/utils"
	kitextracing "github.com/kitex-contrib/obs-opentelemetry/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// 这些测试锁的是本包立项要修的那个 bug：client span 必须挂在调用方的业务
// span 上，而不是挂在 kitex suite 那个「能续上 trace_id、父 span id 却指向
// 从不存在的 span」的 span 上。detachKitexSpan / ClientSpanMiddleware 一旦
// 回归，这里会先炸。

// newTestProvider 装一个带 SpanRecorder 的全局 provider + TraceContext 传播器。
func newTestProvider(t *testing.T) (*tracetest.SpanRecorder, func()) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	oldTP, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sr, func() { otel.SetTracerProvider(oldTP); otel.SetTextMapPropagator(oldProp) }
}

// newClientRPCInfo 造一份带 To() 的 rpcinfo，够中间件取出 service/method。
func newClientRPCInfo(service, method string) rpcinfo.RPCInfo {
	return rpcinfo.NewRPCInfo(
		rpcinfo.NewEndpointInfo("caller", "", nil, nil),
		rpcinfo.NewEndpointInfo(service, method, nil, nil),
		rpcinfo.NewInvocation(service, method),
		rpcinfo.NewRPCConfig(),
		rpcinfo.NewRPCStats(),
	)
}

// kitexSpanCtx 走 suite 的真实装配路径产出一个 kitex span 的 ctx。
//
// 刻意不手搓 scope 名：走 NewClientSuite → stats.Tracer.Start，录到的就是
// 上游真正用的那个 instrumentation scope（实测为
// "github.com/kitex-contrib/obs-opentelemetry"）。上游改了名而本包的判定
// 标记没跟上，这个测试会先失败。
func kitexSpanCtx(t *testing.T, parent context.Context, ri rpcinfo.RPCInfo) context.Context {
	t.Helper()
	o := client.Options{Configs: rpcinfo.NewRPCConfig()}
	var di utils.Slice
	for _, opt := range kitextracing.NewClientSuite().Options() {
		opt.F(&o, &di)
	}
	if o.TracerCtl == nil || !o.TracerCtl.HasTracer() {
		t.Fatal("suite 没有装上 stats.Tracer，产不出 kitex span")
	}
	// suite 的 span 名取自 rpcinfo，缺了会 panic：这里统一补上。
	return o.TracerCtl.DoStart(rpcinfo.NewCtxWithRPCInfo(parent, ri), ri)
}

func TestDetachKitexSpanDetachesKitexSpan(t *testing.T) {
	_, done := newTestProvider(t)
	defer done()

	bizCtx, bizSpan := otel.GetTracerProvider().Tracer("business").Start(context.Background(), "handler.biz")
	defer bizSpan.End()

	ctx := kitexSpanCtx(t, bizCtx, newClientRPCInfo("svc-order", "GetOrder"))
	kitexSpan := oteltrace.SpanFromContext(ctx)
	if !isKitexSpan(kitexSpan) {
		t.Fatalf("isKitexSpan 认不出 suite 产出的 span（scope=%q），后续 detach 判据失效",
			kitexSpan.(sdktrace.ReadOnlySpan).InstrumentationScope().Name)
	}

	got := oteltrace.SpanFromContext(detachKitexSpan(ctx)).SpanContext()
	if want := bizSpan.SpanContext(); got.SpanID() != want.SpanID() || got.TraceID() != want.TraceID() {
		t.Fatalf("detachKitexSpan 后 span context = %s/%s，want 业务 span %s/%s（kitex span 未被剥离）",
			got.TraceID(), got.SpanID(), want.TraceID(), want.SpanID())
	}
	if kitexID := kitexSpan.SpanContext().SpanID(); got.SpanID() == kitexID {
		t.Fatalf("detachKitexSpan 仍指向 kitex span %s", kitexID)
	}
}

// 反向锁：ctx 里是普通业务 span 时绝不能剥离。旧实现「是 SDK span 且有 Parent
// 就上跳」会在这里把 client span 挂到祖父节点上，静默重现断链。
func TestDetachKitexSpanKeepsBusinessSpan(t *testing.T) {
	_, done := newTestProvider(t)
	defer done()
	tracer := otel.GetTracerProvider().Tracer("business")

	// 1. 根业务 span：没有父可上跳，必须原样保留。
	rootCtx, rootSpan := tracer.Start(context.Background(), "handler.root")
	defer rootSpan.End()
	if got := oteltrace.SpanFromContext(detachKitexSpan(rootCtx)).SpanContext(); got.SpanID() != rootSpan.SpanContext().SpanID() {
		t.Fatalf("根业务 span 被剥离：got %s want %s", got.SpanID(), rootSpan.SpanContext().SpanID())
	}

	// 2. 嵌套业务 span：有有效 Parent，但不是 kitex span —— 不得上跳。
	childCtx, childSpan := tracer.Start(rootCtx, "service.child")
	defer childSpan.End()
	got := oteltrace.SpanFromContext(detachKitexSpan(childCtx)).SpanContext()
	if got.SpanID() != childSpan.SpanContext().SpanID() {
		t.Fatalf("嵌套业务 span 被误判为 kitex span 并上跳：got %s want %s",
			got.SpanID(), childSpan.SpanContext().SpanID())
	}
}

func TestDetachKitexSpanWithoutSpan(t *testing.T) {
	// ctx 里没有 span 时 SpanFromContext 返回 noopSpan（非 nil），
	// 不得 panic，且剥离结果同样无效 —— 后续 Start 产出根 span。
	if got := oteltrace.SpanFromContext(detachKitexSpan(context.Background())).SpanContext(); got.IsValid() {
		t.Fatalf("空 ctx 剥离出有效 span context %s", got.SpanID())
	}
}

func TestInjectSpanContext(t *testing.T) {
	_, done := newTestProvider(t)
	defer done()

	_, span := otel.GetTracerProvider().Tracer("business").Start(context.Background(), "handler.biz")
	defer span.End()

	ctx := injectSpanContext(oteltrace.ContextWithSpan(context.Background(), span))

	raw, ok := metainfo.GetValue(ctx, "traceparent")
	if !ok {
		t.Fatal("injectSpanContext 没写出 traceparent")
	}
	// 反解出来必须就是本 span：下游 extract 到的父为此 span，续链才不断。
	got := oteltrace.SpanContextFromContext(
		propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": raw}))
	if want := span.SpanContext(); got.SpanID() != want.SpanID() || got.TraceID() != want.TraceID() {
		t.Fatalf("traceparent %q 反解为 %s/%s，want %s/%s", raw, got.TraceID(), got.SpanID(), want.TraceID(), want.SpanID())
	}
	// 只搬传播键，不复制业务头：TraceContext 在無 tracestate 时只写 traceparent。
	if all := metainfo.GetAllValues(ctx); len(all) != 1 {
		t.Fatalf("注入后 metainfo = %v，want 只含 traceparent", all)
	}
}

func attr(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}

func TestClientSpanMiddleware(t *testing.T) {
	sr, done := newTestProvider(t)
	defer done()

	parentCtx, parent := otel.GetTracerProvider().Tracer("business").Start(context.Background(), "handler.biz")
	defer parent.End()

	var downstream context.Context
	ep := ClientSpanMiddleware()(func(ctx context.Context, req, resp interface{}) error {
		downstream = ctx
		return nil
	})
	if err := ep(rpcinfo.NewCtxWithRPCInfo(parentCtx, newClientRPCInfo("svc-order", "GetOrder")), nil, nil); err != nil {
		t.Fatalf("middleware returned err: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	s := spans[0]
	if want := "client.svc-order.GetOrder"; s.Name() != want {
		t.Errorf("span name = %q, want %q", s.Name(), want)
	}
	if s.SpanKind() != oteltrace.SpanKindClient {
		t.Errorf("span kind = %v, want client", s.SpanKind())
	}
	if s.Parent().SpanID() != parent.SpanContext().SpanID() || s.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("span parent = %s/%s，want 与业务 span 同 trace 且父为 %s（断链）",
			s.SpanContext().TraceID(), s.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	if got := attr(s.Attributes(), "rpc.system"); got != "kitex" {
		t.Errorf("attr rpc.system = %q, want kitex", got)
	}
	if got := attr(s.Attributes(), "rpc.service"); got != "svc-order" {
		t.Errorf("attr rpc.service = %q, want svc-order", got)
	}
	if got := attr(s.Attributes(), "rpc.method"); got != "GetOrder" {
		t.Errorf("attr rpc.method = %q, want GetOrder", got)
	}
	// 下发侧的 traceparent 必须是本 span，而不是上游那个不落地的 span。
	raw, ok := metainfo.GetValue(downstream, "traceparent")
	if !ok {
		t.Fatal("middleware 没往下游注入 traceparent")
	}
	if !strings.Contains(raw, s.SpanContext().SpanID().String()) {
		t.Fatalf("下游 traceparent %q 不含本 span id %s", raw, s.SpanContext().SpanID())
	}
}

// 端到端锁断链修复：ctx 里夹着 kitex span 时，client span 仍挂在业务 span 上。
func TestClientSpanMiddlewareSkipsKitexSpan(t *testing.T) {
	sr, done := newTestProvider(t)
	defer done()

	parentCtx, parent := otel.GetTracerProvider().Tracer("business").Start(context.Background(), "handler.biz")
	defer parent.End()
	ri := newClientRPCInfo("svc-order", "GetOrder")
	kCtx := kitexSpanCtx(t, parentCtx, ri)

	ep := ClientSpanMiddleware()(func(ctx context.Context, req, resp interface{}) error { return nil })
	if err := ep(rpcinfo.NewCtxWithRPCInfo(kCtx, ri), nil, nil); err != nil {
		t.Fatalf("middleware returned err: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatalf("client span 挂在 %s，want 业务 span %s（kitex span 未剥离，断链重现）",
			s.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	if s.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("trace id = %s, want %s", s.SpanContext().TraceID(), parent.SpanContext().TraceID())
	}
}

func TestClientSpanMiddlewareWithoutRPCInfo(t *testing.T) {
	sr, done := newTestProvider(t)
	defer done()

	ep := ClientSpanMiddleware()(func(ctx context.Context, req, resp interface{}) error { return nil })
	if err := ep(context.Background(), nil, nil); err != nil {
		t.Fatalf("middleware returned err: %v", err)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if got := spans[0].Name(); got != "client.rpc" {
		t.Fatalf("span name = %q, want client.rpc（rpcinfo 缺失时的兜底）", got)
	}
	if len(spans[0].Attributes()) != 0 {
		t.Fatalf("rpcinfo 缺失时不应写 rpc.* 属性，got %v", spans[0].Attributes())
	}
}

func TestClientSpanMiddlewareRecordsError(t *testing.T) {
	sr, done := newTestProvider(t)
	defer done()

	boom := errors.New("boom")
	ep := ClientSpanMiddleware()(func(ctx context.Context, req, resp interface{}) error { return boom })
	if err := ep(rpcinfo.NewCtxWithRPCInfo(context.Background(), newClientRPCInfo("svc-order", "GetOrder")), nil, nil); err != boom {
		t.Fatalf("middleware err = %v, want %v", err, boom)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if spans[0].Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", spans[0].Status().Code)
	}
	if len(spans[0].Events()) == 0 {
		t.Fatal("出错时没有记录 exception 事件")
	}
}
