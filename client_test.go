package rpcx

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/gospacex/coordx"
)

// fakeDiscover 顶替 coordx.Discover，让发现适配不依赖 consul。
type fakeDiscover struct {
	insts    []*coordx.ServiceInstance
	services []string
	err      error
}

func (f *fakeDiscover) Discovery(_ context.Context, service string) ([]*coordx.ServiceInstance, error) {
	f.services = append(f.services, service)
	return f.insts, f.err
}

func (f *fakeDiscover) Watch(_ context.Context, _ string, _ func([]*coordx.ServiceInstance)) error {
	return nil
}

func (f *fakeDiscover) HealthCheck() error { return nil }
func (f *fakeDiscover) Close() error       { return nil }

func TestCoordxResolverTarget(t *testing.T) {
	r := &coordxResolver{service: "self-svc"}
	cases := []struct {
		name   string
		target rpcinfo.EndpointInfo
		want   string
	}{
		{"被调方有服务名", rpcinfo.NewEndpointInfo("svc-order", "GetOrder", nil, nil), "svc-order"},
		{"被调方无服务名", rpcinfo.NewEndpointInfo("", "GetOrder", nil, nil), "self-svc"},
		{"target 为 nil", nil, "self-svc"},
	}
	for _, c := range cases {
		if got := r.Target(context.Background(), c.target); got != c.want {
			t.Errorf("%s: Target = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestCoordxResolverResolve(t *testing.T) {
	// 坐标不全的实例必须被跳过：它们拼出来是 ":0"，进了负载均衡只会让
	// 调用方在「看起来发现了实例」的情况下拿到建连失败。
	fake := &fakeDiscover{insts: []*coordx.ServiceInstance{
		nil,
		{ID: "ok-1", Addr: "10.0.0.1", Port: 9013, Metadata: map[string]string{"zone": "a"}},
		{ID: "no-addr", Port: 9014},
		{ID: "no-port", Addr: "10.0.0.3"},
		{ID: "ok-2", Addr: "10.0.0.2", Port: 9015},
	}}
	r := &coordxResolver{disc: fake, service: "self-svc"}

	res, err := r.Resolve(context.Background(), "svc-order")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Instances) != 2 {
		t.Fatalf("Resolve 返回 %d 个实例，want 2（坐标不全的应被丢弃）", len(res.Instances))
	}
	for i, want := range []string{"10.0.0.1:9013", "10.0.0.2:9015"} {
		got := res.Instances[i].Address().String()
		if !strings.Contains(got, want) {
			t.Errorf("instances[%d] 地址 = %q, want 含 %q", i, got, want)
		}
	}
	if res.Cacheable {
		t.Error("Cacheable=true，实例列表会被 kitex 缓存住，热度数据不再新鲜")
	}
	if res.CacheKey != "svc-order" {
		t.Errorf("CacheKey = %q, want svc-order", res.CacheKey)
	}
	if fake.services[0] != "svc-order" {
		t.Errorf("传给 coordx 的服务名 = %q, want svc-order", fake.services[0])
	}

	// desc 为空时回落到构造时的服务名。
	if _, err := r.Resolve(context.Background(), ""); err != nil {
		t.Fatalf("Resolve(空 desc): %v", err)
	}
	if got := fake.services[len(fake.services)-1]; got != "self-svc" {
		t.Errorf("空 desc 时用 %q 发现，want 回落 self-svc", got)
	}

	// 空结果：不报错，实例切片为空但非 nil。
	empty := &coordxResolver{disc: &fakeDiscover{}}
	res, err = empty.Resolve(context.Background(), "svc-order")
	if err != nil || len(res.Instances) != 0 {
		t.Fatalf("空发现的 Resolve = (%+v, %v)，want 0 实例且无错", res, err)
	}

	// coordx 出错要透出，不吞。
	if _, err := (&coordxResolver{disc: &fakeDiscover{err: errors.New("boom")}}).Resolve(context.Background(), "svc-order"); err == nil {
		t.Error("Resolve 应当把 coordx 的错误透出")
	}
}

func TestCoordxResolverDiffAndName(t *testing.T) {
	r := &coordxResolver{disc: &fakeDiscover{}, service: "self-svc"}
	if got := r.Name(); got != resolverName {
		t.Errorf("Name = %q, want %q", got, resolverName)
	}
	a := discovery.NewInstance("tcp", "10.0.0.1:9013", discovery.DefaultWeight, nil)
	b := discovery.NewInstance("tcp", "10.0.0.2:9013", discovery.DefaultWeight, nil)

	change, ok := r.Diff("svc-order",
		discovery.Result{Instances: []discovery.Instance{a}},
		discovery.Result{Instances: []discovery.Instance{a, b}})
	if !ok {
		t.Fatal("实例变化时 Diff 应当报告有变化")
	}
	if len(change.Added) != 1 || len(change.Removed) != 0 {
		t.Fatalf("Diff = +%d/-%d, want +1/-0", len(change.Added), len(change.Removed))
	}
	if !strings.Contains(change.Added[0].Address().String(), "10.0.0.2:9013") {
		t.Errorf("新增实例 = %q, want 10.0.0.2:9013", change.Added[0].Address())
	}
}
