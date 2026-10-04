package rpcx

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudwego/kitex/pkg/registry"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/gospacex/coordx"
)

// fakeRegister 顶替 coordx.Register，让注册/注销的适配不依赖 consul。
type fakeRegister struct {
	registered   []*coordx.ServiceInstance
	deregistered []*coordx.ServiceInstance
	services     []string
	err          error
}

func (f *fakeRegister) Register(_ context.Context, service string, inst *coordx.ServiceInstance) error {
	f.registered = append(f.registered, inst)
	f.services = append(f.services, service)
	return f.err
}

func (f *fakeRegister) Deregister(_ context.Context, service string, inst *coordx.ServiceInstance) error {
	f.deregistered = append(f.deregistered, inst)
	f.services = append(f.services, service)
	return f.err
}

func (f *fakeRegister) HealthCheck() error { return nil }
func (f *fakeRegister) Close() error       { return nil }

func TestInstanceID(t *testing.T) {
	cases := []struct {
		service, host string
		port          int
		want          string
	}{
		{"srv-order", "10.0.0.1", 9013, "srv-order-10.0.0.1-9013"},
		// 全零值也必须产出一串：注册与注销必须落在同一个实例上，
		// 空串 ID 会让注销作用在「默认实例」。
		{"", "", 0, "--0"},
		{"srv-order", "", 9013, "srv-order--9013"},
	}
	for _, c := range cases {
		if got := instanceID(c.service, c.host, c.port); got != c.want {
			t.Errorf("instanceID(%q,%q,%d) = %q, want %q", c.service, c.host, c.port, got, c.want)
		}
	}
}

func TestSplitAddrPort(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		wantH   string
		wantP   int
		wantErr bool
	}{
		{"host:port", "127.0.0.1:9013", "127.0.0.1", 9013, false},
		{"ipv6", "[::1]:9013", "::1", 9013, false},
		{"端口 0", "127.0.0.1:0", "127.0.0.1", 0, false},
		{"缺端口", "127.0.0.1", "", 0, true},
		{"端口非数字", "127.0.0.1:abc", "", 0, true},
		{"空地址", "", "", 0, true},
	}
	for _, c := range cases {
		h, p, err := splitAddrPort(utils.NewNetAddr("tcp", c.addr))
		if (err != nil) != c.wantErr {
			t.Errorf("%s: splitAddrPort(%q) err = %v, wantErr %v", c.name, c.addr, err, c.wantErr)
			continue
		}
		if h != c.wantH || p != c.wantP {
			t.Errorf("%s: splitAddrPort(%q) = (%q,%d), want (%q,%d)", c.name, c.addr, h, p, c.wantH, c.wantP)
		}
	}
	if _, _, err := splitAddrPort(nil); err == nil {
		t.Error("splitAddrPort(nil) 应当报错，nil 地址不该被当成 :0 注册进去")
	}
}

func TestCoordxRegistryInstance(t *testing.T) {
	c := &coordxRegistry{service: "cfg-svc", host: "10.0.0.1", port: 9013}

	// info 为 nil：全用构造参数。
	inst, svc := c.instance(nil)
	if svc != "cfg-svc" || inst.ID != "cfg-svc-10.0.0.1-9013" {
		t.Fatalf("instance(nil) = (%s, %+v)", svc, inst)
	}

	// kitex 传来的 info 优先：服务名与「端口分配后」的真实地址都覆盖。
	inst, svc = c.instance(&registry.Info{
		ServiceName: "srv-order",
		Addr:        utils.NewNetAddr("tcp", "10.0.0.2:9020"),
	})
	if svc != "srv-order" {
		t.Errorf("service = %q, want srv-order", svc)
	}
	if inst.ID != "srv-order-10.0.0.2-9020" || inst.Addr != "10.0.0.2" || inst.Port != 9020 {
		t.Errorf("instance(real info) = %+v, want 10.0.0.2:9020", inst)
	}
	if !inst.Healthy {
		t.Error("注册出去的实例必须 Healthy=true，否则发现侧会直接过滤掉")
	}

	// info.Addr 非法时回落到构造时的地址，不能注册出 ":0"。
	inst, _ = c.instance(&registry.Info{ServiceName: "srv-order", Addr: utils.NewNetAddr("tcp", "bogus")})
	if inst.ID != "srv-order-10.0.0.1-9013" {
		t.Errorf("Addr 非法时 instance = %+v, want 回落 10.0.0.1:9013", inst)
	}

	// 只有服务名没有地址：服务名覆盖，地址回落。
	if inst, svc = c.instance(&registry.Info{ServiceName: "srv-only"}); svc != "srv-only" || inst.Port != 9013 {
		t.Errorf("instance(name only) = (%s, %+v)", svc, inst)
	}
}

func TestCoordxRegistryRegisterDeregister(t *testing.T) {
	fake := &fakeRegister{}
	c := &coordxRegistry{reg: fake, service: "cfg-svc", host: "10.0.0.1", port: 9013}
	info := &registry.Info{ServiceName: "srv-order", Addr: utils.NewNetAddr("tcp", "10.0.0.2:9020")}

	if err := c.Register(info); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Deregister(info); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if len(fake.registered) != 1 || len(fake.deregistered) != 1 {
		t.Fatalf("注册 %d 次注销 %d 次，want 各 1", len(fake.registered), len(fake.deregistered))
	}
	// 注册与注销必须写同一个 ID，否则注销作用在另一个实例上、心跳停不下来。
	if fake.registered[0].ID != fake.deregistered[0].ID {
		t.Fatalf("注册 ID %q != 注销 ID %q", fake.registered[0].ID, fake.deregistered[0].ID)
	}
	if want := "srv-order-10.0.0.2-9020"; fake.registered[0].ID != want || fake.registered[0].ServiceID != want {
		t.Fatalf("实例 ID/ServiceID = %q/%q, want %q",
			fake.registered[0].ID, fake.registered[0].ServiceID, want)
	}
	for _, s := range fake.services {
		if s != "srv-order" {
			t.Fatalf("传给 coordx 的服务名 = %q, want srv-order", s)
		}
	}

	// coordx 出错要透出，不吞。
	boom := &fakeRegister{err: errors.New("boom")}
	if err := (&coordxRegistry{reg: boom, service: "s", host: "h", port: 1}).Register(nil); err == nil {
		t.Error("Register 应当把 coordx 的错误透出")
	}
	if err := (&coordxRegistry{reg: boom, service: "s", host: "h", port: 1}).Deregister(nil); err == nil {
		t.Error("Deregister 应当把 coordx 的错误透出")
	}
}
