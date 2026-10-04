package rpcx

import (
	"reflect"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	kitextracing "github.com/kitex-contrib/obs-opentelemetry/tracing"
)

func TestBuildServerOptionsIncludesAddr(t *testing.T) {
	opts := BuildServerOptions(ServerOptions{
		ServiceName:  "srv-order",
		Addr:         "127.0.0.1:9013",
		RegistryAddr: "127.0.0.1:8500",
	})
	if len(opts) == 0 {
		t.Fatal("no server options produced")
	}
}

func TestBuildClientOptionsIncludesTarget(t *testing.T) {
	opts := BuildClientOptions(ClientOptions{
		ServiceName:  "srv-order",
		RegistryAddr: "127.0.0.1:8500",
		RPCTimeout:   3 * time.Second,
	})
	if len(opts) == 0 {
		t.Fatal("no client options produced")
	}
}

// 注册/发现的适配失败会被 BuildXxxOptions 静默吞掉（见函数 godoc），
// 因此这里用「选项条数」把「coordx 真的挂上了」钉住：少了那一条就说明
// coordx.NewRegister / NewDiscover 报错被吞了，服务会起了但不被发现。
//
// 条数来源固定：
//
//	server: ServiceAddr + ExitWaitTime + ReadWriteTimeout = 3，加注册 +1
//	client: RPCTimeout + ConnectTimeout = 2，加发现 +1
func TestBuildOptionsAttachesCoordx(t *testing.T) {
	if got := len(BuildServerOptions(ServerOptions{
		ServiceName: "srv-order", Addr: "127.0.0.1:9013", RegistryAddr: "127.0.0.1:8500",
	})); got != 4 {
		t.Fatalf("server options with registry = %d, want 4 (coordx registry dropped?)", got)
	}
	if got := len(BuildServerOptions(ServerOptions{
		ServiceName: "srv-order", Addr: "127.0.0.1:9013",
	})); got != 3 {
		t.Fatalf("server options without registry = %d, want 3", got)
	}
	if got := len(BuildClientOptions(ClientOptions{
		ServiceName: "srv-order", RegistryAddr: "127.0.0.1:8500",
	})); got != 3 {
		t.Fatalf("client options with resolver = %d, want 3 (coordx resolver dropped?)", got)
	}
	if got := len(BuildClientOptions(ClientOptions{
		ServiceName: "srv-order", Target: "127.0.0.1:9013, 127.0.0.1:9014",
	})); got != 3 {
		t.Fatalf("client options with direct target = %d, want 3", got)
	}
	if got := len(BuildClientOptions(ClientOptions{
		ServiceName: "srv-order",
	})); got != 2 {
		t.Fatalf("client options without discovery = %d, want 2", got)
	}
}

// kitex 的 Option 是闭包，拿 reflect 的 code pointer 就能把「谁在前」钉住：
// 同一处 func literal 每次调用返回的闭包共享同一个 code pointer，
// 因此可以拿一份「已知来源」的 option 去比对 BuildClientOptions 产出的列表。
func optionCodePtr(o client.Option) uintptr { return reflect.ValueOf(o.F).Pointer() }

// suite 必须先注册（内层、后执行），本包的 ClientSpanMiddleware 后注册（外层、
// 先执行）：只有本包的 span 先建好，被注入给下游的才是它。这是 README 里
// 唯一不可换的不变量，也是 detachKitexSpan 能认出 kitex span 的前提。
func TestBuildClientOptionsTraceOrder(t *testing.T) {
	traced := BuildClientOptions(ClientOptions{ServiceName: "bff", TraceEnabled: true})
	plain := BuildClientOptions(ClientOptions{ServiceName: "bff"})
	if got := len(traced) - len(plain); got != 2 {
		t.Fatalf("tracing adds %d options, want 2 (suite + ClientSpanMiddleware)", got)
	}

	suitePtr := optionCodePtr(client.WithSuite(kitextracing.NewClientSuite()))
	mwPtr := optionCodePtr(client.WithMiddleware(ClientSpanMiddleware()))
	if suitePtr == mwPtr {
		t.Fatalf("suite 与 middleware 的 code pointer 相同（%#x），无法区分顺序", suitePtr)
	}

	iSuite, iMW := -1, -1
	for i, opt := range traced {
		switch optionCodePtr(opt) {
		case suitePtr:
			iSuite = i
		case mwPtr:
			iMW = i
		}
	}
	if iSuite < 0 || iMW < 0 {
		t.Fatalf("未在 %d 个选项里定位到 suite(%d)/middleware(%d)", len(traced), iSuite, iMW)
	}
	if iSuite > iMW {
		t.Fatalf("注册顺序反了：suite 在 %d、ClientSpanMiddleware 在 %d，want suite 在前", iSuite, iMW)
	}
}

func TestSplitTarget(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{{"", 0}, {"127.0.0.1:9013", 1}, {"a:1, b:2", 2}, {"a:1,,b:2,", 2}}
	for _, c := range cases {
		if got := len(splitTarget(c.in)); got != c.want {
			t.Fatalf("splitTarget(%q) = %d items, want %d", c.in, got, c.want)
		}
	}
}
