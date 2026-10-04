package rpcx

import (
	"testing"
	"time"
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

func TestBuildClientOptionsTraceOrder(t *testing.T) {
	// suite 与 ClientSpanMiddleware 必须同时出现（顺序 = 注册顺序）：
	// 少了 middleware 就会出现 2026-10-02 实测的「span 树断成两棵子树」。
	traced := BuildClientOptions(ClientOptions{ServiceName: "bff", TraceEnabled: true})
	plain := BuildClientOptions(ClientOptions{ServiceName: "bff"})
	if got := len(traced) - len(plain); got != 2 {
		t.Fatalf("tracing adds %d options, want 2 (suite + ClientSpanMiddleware)", got)
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
