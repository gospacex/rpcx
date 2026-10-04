// coordx → kitex registry.Registry 适配：kitex 服务端的注册/注销
// 一律经 coordx 落地，本包不直接依赖任何注册中心客户端。
package rpcx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/cloudwego/kitex/pkg/registry"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/gospacex/coordx"
	// consul 驱动的 init() 会把 "consul" 装进 coordx 的工厂表；缺少这一行
	// coordx.NewRegister 会直接返回 "register driver not registered"，
	// 而 BuildServerOptions 静默跳过错误，现象是服务「起了但不被发现」。
	_ "github.com/gospacex/coordx/consul"
)

// coordxDriver coordx 驱动名。新增注册中心请在 coordx 侧实现驱动，
// 再在本包 blank import 并把名字暴露出去，不要引入第二个注册中心库。
const coordxDriver = "consul"

// coordxOpTimeout 单次注册/注销/发现操作的上限。
// coordx 驱动的 TTL 心跳由内部 ticker 维持、不继承这里的 ctx，
// 因此超时取消不会影响已注册实例的续期。
const coordxOpTimeout = 5 * time.Second

// coordxRegistry 把 coordx.Register 适配成 kitex 的 registry.Registry。
type coordxRegistry struct {
	reg     coordx.Register
	service string
	host    string
	port    int
}

// newCoordxRegistry 创建注册句柄并按 service/addr 预填实例坐标。
//
// addr 用 kitex 官方 utils.NewNetAddr 归一（不自行实现 net.Addr），
// host/port 在此一次性拆好：注册与注销必须写同一个 instance ID，
// 否则注销会作用在另一个实例上。
func newCoordxRegistry(registryAddr string, trace bool, service, addr string) (registry.Registry, error) {
	reg, err := coordx.NewRegister(&coordx.Config{
		Driver:      coordxDriver,
		Addr:        registryAddr,
		EnableTrace: trace,
	})
	if err != nil {
		return nil, err
	}
	netAddr := utils.NewNetAddr("tcp", addr)
	host, port, err := splitAddrPort(netAddr)
	if err != nil {
		return nil, err
	}
	return &coordxRegistry{reg: reg, service: service, host: host, port: port}, nil
}

// Register 注册实例的接口实现。
func (c *coordxRegistry) Register(info *registry.Info) error {
	inst, service := c.instance(info)
	ctx, cancel := context.WithTimeout(context.Background(), coordxOpTimeout)
	defer cancel()
	return c.reg.Register(ctx, service, inst)
}

// Deregister 注销实例的接口实现。
func (c *coordxRegistry) Deregister(info *registry.Info) error {
	inst, service := c.instance(info)
	ctx, cancel := context.WithTimeout(context.Background(), coordxOpTimeout)
	defer cancel()
	return c.reg.Deregister(ctx, service, inst)
}

// instance 产出本次注册/注销用的实例坐标。
//
// kitex 传入的 info 优先于构造时的入参：kitex 会用**端口分配后**的真实监听
// 地址填 Info.Addr（配置里的 ":0" 在此已被替换），比装配时拿到的配置值准。
func (c *coordxRegistry) instance(info *registry.Info) (*coordx.ServiceInstance, string) {
	service, host, port := c.service, c.host, c.port
	if info != nil {
		if info.ServiceName != "" {
			service = info.ServiceName
		}
		if info.Addr != nil {
			if h, p, err := splitAddrPort(info.Addr); err == nil {
				host, port = h, p
			}
		}
	}
	id := instanceID(service, host, port)
	return &coordx.ServiceInstance{
		ID:        id,
		ServiceID: id,
		Addr:      host,
		Port:      port,
		Healthy:   true,
	}, service
}

// instanceID 实例唯一标识。注册与注销必须用同一串，否则心跳停不下来、
// 服务残留在注册中心。
func instanceID(service, host string, port int) string {
	return fmt.Sprintf("%s-%s-%d", service, host, port)
}

// splitAddrPort 从 net.Addr 拆出 host 与端口号。
func splitAddrPort(addr net.Addr) (string, int, error) {
	if addr == nil {
		return "", 0, errors.New("rpcx: nil listen addr")
	}
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", 0, fmt.Errorf("rpcx: bad addr %q: %w", addr.String(), err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("rpcx: bad port %q in addr %q: %w", portStr, addr.String(), err)
	}
	return host, port, nil
}
