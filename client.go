// coordx → kitex discovery.Resolver 适配：kitex 客户端的服务发现
// 一律经 coordx 落地，本包不直接依赖任何注册中心客户端。
package rpcx

import (
	"context"
	"net"
	"strconv"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/gospacex/coordx"
	// consul 驱动的 init() 会把 "consul" 装进 coordx 的工厂表；缺少这一行
	// coordx.NewDiscover 会返回 "discover driver not registered"，
	// 而 BuildClientOptions 静默跳过错误，现象是调用时才报「无可用实例」。
	_ "github.com/gospacex/coordx/consul"
)

// resolverName 适配器的名字，出现在 kitex 的诊断信息里。
const resolverName = "coordx"

// coordxResolver 把 coordx.Discover 适配成 kitex 的 discovery.Resolver。
type coordxResolver struct {
	disc    coordx.Discover
	service string
}

// newCoordxResolver 创建发现句柄。
//
// service 作兜底：Target 拿不到服务名时用它（见 Target 方法）。
func newCoordxResolver(registryAddr string, trace bool, service string) (discovery.Resolver, error) {
	d, err := coordx.NewDiscover(&coordx.Config{
		Driver:      coordxDriver,
		Addr:        registryAddr,
		EnableTrace: trace,
	})
	if err != nil {
		return nil, err
	}
	return &coordxResolver{disc: d, service: service}, nil
}

// Target 返回用于缓存键的目标描述。
//
// kitex 传来的 target.ServiceName 是真正的被调方（同一次 client 只调一个
// 下游，但它比构造时传入的自身服务名准），拿不到才回落到构造参数。
func (r *coordxResolver) Target(_ context.Context, target rpcinfo.EndpointInfo) string {
	if target != nil && target.ServiceName() != "" {
		return target.ServiceName()
	}
	return r.service
}

// Resolve 拉取一次实例列表。
func (r *coordxResolver) Resolve(ctx context.Context, desc string) (discovery.Result, error) {
	service := desc
	if service == "" {
		service = r.service
	}
	ctx, cancel := context.WithTimeout(ctx, coordxOpTimeout)
	defer cancel()

	insts, err := r.disc.Discovery(ctx, service)
	if err != nil {
		return discovery.Result{}, err
	}
	instances := make([]discovery.Instance, 0, len(insts))
	for _, ins := range insts {
		// 跳过坐标不全的实例：它们拼出来是 ":0" 这类无效地址，进了负载均衡
		// 只会让调用方在「看起来发现了实例」的情况下拿到建连失败。
		if ins == nil || ins.Addr == "" || ins.Port == 0 {
			continue
		}
		instances = append(instances, discovery.NewInstance(
			"tcp", net.JoinHostPort(ins.Addr, strconv.Itoa(ins.Port)),
			discovery.DefaultWeight, ins.Metadata,
		))
	}
	// Cacheable 置 false：实例列表随注册中心变动，不进 kitex 的结果缓存，
	// 每次都重新走一次 coordx 发现（代价是一次健康查询，换来热度数据的新鲜度）。
	// 已知取舍：本包未实测过 resolve 的调用频次（是否真按每请求触发取决于
	// kitex 的版本与调用形态），因此这里的「一次健康查询」只是量级估计，
	// 不是实测值；若后续实测发现频次要命，再考虑置 true 并给出缓存过期策略。
	return discovery.Result{Cacheable: false, CacheKey: service, Instances: instances}, nil
}

// Diff 复用 kitex 的缺省实现：按地址比集合差异。
func (r *coordxResolver) Diff(cacheKey string, prev, next discovery.Result) (discovery.Change, bool) {
	return discovery.DefaultDiff(cacheKey, prev, next)
}

// Name 返回解析器名称。
func (r *coordxResolver) Name() string { return resolverName }
