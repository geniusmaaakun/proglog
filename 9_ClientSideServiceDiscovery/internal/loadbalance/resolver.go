package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/travisjeffery/proglog/api/v1"
)

//Builder, Resolverインターフェースを実装する型
//clientConnコネクションはユーザーのクライアントコネクションでgRPCはこれをリゾルバに渡して、リゾルバが発見したサーバーで更新するようにする
//resolverConnはリゾルバ自身のサーバーへのクライアントコネクションなのでGetServersを呼び出してサーバーを取得する
type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

//BuilderインターフェースはBuildとSchemeの二つから構成される
var _ resolver.Builder = (*Resolver)(nil)

//サーバーを発見できるリゾルバを構築するのに必要なデータ（ターゲットアドレスなど）とリゾルバが発見したサーバーで更新するクライアントコネクションを受け取る
//サーバーへのクライアントコネクションを設定する
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const Name = "proglog"

//リゾルバのスキーム識別子を返す
func (r *Resolver) Scheme() string {
	return Name
}

//リゾルバをgRPCに登録
func init() {
	resolver.Register(&Resolver{})
}

//resolverインターフェースはResolveNowとCloseで構成される
var _ resolver.Resolver = (*Resolver)(nil)

//ターゲットを解決し、サーバーを発見し、サーバーとのクライアントコネクションを更新する
//リゾルバがサーバーを発見する方法は、リゾルバと扱うサービスによって異なる
//gRPCクライアントを作成し、jクラスタのサーバーを取得するためにGetServersを呼び出す
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// クラスタを取得して、cc属性を設定する
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}
	//ロードバランサが選択できるサーバーを知らせるために、スライスで状態を更新
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}
	//サーバーを発見したらコネクションを更新
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

//リゾルバを閉じる
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
