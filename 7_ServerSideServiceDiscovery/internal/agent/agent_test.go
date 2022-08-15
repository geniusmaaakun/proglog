package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/travisjeffery/proglog/api/v1"
	"github.com/travisjeffery/proglog/internal/agent"
	"github.com/travisjeffery/proglog/internal/config"
)

//ディスカバリとサービス間のテスト

//セキュリティをテストするためのテストで使われる証明書の設定を定義
func TestAgent(t *testing.T) {
	//クライアントに提供される証明書の設定を定義
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	//サーバー間で提供される証明書の設定を定義し、サーバーが相互に接続してレプリケーションできるようにする
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	//クラスタを設定。三つのノードのクラスタを設定する
	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		//必要な二つのポートを割り当てる
		//gRPCログコレクション用と、serfサービスディスカバリ用
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	//エージェントが正常にシャットダウンしたことを確認し、テストデータを削除するために、テストの後に実行される関数を呼び出す
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agent.Config.DataDir),
			)
		}
	}()

	//ノードが互いを発見する時間を確保する多mに、スリープさせる
	time.Sleep(3 * time.Second)

	//動作確認
	//一つのノードに足して書き込み、そのノードから読み出せることを検証
	//次に別ノードがレコードを複製できたかを検査する
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// レプリケーションが完了するまで待つ
	//レプリケーションはサバー間で非同期で動作するため、サーバーに書き込まれたログがすぐには利用できない
	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	/*
		consumeResponse, err = leaderClient.Consume(
			context.Background(),
			&api.ConsumeRequest{
				Offset: produceResponse.Offset + 1,
			},
		)
		require.Nil(t, consumeResponse)
		require.Error(t, err)
		got := status.Code(err)
		want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
		require.Equal(t, got, want)
	*/
}

//サービスのクライアントを設定する
func client(
	t *testing.T,
	agent *agent.Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)

	client := api.NewLogClient(conn)
	return client
}
