package log

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "github.com/travisjeffery/proglog/api/v1"
)

//発見されたサービスにリクエストし、ログをレプリケーションする

//レプリケーターはgRPCクライアントを使って、他のサーバーに接続する
type Replicator struct {
	DialOptions []grpc.DialOption //gRPCクライアントを設定するためのOP
	LocalServer api.LogClient     //produceを呼び出し、他サーバーから読み出したメッセージをコピーする

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{} //サーバーアドレスからチャネルへのマップ。サーバーが故障したり、離脱したときにレプリケーターがサーバーからのレプリケーションを停止するのに使う
	closed  bool
	close   chan struct{}
}

//指定されたサーバーをレプリケーション対象のサーバーのリストに追加し、実際のレプリケーションロジックを実行するゴルーチンを起動する
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// すにでレプリケーションを行っているのでスキップ
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])

	return nil
}

//見つかったサーバーからログのストリームを読みだし、ローカルサーバに書き込んでコピーを保存する
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

//サーバーがクラスタから離脱するときにレプリケートするサーバーのリストから離脱するサーバーを削除し、そのサーバーに関連付けられたチャネルを閉じる
//チャネルを閉じることでreplicateを実行しているゴルーチンに、そのサーバーからレプリケーションを停止するように通知する
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	//サーバーのマップを遅延初期化する
	//構造体は遅延初期化を使って有用な0値を与えるべき
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

//コンストラクタではなぜダメなのか？
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

//レプリケーターを閉じて、クラスタに参加する新たなサーバーをレプリケーションしないようにreplicateを実行しているゴルーチンを終了させて既存のサーバーのレプリケーションを停止する
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

//エラー処理
//他にエラーの使い道がないので、コードを短く簡単にする
//エラーを記録するのみ
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
