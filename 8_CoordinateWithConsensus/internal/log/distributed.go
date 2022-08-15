package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"

	"github.com/hashicorp/raft"

	api "github.com/travisjeffery/proglog/api/v1"
)

//Raftを実装

//分散ログの型
type DistributedLog struct {
	config  Config
	log     *Log
	raftLog *logStore
	raft    *raft.Raft
}

//コンストラクタ
func NewDistributedLog(dataDir string, config Config) (
	*DistributedLog,
	error,
) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

//このサーバーのログストアを作成し、ユーザーのレコードを保存する
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

//サーバーの設定を行い、Raftのインスタンスを作成する
//Raftのログストアを作成し、独自のログを使う。
//ログの初期オフセットを1に設定する
//Raftは特定のログインターフェースが必要なので、ログの実装を保持して、APIを提供する
func (l *DistributedLog) setupRaft(dataDir string) error {
	var err error

	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	l.raftLog, err = newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	//Raftリーダーからすべてのデータをストリーミングするのではなく、新たなサーバーはスナップショットから復元し、リーダーから最新の変更点を取得する。リーダーへの負荷も少ない
	//スナップショットを頻繁に行い、スナップショット内のデータとリーダー上のデータの際を最小にする
	retain := 1 //一つのスナップショットを保持する
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	//ストリームレイヤを内包する低レベルのストリーム抽象化を提供するトランスポートを作成する
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID //一意なID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	//Raftインスタンスを作成し、クラスタを起動する
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		l.raftLog,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		l.raftLog,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

//ログにレコードを追加する
//絵レコードをログに追加するようにRaftに指示する
//Raftサーバーの大部分に対して、コマンドを複製し、最終的にraftサーバーの大部分にレコードを追加する
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {

	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

//RaftのAPIを内包していて、リクエストを適用して、レスポンスを返す
//リクエスト種別とリクエストを[]byteへマーシャルして、Raftが複製するレコードのデータとして使う
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	//レコードを複製してリーダのログにレコードを追加する
	future := l.raft.Apply(buf.Bytes(), timeout)
	//raftのレプリケーションに問題が発生した場合、エラーを返す。タイムアウトやシャットダウンなど
	if future.Error() != nil {
		return nil, future.Error()
	}
	//Raftは単一の値しか返さないので型アサーションによってエラーを判別する
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

//サーバーのログのオフセットで指定されたレコードを読み出す
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

//Raftクラスタにサーバーを追加する
//すべてのサーバーを投票者として追加する
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// サーバはすでに参加している
				return nil
			}
			// 既存のサーバを取り除く
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

//サーバーをクラスタから取り除く
//リーダーを取り除くと新たなリーダーを選出する
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	//リーダーではないノードでクラスタを変更するとエラーが返ってくる
	return removeFuture.Error()
}

//クラスタがリーダーを選出するか、タイムアウトするまで待つ
//ほとんどの操作はリーダー以外できないので、テストで便利
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

//Raftインスタンスをシャットダウンし、Raftのログストアおよびローカルのログを閉じる
func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	if err := l.raftLog.Log.Close(); err != nil {
		return err
	}
	return l.log.Close()
}

//有限ステートマシン
//ビジネスロジックの実装を有限ステートマシンに委ねる

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

//ログエントリをコミットした後に呼び出す
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	//複数のリクエストの種類を受け取れるようにしている
	reqType := RequestType(buf[0])
	//リクエスト種別で切り分けて、実行するメソッドを呼び出す
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

//リクエストをアンマーシャルしてから、ローカルのログにレコードを追加する
func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

//定期実行して状態のスナップショットを撮る
//デフォルトでは２分
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	//ログのデータをすべて読み出すReaderを返す
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

//Raftは状態を保存するためにFSMSnapshotに対してPersistメソッドを呼び出す
//場所はRaftに設定したスナップショットストアに依存する。
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *snapshot) Release() {}

//スナップショットからFSVを復元する
//RaftはスナップショットからFSVを復元するためにRestoreを呼び出す
//リーダーの状態と一致するように既存の状態を破棄する
//ログをリセットし、初期オフセットをスナップショットから読み取った最初のレコードのオフセットに設定し、ログのオフセットが一致するようにする。
//スナップショットのレコードを読み込んで新たなログに追加する
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

//Raftのログストアの定義
//Raftはログを複製し、そのログのレコードでステートマシンを呼び出す
//Raftのログストアとして独自のログストアを使っているので、Raftが要求するLogStore interfaceを満たすために、ログを内包する必要がある
var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

//ログのレコードや情報を取得するメソッド
//オフセットをRaftでは　indexと呼ぶ

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

//ログにレコードを追加する
func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}
func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		//変換して追加
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

//古いレコードを削除する
//オフセット間のレコードを削除する
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

//ストリームレイや
//RaftはRaftサーバーと接続するために低レベルのストリーム抽象化を提供するために、トランスポートのストリームレイやを使う。
//ストリームレイやはRaftのStreamLayerインターフェースを満足する必要がある

//インターフェースを満たしているかを確認
var _ raft.StreamLayer = (*StreamLayer)(nil)

//TLSを設定できるようにする
type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

//Raftクラスタ内の他のサーバに出ていくコネクションを作る
//サーバーに接続する際に、コネクション種別を識別するためにRaftRPCバイトを書き込み、ログのgRPCリクエストと同い時ポートでRaftを多重化できる
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// Raft RPCであることを特定する
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

//入ってくるコネクションを受け入れ、コネクション種別を識別するバイトを読み出して、サーバー側のTLS接続を作成する。
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

//リスナーをクローズする
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

//リスナーのアドレスを返す
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
