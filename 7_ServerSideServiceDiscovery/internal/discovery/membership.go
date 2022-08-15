package discovery

import (
	"net"

	"go.uber.org/zap"

	"github.com/hashicorp/serf/serf"
)

//serfによるディスカバリサービス

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	//ノードがクラスタに参加、離脱したときにserfのイベントを受信する手段
	events chan serf.Event
	logger *zap.Logger
}

//設定とハンドラを持つMenbershipを作成
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

//serfの設定
type Config struct {
	//識別子。設定しない場合はホスト名を使う
	NodeName string
	//アドレスとポート
	BindAddr string
	//他のノードと共有し、このノードの処理方法をクラスタに知らせるためのタグ
	//互いにRPCを呼び出すことができたりする
	Tags map[string]string
	//既存のクラスタがあり、そのクラスタに追加したい新たなノードを作成した場合、新たなノードをクラスタ内の少なくとも一つのノードに向ける必要がある。
	//新たなノードが既存のクラスタないの一つに接続すると、残りのノードについて知る。その逆も同様
	///新たなノードが既存のクラスタに参加するように設定するフィールド
	//本番環境では少なくとも３つのアドレスを指定する
	StartJoinAddrs []string
}

func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

//サーバーがクラスタに参加、離脱したことを知る必要があるサービスないのコンポーネントを表す
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

//ループ処理でserfからイベントチャネルに送られてくるイベントを読み込んで、受信した各イベントをイベントの種別に応じて処理する
//ノードがクラスタに参加したり離脱したりすると、serfはイベントをクラスタに参加あるいは離脱したノード自身を含む全てのノードに送信する
//イベントが表すノードがローカルサーバーであるかを検査し、サーバーが自分自身に作用しないようにする
//自分自身を複製しないようにする
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

//指定されたserfメンバーがローカルメンバーかを、メンバーの名前を確認して返す
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

//クラスタのserfメンバーのその時点のスナップショットを返す
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

//メンバーがserfクラスタから離脱することを指示
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

//与えられたエラーとメッセージをロギングする
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
