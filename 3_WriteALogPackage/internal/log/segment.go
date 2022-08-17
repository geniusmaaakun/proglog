package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/travisjeffery/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

//ストアとインデックスをまとめている抽象的概念
//ログがアクティブなセグメントにレコードを追加する場合、セグメントはデータをストアに書き込み、インデックスに新たなエントリを追加する
//読み取りの場合、セグメントはインデックスからエントリを検索し、ストアからデータを取り出す必要がある

//セグメントはストアとインデックスを呼び出す必要があるためフィールドとして保持
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64 //相対的なオフセット計算のオフセット、新たなレコードを追加する際のオフセット
	config                 Config //セグメントが最大化を確認するために必要
}

//ログは現在のアクティブセグメントが最大サイズに達した時など、新たなセグメントを追加する必要がある時に呼び出す
//まずストアファイルインデックスファイルを開く。ファイルが存在しない場合は作成
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

//セグメントにレコードを書き込み、新たに追加されたレコードのオフセットを返す
//ログはAPIレスポンスでそのオフセットを返す
//セグメントはデータをストアに追加し、その後インデックスエントリを追加するという２ステップの処理でレコードを追加する
//失敗した場合はゴミとして残る設計になっている
//インデックスのオフセットはbaseに対する相対的なもので、相対オフセットを計算する
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

//指定されたオフセットのレコードを返す
//書き込みと同様にレコードを読み込むために、セグメントは最初に絶対オフセットを相対オフセットに変換し、関連するインデックスエントリの内容を取得する
//インデックスエントリから位置を取得すると、セグメントはストア内のレコード位置から適切な量のデータを読み出せる
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

//セグメントが最大サイズに達したかどうか
//ストアまたは、インデックスへの書き込みがいっぱいになったかどうかで判定
//このメソッドを使って新たなセグメントを作成する必要があるのかを判定する
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

//σグメントを閉じてインデックスファイルとストアファイルを削除
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
