package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/travisjeffery/proglog/api/v1"
)

//セグメントを全てまとめている抽象的概念。セグメントの集まりを管理

//ログはセグメントの集まりと、書き込みを追加するアクティブセグメントを保存
type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

//logインスタンスを生成
//設定がない場合はデフォルト値を設定
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

//ログの開始処理としてディスク上のセグメントの一覧を取得し、ファイル名からベースオフセットの値を求めてソートする
//セグメントのスライスを古い順に並べるため
//ディスク上に存在するセグメントを処理して設定する
//既存のセグメントがない限りnewSegmantを使って最初のセグメントを作成する
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsetはindexとstoreの重複を含むので、スキップする。
		// baseOffset contains dup for index and store so we skip
		// the dup
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

//ログにレコード追加する。
//アクティブセグメントが最大サイズ以上の場合、新たなアクティブセグメントを作成する
//その後レコードは、アクティブセグメントに追加される
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	return off, err
}

//指定されたオフセットに保存されているレコードを読み出す
//指定されたレコードを含むセグメントを検索する
//セグメントは古い順に並んでおり、セグメントのベースオフセットはセグメント内の最小オフセットなので。ベースオフセットが探しているオフセット以下であり、かつnextOffsetが探しているオフセットより大きい、最初のセグメントを探す
//レコードを含むセグメントを見つけたら、そのセグメントのインデックスからインデックスエントリを取得し、ストアファイルからデータを読み込み、呼び出し元に返す
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

//セグメントを全て閉じる
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

//ログをクローズして、データを削除する
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

//ログを削除して、置き換える新たなログを作成する
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

//ログに保存されているオフセット範囲を返す
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.highestOffset()
}

func (l *Log) highestOffset() (uint64, error) {
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

//最大オフセットがlowestよりも小さいセグメントを全て削除
//ディスク容量は無限ではないので、定期的にTruncateを呼び出す。
//それまでに処理したデータで不要になった古いセグメントを削除
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

//ログ全体を返すReaderを返す
//スナップショットをサポートし、ログの復旧をサポートする必要がある場合必要な機能
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		//読み出しの際、独自の設定を使うため独自のReaderにする
		readers[i] = &originReader{segment.store, 0}
	}
	//
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

//ログを追加する。新たなセグメントを作成
//ログのセグメントのスライスに追加する。新たなセグメントをアクティブなセグメントとして設定する
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
