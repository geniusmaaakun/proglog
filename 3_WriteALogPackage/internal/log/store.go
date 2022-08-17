package log

//store レコードを保存

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

///レコードサイズとインデックスエントリを永続化するためのエンコーディング
var (
	enc = binary.BigEndian
)

//レコードの長さを格納するために使うバイト数
const (
	lenWidth = 8
)

//ファイルを保持し、ファイルにバイトを追加したり、ファイルからバイトを読み込む
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

//与えられたファイルに対するstoreを作成
func newStore(f *os.File) (*store, error) {
	//ファイルサイズを取得
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

//与えられたバイトをストアに永続化する
//書き込んだバイト数、書き込み開始した位置を返す
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	//bigEndianで書き込む レコード長さを書き込む
	//何バイトと読み出せばいいかわかるようにしている
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	//直接ではなくバッファ付きライターに書き込む
	//小さなレコードを多数書き込む場合は有効
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	//ストアがファイル内でレコードを保持する位置を返す
	//セグメントは、このレコードに関連するインデックスエントリを作成する際に、この位置を使う
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

//指定された位置に格納されているレコードを返す
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//バッファがまだディスクにフラッシュされていないレコードを読み出そうとしている場合に備えて、フラッシュしておく
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	//レコード全体を読み込むためのバッファを確保し、読み込む
	size := make([]byte, lenWidth)
	//posの位置から読み込む。レコードサイズを取得
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	//戻り値で返す場合は、Cと違いヒープに割り当てられる
	return b, nil
}

//ストアのファイルのoffオフセットから、len(p)バイトをpへと読み込む
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

//ファイルをクローズする前にバッファされたデータを永続化する
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
