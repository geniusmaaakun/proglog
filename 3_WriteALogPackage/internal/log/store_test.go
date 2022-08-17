package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	//書き込む内容
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	//tmpファイル作成
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	//store作成
	s, err := newStore(f)
	require.NoError(t, err)

	//追加
	testAppend(t, s)
	//読み込み
	testRead(t, s)
	testReadAt(t, s)

	//再起動時に読み込めるかのテスト
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	//3回書き込む
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		//書き込み開始した位置から書き込んだバイト数が8*iと一致するか
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		//読み込んだバイト文字を返す
		read, err := s.Read(pos)
		require.NoError(t, err)
		//書き込み文字と一致するか
		require.Equal(t, write, read)
		//次の位置へ
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		//オフセットから8バイト読み込む
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		//8バイト読み込んだか
		require.Equal(t, lenWidth, n)
		off += int64(n)

		//レコードサイズを取得できる
		size := enc.Uint64(b)
		//サイズ分の領域を確保
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)
	//書き込んだ後に閉じてからFrushされるか
	testAppend(t, s)
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
