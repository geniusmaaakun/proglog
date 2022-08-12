package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

//インデックスファイルを保存するファイル

//インデックスエントリの構成するバイト数を定義
//レコードのオフセットとストアファイル内の位置
const (
	offWidth uint64 = 4                   //オフセットはuint32なので4バイト
	posWidth uint64 = 8                   //位置は8バイト
	entWidth        = offWidth + posWidth //ファイル内の位置
)

//インデックスファイル
//永続化されたファイルとメモリマップされたファイルから構成
//sizeはインデックスのサイズ。次にインデックスに追加されるエントリをどこに書き込むかを表す
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

//指定されたファイルからIndexを作成
//Indexを作成し、ファイルの現在のサイズを保存
//インデックスエントリを追加する際に、インデックスファイル内のデータ量を管理できる
//ファイルをメモリへマップする前に、ファイルを細田のインデックスサイズまで大きくし、作成したIndexを呼び出し元に返す
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	//一度メモリにマップされたファイルはサイズ変更できないため
	//Truncateは、指定されたファイルのサイズを変更します。ファイルがシンボリックリンクの場合、そのリンク先のサイズを変更する。エラーが発生した場合、型は*PathErrorである。
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

//メモリにマップされたファイルのデータを永続化されたファイルへ同期し、永続化されたファイルの内容を安定したストレージへ同期
//永続化されたファイルをその中にある実際のデータ量まで切り詰めて、ファイルを閉じる
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	//サービスを停止する際はファイルサイズを切り詰める
	//使われていない領域が残り、サービスを正しく再起動できない。グレースフルシャットダウン
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

//オフセットを受け取りストア内の関連したレコードの位置を返す
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

//与えられたオフセットと位置をインデックスに追加する
//エントリに書き込む領域があるかを確認する。
//領域があればオフセットと位置をエンコードして、メモリにマップされたファイルに書き込む。次に書き込む位置を進める
func (i *index) Write(off uint32, pos uint64) error {
	if i.isMaxed() {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}
