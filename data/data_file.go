package data

import (
	"bitcask-gown/fio"
	"fmt"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

// DataFile 负责文件部分内容，
type DataFile struct {
	FileID    uint32        // 文件 ID 号
	WriteOff  int64         // 告知对于当前文件，已经写入到了哪里
	IOManager fio.IOManager // 命名基于它是用来读写字节的
}

// OpenDataFile 打开或创建新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 1. 拼接文件名，例如：/tmp/bitcask/000000001.data
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d.data", fileId)+DataFileNameSuffix)
	return NewDataFile(fileName, fileId)
}

func NewDataFile(fileName string, fileId uint32) (*DataFile, error) {
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileID:    fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

func (fio *DataFile) Sync() error {
	return fio.IOManager.Sync()
}

func (fio *DataFile) Write(buf []byte) error {
	n, err := fio.IOManager.Write(buf)
	if err != nil {
		return err
	}
	// 递增其 WriteOff 的字段
	fio.WriteOff += int64(n)
	return nil
}

func (fio *DataFile) Close() error {
	return fio.IOManager.Close()
}
