package data

import (
	"bitcask-gown/fio"
	"bytes"
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
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
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

// ReadLogRecord 从 fio 这个 DataFile 之中读取 LogRecord 以及 Size 信息
func (fio *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 我考虑是先读取 logRecordHeader，随后根据对应的 Key，Value 的长度，来进一步读取实际的 Key，Value
	headerBuf, err := fio.readNBytes(15, offset)
	if err != nil {
		return nil, 0, err
	}

	KeySize, ValueSize := int(header[5:8]), int(header[8:12])
	Key, Value := make([]byte, KeySize), make([]byte, ValueSize)

	key, err := fio.readNBytes(KeySize, offset)
	if err != nil {
		return nil, 0, err
	}

	value, err := fio.readNBytes(ValueSize, offset)
	if err != nil {
		return nil, 0, err
	}

	return &LogRecord{
		Key: key,
		Value: value,
		Type: //
	}, KeySize + ValueSize, nil
}

// 读取 df 上的前 N 个字节，将其存储在 buf 变量上
func (df *DataFile) readNBytes(n int64, offset int64) (buf []byte, err error) {
	b := make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return
}
