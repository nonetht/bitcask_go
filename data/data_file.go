package data

import (
	"bitcask-gown/fio"
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

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
	fileSize, err := fio.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var heaSize int64 = maxLogRecordHeaderSize
	// 处理其中的 corner case，就是我们的 maxHeaderSize + offset < fileSize。如果条件为真，那么将 heaSize 定为
	if heaSize+offset > fileSize {
		heaSize = fileSize - offset
	}

	buf, err := fio.readNBytes(heaSize, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(buf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.CRC == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	// 在读取到 header 之后，我们转向获取对应的 keySize，valueSize
	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize)
	var recSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.Type,
	}

	kvBuf, err := fio.readNBytes(keySize+valueSize, offset+headerSize)
	if err != nil {
		return nil, 0, err
	}

	key, value := kvBuf[:keySize], kvBuf[keySize:]
	logRecord.Key = key
	logRecord.Value = value

	crc := getLogRecordCRC(logRecord, buf[4:])
	if crc != header.CRC {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recSize, nil
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
