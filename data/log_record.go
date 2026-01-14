package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordToDelete
	LogRecordTxnFinished
)

// 定义 LogRecord 的头部信息最大值是15. crc(4) + Type(1) + KeySize(5) + ValueSize(5) = 15
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

// LogRecord 我们是以类似日志写入的方式来追加 LogRecord，同时增加 Type 来表示这是一个新增数据或者待删除数据。
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// NewLogRecord 创建一条新的 LogRecord，返回其位置信息（不是实例）。
func NewLogRecord(key, value []byte) *LogRecord {
	return &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
}

// logRecordHeader 定义了 LogRecord 的头部信息
type logRecordHeader struct {
	CRC       uint32        // 校验值
	Type      LogRecordType // 类型
	KeySize   uint32        // 变长类型，Key 的长度大小
	ValueSize uint32        // Value 的长度
}

// LogRecordPos 记录存储的文件名称 Fid 以及对应的位置 Offset
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

// EncodeLogRecord 将 LogRecord 进行编码操作，转换为 []byte 字节数组
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	tempBuf := make([]byte, maxLogRecordHeaderSize)
	tempBuf[4] = record.Type
	keySize, valueSize := len(record.Key), len(record.Value)
	// 应该从索引值 5 之后写入
	index := binary.PutVarint(tempBuf[5:], int64(keySize))
	// 从索引值 5 + index 开始写入
	index += binary.PutVarint(tempBuf[5+index:], int64(valueSize))

	headerSize := 5 + index // 5 是代表其中 CRC + Type 得到的类型
	// 将 crc 也考虑在内；其中之前的实现，使用的 CheckSumIEEE 方法，包含了 headerBody 以及 record
	crc := getLogRecordCRC(record, tempBuf[4:headerSize])
	binary.LittleEndian.PutUint32(tempBuf, crc)

	recSize := headerSize + keySize + valueSize
	buf := make([]byte, recSize)

	// copy -> func copy(dst, src []Type) int
	copy(buf, tempBuf[:headerSize]) // tempBuf 可能没有用完
	copy(buf[headerSize:], record.Key)
	copy(buf[headerSize+keySize:], record.Value)

	return buf, int64(recSize)
}

// 对字节数组之中的 Header 信息进行解码，将其由 []byte 转化为 logRecordHeader
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) < 5 {
		return nil, 0
	}

	crc, typ := binary.LittleEndian.Uint32(buf[0:4]), buf[4]
	header := &logRecordHeader{
		CRC:  crc,
		Type: typ,
	}

	var headerSize uint32 = 5
	// 取出对应的 Key 以及其对应长度 kl
	keySize, kl := binary.Varint(buf[5:])
	header.KeySize = uint32(keySize)
	headerSize += uint32(kl)

	// 取出对应 Value 以及对应长度 vl
	valueSize, vl := binary.Varint(buf[headerSize:])
	header.ValueSize = uint32(valueSize)
	headerSize += uint32(vl)

	return header, int64(headerSize)
}

// 计算出 CRC 校验值，先算 HeaderBody 部分，随后累加计算 Key, Value 的内容
func getLogRecordCRC(rec *LogRecord, headerBody []byte) uint32 {
	crc := crc32.ChecksumIEEE(headerBody)
	crc = crc32.Update(crc, crc32.IEEETable, rec.Key)
	crc = crc32.Update(crc, crc32.IEEETable, rec.Value)
	return crc
}
