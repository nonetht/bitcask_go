package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordToDelete
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

func EncodeLogRecord(record *LogRecord) ([]byte, int) {
	return nil, 0
}

// 对字节数组之中的 Header 信息进行解码，将其由 []byte 转化为 logRecordHeader
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int) {
	if len(buf) < 5 {
		return nil, 0
	}

	crc, typ := binary.LittleEndian.Uint32(buf[0:4]), buf[4]
	index := 5

	// 解码 KeySize，从 index 开始读，读出数值以及长度
	keySize, n := binary.Varint(buf[index:])
	index += n

	// 解码 ValueSize 从 index 开始读，读出数值以及长度
	valueSize, n := binary.Varint(buf[index:])
	index += n

	return &logRecordHeader{
		CRC:       crc,
		Type:      typ,
		KeySize:   uint32(keySize),
		ValueSize: uint32(valueSize),
	}, index
}
