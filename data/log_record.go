package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordToDelete
)

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

// LogRecordPos 记录存储的文件名称 Fid 以及对应的位置 Offset
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

func EncodeLogRecord(record *LogRecord) ([]byte, int) {
	return nil, 0
}
