package data

import "bitcask-gown/fio"

// DataFile 负责文件部分内容，
type DataFile struct {
	FileID    uint32        // 文件 ID 号
	WriteOff  int64         // 告知对于当前文件，已经写入到了哪里
	IoManager fio.IOManager // 命名基于它是用来读写字节的
}

func (d DataFile) appendLogRecord(record *LogRecord) {

}
