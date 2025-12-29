package bitcask_gown

import (
	"bitcask-gown/data"
	"bitcask-gown/index"
	"sync"
)

// DB 定义数据库，以及相应字段
type DB struct {
	option     Options
	lock       *sync.RWMutex             // 支持并发，需要锁
	activeFile *data.DataFile            // 当前正在执行写入的活跃文件
	oldFiles   map[uint32]*data.DataFile // 已经“写满”的旧数据文件
	index      index.Indexer             // 索引部分，存储数据位置信息的地方
}

// Put 向 db 之中添加一条新的 logRecord 信息，将 logRecord 添加到活跃文件之后，还要将其添加到索引之中。
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	logRecord := data.NewLogRecord(key, value)
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 根据 key 来获取对应的 value 值的信息
func (db *DB) Get(key []byte) ([]byte, error) {
	// 仍然是老规矩加锁
	db.lock.RLock()
	defer db.lock.RUnlock()

	pos, ok := db.index.Get(key)
	if !ok {
		return nil, ErrIndexNotFound
	}

	var dataFile *data.DataFile

	if db.activeFile.FileID == pos.Fid {
		dataFile = db.activeFile
	}
	if db.oldFiles[pos.Fid] != nil {
		dataFile = db.oldFiles[pos.Fid]
	}

	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

// 理解为 Put 方法的辅助函数，对于这种私有辅助方法，可以不加锁
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	// 说明是第一次创建的 db 数据库实例，其 fileID 为0.
	if db.activeFile == nil {
		if err := db.createActiveFile(); err != nil {
			return nil, err
		}
	}

	encRecord, size := data.EncodeLogRecord(record) // 后续会实现将 logRecord 解码

	// 判断是否超过文件大小，如果超过则创建新的 activeFile；注意这里要执行类型转换
	if db.activeFile.WriteOff+int64(size) > db.option.DataFileSize {
		// 1.持久化活跃文件
		if err := db.activeFile.IOManager.Sync(); err != nil {
			return nil, err
		}
		// 2.将“写满”的活跃文件，转换为旧文件
		db.oldFiles[db.activeFile.FileID] = db.activeFile
		// 3.创建一个新的活跃文件（ID 递增）
		if err := db.createActiveFile(); err != nil {
			return nil, err
		}
	}

	offset := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	if db.option.SyncWrites {
		err := db.activeFile.Sync()
		if err != nil {
			return nil, err
		}
	}
	// 新建 logRecordPos 信息，随后返回
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: offset,
	}
	return pos, nil
}

// 对应两种case：1. 无活跃文件，创建 fileId = 0的活跃文件。2. 有活跃文件，则创建原活跃文件 fileId + 1的活跃文件
func (db *DB) createActiveFile() error {
	var newActiveFileID uint32 = 0
	if db.activeFile != nil {
		newActiveFileID = db.activeFile.FileID + 1
	}
	newActiveFile, err := data.OpenDataFile(db.option.DirPath, newActiveFileID)
	if err != nil {
		return err
	}
	db.activeFile = newActiveFile
	return nil
}
