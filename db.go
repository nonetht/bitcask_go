package bitcask_gown

import (
	"bitcask-gown/data"
	"bitcask-gown/index"
	"sync"
)

// DB 定义数据库，以及相应字段
type DB struct {
	lock       *sync.RWMutex             // 支持并发，需要锁
	activeFile *data.DataFile            // 当前正在执行写入的活跃文件
	oldFiles   map[string]*data.DataFile // 已经“写满”的旧数据文件
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
	// 添加 logRecord 之前，如果没有 activeFile 的话，怎么办呢？我选择初始化，创建一个活跃文件
	if db.activeFile == nil {
		err := db.createActiveFile()
		if err != nil {
			return err
		}
	}
	db.activeFile.appendLogRecord(logRecord)
}

func (db *DB) createActiveFile() error {

}
