package bitcask_gown

import (
	"bitcask-gown/data"
	"bitcask-gown/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 定义数据库，以及相应字段
type DB struct {
	option     Options
	fileIds    []int
	lock       *sync.RWMutex             // 支持并发，需要锁
	activeFile *data.DataFile            // 当前正在执行写入的活跃文件
	oldFiles   map[uint32]*data.DataFile // 已经“写满”的旧数据文件
	index      index.Indexer             // 索引部分，存储数据位置信息的地方
}

func Open(opt Options) (*DB, error) {
	// 校验配置信息
	if ok := checkOptions(opt); ok != nil {
		return nil, ok
	}

	// 打开对应的 DirPath 文件夹，如果不存在的话，则创建一个新的文件夹
	if _, err := os.Stat(opt.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(opt.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db, err := NewDB(opt)
	if err != nil {
		return nil, err
	}

	// 填充 db 结构体之中的 activeFile, oldFiles 字段
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// 填充 db 结构体之中的 Indexer 字段
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}
	return db, nil
}

// NewDB 创建数据库实例
func NewDB(options Options) (*DB, error) {
	return &DB{
		option:     options,
		fileIds:    []int{},
		lock:       new(sync.RWMutex),
		activeFile: nil,
		oldFiles:   make(map[uint32]*data.DataFile),
		index:      index.NewBTree(),
	}, nil
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

	//
	if pos.Fid == db.activeFile.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[pos.Fid]
		if dataFile == nil {
			return nil, ErrDataFileNotFound
		}
	}

	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

// Delete 采用追加写入的方式来删除一条数据，并且更新索引
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	recToDelete := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordToDelete,
	}

	_, err := db.appendLogRecord(recToDelete)
	if err != nil {
		return err
	}

	// 内存索引更新
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
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
		// 2.保存旧活跃文件
		oldActiveFile := db.activeFile
		// 3.创建一个新的活跃文件（ID 递增）
		if err := db.createActiveFile(); err != nil {
			return nil, err
		}
		// 4.将“写满”的活跃文件，转换为旧文件
		db.oldFiles[oldActiveFile.FileID] = oldActiveFile
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

// 从磁盘之中加载数据文件
func (db *DB) loadDataFile() error {
	// 读取配置文件下其中的文件夹路径信息
	dirEntries, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}

	// 遍历路径下所有 .data 后缀文件，将其添加到 dataFileIds 数组之中
	// 其中涉及到了很多我之前没接触过的方法：strings.HasSuffix, strings.Split ...
	var dataFileIds []int
	db.fileIds = dataFileIds

	for _, dirEntry := range dirEntries {
		if strings.HasSuffix(dirEntry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(dirEntry.Name(), ".")
			fileName, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return err
			}
			dataFileIds = append(dataFileIds, fileName)
		}
	}

	// 对 dataFileIds 进行排序
	sort.Ints(dataFileIds)
	for i, fileId := range dataFileIds {
		if i == len(dataFileIds)-1 {
			db.activeFile, err = data.OpenDataFile(db.option.DirPath, uint32(fileId))
			if err != nil {
				return err
			}
		} else {
			oldFile, err := data.OpenDataFile(db.option.DirPath, uint32(fileId))
			if err != nil {
				return err
			}
			db.oldFiles[oldFile.FileID] = oldFile
		}
	}
	return nil
}

func (db *DB) loadIndexFromDataFile() error {
	// 判断是否存在数据文件
	if len(db.fileIds) == 0 {
		return ErrDataFileNotFound
	}

	for _, fileId := range db.fileIds {
		var offset int64 = 0
		dataFile, err := data.OpenDataFile(db.option.DirPath, uint32(fileId))
		if err != nil {
			return err
		}

		record, i, err := dataFile.ReadLogRecord(offset)
		if err != nil {
			return err
		}

		// 创建一条新的 pos
		pos := &data.LogRecordPos{
			Fid:    uint32(fileId),
			Offset: offset,
		}

		// 向索引之中添加该 pos
		if ok := db.index.Put(record.Key, pos); !ok {
			return ErrIndexUpdateFailed
		}

		offset += i // 递增 offset 部分内容
	}

	return nil
}
