package bitcask_gown

import (
	"bitcask-gown/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTxnSerialNum uint64 = 0

var txnFinKey = "txn-finished"

type WriteBatch struct {
	mu            *sync.Mutex // 务必有这个，否则只是通过 db 之中就会导致整个数据库读写都被阻塞
	db            *DB
	pendingWrites map[string]*data.LogRecord
	setup         *WriteBatchSetup
}

func NewWriteBatch(db *DB, setup *WriteBatchSetup) *WriteBatch {
	return &WriteBatch{
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
		setup:         setup,
	}
}

// Put 将 key，value 以 logRecord 形式写入到 pendingWrites 之中
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 仍然是加锁防止竞态条件
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := data.NewLogRecord(key, value)
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Delete 向 pendWrites 之中写入 logRecord（类型为 toDelete 类型）
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 检验 pendingWrites 是否已存在对应的 key
	existRec, existPending := wb.pendingWrites[string(key)]
	if existPending {
		if existRec.Type == data.LogRecordToDelete {
			return nil
		}
	} else {
		// 去索引之中检查，看索引里有没有，如果也没有，直接返回
		if pos, _ := wb.db.index.Get(key); pos == nil {
			return nil
		}
	}

	// 说明 key 存在于 pendingWrites （类型为Normal） 或者 Index 之中；构建的 rec 其中 value 没有保留必要。
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordToDelete,
	}

	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 将待写入区域的 logRecord 全部写入，并更新索引
func (wb *WriteBatch) Commit() error {
	// 锁住暂存区
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 检验 pendingWrites 是否有效
	if len(wb.pendingWrites) == 0 {
		return ErrPendingWritesInvalid
	}

	// 检验单次写入是否超过了最大界限
	if len(wb.pendingWrites) > int(wb.setup.MaxBatchNum) {
		return ErrExceedMaxBatchNum
	}

	// 锁住 DB，保证事务提交的串行化
	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

	// 获取当前最新的事务序列号
	serialNum := atomic.AddUint64(&wb.db.serialNum, 1)

	// 创建 positions 用户存储 key - pos 的映射
	positions := make(map[string]*data.LogRecordPos)

	// 将所有的 logRecord 添加到 dataFile 之中
	for _, rec := range wb.pendingWrites {
		// appendLogRecord 是 db.go 之中的方法，负责追加写入到 activeFile
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   recKeyWithSerialNum(rec.Key, serialNum),
			Value: rec.Value,
			Type:  rec.Type,
		})

		if err != nil {
			return err
		}
		positions[string(rec.Key)] = pos // 将 key - pos 存储到 positions 之中，便于后期索引更新
	}

	// 写入到最后，我们需要创建一个新的类型为 logRecordTxnFinshed 的记录（用以标志事务结束），然后写入到 dataFile 之中
	lstRec := &data.LogRecord{
		Key:  []byte(txnFinKey), // key 内容不重要，但是最好不要为空
		Type: data.LogRecordTxnFinished,
	}
	_, err := wb.db.appendLogRecord(lstRec)
	if err != nil {
		return err
	}

	// 根据配置选择是否持久化
	if wb.setup.SyncWrites {
		if err := wb.db.Sync(); err != nil {
			return err
		}
	}

	for _, rec := range wb.pendingWrites {
		if rec.Type == data.LogRecordNormal {
			wb.db.index.Delete(rec.Key)
		} else if rec.Type == data.LogRecordToDelete {
			wb.db.index.Put(rec.Key, positions[string(rec.Key)])
		}
	}

	// 最后将其进行清空即可
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// rec 之中，key + serialNum 编码
func recKeyWithSerialNum(key []byte, serialNum uint64) []byte {
	// 创建字节型数组 serialNumBytes
	serialNumBytes := make([]byte, binary.MaxVarintLen64)
	// 随后将 serialNum 放入到刚才创建的字节数组 serialNumBytes 之中
	n := binary.PutUvarint(serialNumBytes[:], serialNum)

	// 创建一个新的数组 encKey，长度为原本 key 长度再加上 serialNumBytes 的长度
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], serialNumBytes[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 logRecord.Key，获取对应的 key 以及 事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	serialNum, n := binary.Uvarint(key) // TODO: 我还不知道 Uvarint 方法是什么意思来
	realKey := key[n:]
	return realKey, serialNum
}
