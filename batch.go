package bitcask_gown

import "bitcask-gown/data"

type WriteBatch struct {
	db            *DB
	pendingWrites map[string]*data.LogRecord
	setup         *WriteBatchSetup
}

// Put 将 key，value 以 logRecord 形式写入到 pendingWrites 之中
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 仍然是加锁防止竞态条件
	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

	logRecord := data.NewLogRecord(key, value)
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Delete 向 pendWrites 之中写入 logRecord（类型为 toDelete 类型）
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

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

func (wb *WriteBatch) Commit() error {
	// 保证线程安全
	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

	// 检验 pendingWrites 是否有效
	if len(wb.pendingWrites) == 0 {
		return ErrPendingWritesInvalid
	}

	// 检验单次写入是否超过了最大界限
	if len(wb.pendingWrites) > int(wb.setup.MaxBatchNum) {
		return ErrExceedMaxBatchNum
	}

	positions := make(map[string]*data.LogRecordPos)

	// 将所有的 logRecord 添加到 dataFile 之中
	for key, rec := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(rec)
		if err != nil {
			return err
		}
		positions[key] = pos // 将 key - pos 存储到 positions 之中，便于后期索引更新
	}

	// 写入到最后，我们需要创建一个新的类型为 logRecordTxnFinshed 的记录，然后写入到 dataFile 之中
	lstRec := &data.LogRecord{
		Key:  []byte{},
		Type: data.LogRecordTxnFinished,
	}
	_, err := wb.db.appendLogRecord(lstRec)
	if err != nil {
		return err
	}

	// 通过 positions 更新索引
	for key, pos := range positions {
		if ok := wb.db.index.Put([]byte(key), pos); !ok {
			return ErrIndexUpdateFailed
		}
	}

	return nil
}
