package bitcask_gown

import (
	"bitcask-gown/index"
	"bytes"
)

type IteratorOption struct {
	Prefix  []byte // 制定部分字节数组为前缀内容
	Reverse bool
}

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	opt       *IteratorOption
}

func (i Iterator) Rewind() {
	i.indexIter.Rewind()
}

func (i Iterator) Seek(key []byte) {
	i.indexIter.Seek(key)
}

func (i Iterator) Next() {
	i.indexIter.Next()
}

func (i Iterator) Valid() bool {
	return i.indexIter.Valid()
}

func (i Iterator) Key() []byte {
	return i.indexIter.Key()
}

// Value 这次我们读取的为真实的 Value
func (i Iterator) Value() ([]byte, error) {
	pos := i.indexIter.Value()
	i.db.lock.Lock()
	defer i.db.lock.Unlock()
	return i.db.getValueByPos(pos)
}

func (i Iterator) Close() {
	i.indexIter.Close()
}

// skipToNext 如果配置了 Prefix，需要跳过不符合前缀的 Key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.opt.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		// 如果 key 以前缀开头，说明符合条件，直接返回
		if prefixLen <= len(key) && bytes.Compare(it.opt.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
