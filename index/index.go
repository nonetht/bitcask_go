package index

import "bitcask-gown/data"

// Indexer 索引接口，只有实现了基本的增、删、查的功能，才可以称之为索引。此外之所以使用接口，是因为便于后续其他数据结构实现的方式。
type Indexer interface {
	// Put 像索引中添加 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Delete 根据 key，从索引之中删除掉对应位置信息
	Delete(key []byte) bool
	// Get 根据 key，从索引中，取出对应位置信息
	Get(key []byte) (*data.LogRecordPos, bool)
}
