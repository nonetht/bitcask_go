package index

import (
	"bitcask-gown/data"
)

// Indexer 索引接口，只有实现了基本的增、删、查的功能，才可以称之为索引。此外之所以使用接口，是因为便于后续其他数据结构实现的方式。
type Indexer interface {
	// Put 像索引中添加 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Delete 根据 key，从索引之中删除掉对应位置信息
	Delete(key []byte) bool
	// Get 根据 key，从索引中，取出对应位置信息
	Get(key []byte) (*data.LogRecordPos, bool)
}

// Iterator 通用索引迭代器的接口
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于等于的 key，根据这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
