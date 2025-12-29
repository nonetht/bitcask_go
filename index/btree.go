package index

import (
	"bitcask-gown/data"
	"bytes"
	"sync"

	"github.com/google/btree"
)

// BTree Go之中实现的BTree并不是线程安全的，因此需要锁
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree 创建一个新的 BTree 结构体
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Put 将对应的 Item 添加到索引之中
func (b *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.tree.ReplaceOrInsert(&Item{key, pos})
	return true
}

// Delete 将 key 所对应的 Item 从索引中删除。如果删除成功，返回 true，反之为 false。
func (b *BTree) Delete(key []byte) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	if it := b.tree.Delete(&Item{key: key}); it != nil {
		return true
	}
	return false
}

// Get 从索引中获取 key 对应的 Item，如果获取成功返回对应的记录和 true，反之为 nil，false。
func (b *BTree) Get(key []byte) (*data.LogRecordPos, bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	it := b.tree.Get(&Item{key: key})
	if it == nil {
		return nil, false
	}
	return it.(*Item).pos, true
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) < 0 // TODO: 类型断言是什么来着？
}
