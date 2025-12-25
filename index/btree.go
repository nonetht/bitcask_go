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

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) < 0 // TODO: 类型断言是什么来着？
}

// NewBTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (b *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	oldItem := b.tree.ReplaceOrInsert(&Item{key, pos})
	return oldItem == nil
}

func (b *BTree) Delete(key []byte) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	oldItem := b.tree.ReplaceOrInsert(&Item{key, nil})
	return oldItem != nil
}

func (b *BTree) Get(key []byte) (*data.LogRecordPos, bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	it := b.tree.Get(&Item{key: key})
	if it == nil {
		return nil, false
	}
	return it.(*Item).pos, true
}
