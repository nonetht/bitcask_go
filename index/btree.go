package index

import (
	"bitcask-gown/data"
	"bytes"
	"sort"
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

// Item 我们向 btree 之中就是添加 Item
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) < 0
}

type btreeIterator struct {
	currIndex int
	reverse   bool // 如果为 true，则是倒序遍历；反之则是正序遍历
	values    []*Item
}

func (b *btreeIterator) Rewind() {
	b.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于等于的 key，根据这个 key 开始遍历
func (b *btreeIterator) Seek(key []byte) {
	if b.reverse {
		// Search uses binary search to find and return the smallest index i in [0, n) at which f(i) is true
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) <= 0
		})
	} else {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) >= 0
		})
	}
}

func (b *btreeIterator) Next() {
	b.currIndex++
}

func (b *btreeIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

func (b *btreeIterator) Key() []byte {
	return b.values[b.currIndex].key
}

func (b *btreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIndex].pos
}

func (b *btreeIterator) Close() {
	b.values = nil
}

func (bt *BTree) Iterator(reverse bool) Iterator { // 接口实现的时候，务必保证签名完全一致。
	if bt == nil {
		return nil
	}

	bt.lock.RLock()
	defer bt.lock.RUnlock()

	return newBTreeIterator(bt.tree, reverse)
}

func newBTreeIterator(bt *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, bt.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true // 返回 false 会终止遍历，但是我也没有执行遍历...
	}

	if reverse {
		// 从大到小，倒序就是
		bt.Descend(saveValues) // Descend 函数不断调用函数 saveValues，直到其返回 false
	} else {
		// 从小到大，正序
		bt.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}
