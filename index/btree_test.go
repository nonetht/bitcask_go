package index

import (
	"bitcask-gown/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBTree_Put 其中参数类型 *testing.T 是什么意思呢?
func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	bt.Put(nil, &data.LogRecordPos{1, 100})

	// 测试获取key=nil对应值的情况
	pos1, ok := bt.Get(nil) // pos1 类型是 *data.LogRecordPos
	assert.True(t, ok)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	// 测试获取key="a"对应值的情况
	bt.Put([]byte("a"), &data.LogRecordPos{2, 2})

	pos2, ok := bt.Get([]byte("a")) // []byte类型总感觉怪...
	assert.True(t, ok)
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)

	// 连续两次Put函数添加，会改变key对应的value，测试value是否如期改变
	bt.Put([]byte("a"), &data.LogRecordPos{1, 3})
	pos3, ok := bt.Get([]byte("a"))
	assert.True(t, ok)
	assert.Equal(t, uint32(1), pos3.Fid)
	assert.Equal(t, int64(3), pos3.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	bt.Put(nil, &data.LogRecordPos{1, 100})
	res1 := bt.Delete(nil)
	assert.True(t, res1)

	bt.Put([]byte("a"), &data.LogRecordPos{2, 111})
	res2 := bt.Delete([]byte("a"))
	assert.True(t, res2)
}
