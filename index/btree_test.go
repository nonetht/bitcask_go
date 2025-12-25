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
	pos1 := bt.Get(nil) // pos1 类型是 *data.LogRecordPos
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	// 测试获取key="a"对应值的情况
	bt.Put([]byte("a"), &data.LogRecordPos{2, 2})

	pos2 := bt.Get([]byte("a")) // []byte类型总感觉怪...
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)

	// 连续两次Put函数添加，会改变key对应的value，测试value是否如期改变
	bt.Put([]byte("a"), &data.LogRecordPos{1, 3})
	pos3 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos3.Fid)
	assert.Equal(t, int64(3), pos3.Offset)
	t.Log(pos3)
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

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()

	// 如何测试我们的迭代器呢？对于迭代器的功能，我们应该怎样测试呢？还有就是像 roseduan 这种测试的方法实在是不太好。
	// TODO: 因为万一前面的 case 没有通过测试，那么后面的测试也是无法通过。我的想法是，将所有的 case 拆分为单独的子测试出来。
	// 1. btree 为空的情况下
	iter1 := bt1.Iterator(false)
	assert.Equal(t, iter1.Valid(), false)

	// 2. Put 进去一个值的时候，测试其 (Key，Value)
	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iter2 := bt1.Iterator(false)
	assert.True(t, iter2.Valid())
	//t.Log(iter2.Key())   // [99, 111, 100, 1010]
	//t.Log(iter2.Value()) // &{1 100}
	//assert.Equal(t, []byte("code"), iter2.Key())
	//assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 100}, iter2.Key())
	// 随后我们执行了测试如下，但是我感觉既然都看到了打印结果了，这条测试是否还有意义呢？我是认为这条测试没什么意义，总感觉是画蛇添足。
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())

	iter2.Next()
	// 我还是感觉有些画蛇添足，既然通过 Log 函数知道了结果，为什么还要再次验证呢？
	//t.Log(iter2.Valid()) // false
	assert.Equal(t, iter2.Valid(), false)

	// 3. 多条数据的话呢？
	// TODO: 我不太理解，就是第二个参数 LogRecordPos 的意义，我的理解是指定文件名称和位置，这种在同一位置连续 Put，会造成原本的被覆盖掉吗？
	bt1.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("b"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("c"), &data.LogRecordPos{Fid: 1, Offset: 10})

	iter3 := bt1.Iterator(false)
	assert.True(t, iter3.Valid())

	//var offset int
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
		// TODO: 完善当前的测试，应该进一步提高测试准确性。
		//assert.Equal(t, string(iter3.Key()), string(rune('a'+offset)))
		//offset++
	}

	iter4 := bt1.Iterator(true)
	assert.True(t, iter4.Valid())

	//var offset int
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
		// TODO: 完善当前的测试，应该进一步提高测试准确性。
		//assert.Equal(t, string(iter3.Key()), string(rune('a'+offset)))
		//offset++
	}

	// 4. 测试 Seek 函数
	iter5 := bt1.Iterator(false)
	iter5.Seek([]byte("cc"))
	t.Log(iter5.Key(), iter5.Value())

	// 5. 测试反向情况下的 Seek 函数
}
