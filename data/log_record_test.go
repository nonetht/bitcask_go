package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: 关键就是 EncodeLogRecord 也是错误！测试覆盖根本就不行，还是忽略了。
func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	res1, n1 := EncodeLogRecord(rec1)
	//t.Log(res1) // [104 82 240 150 0 8 20 110 97 109 101 98 105 116 99 97 115 107 45 103 111]
	//t.Log(n1)   // 21
	assert.NotNil(t, res1)          // 断言 res1 不是 nil
	assert.Greater(t, n1, int64(5)) // header 的结构就是：crc占据4个，type占据1个。同时可能是空的键值对。

	// value 为空的情况
	// 但是怎样才可以定义为空呢？是nil还是空的字符串呢？
	// 俺也不知道，一会试一试
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte(nil),
		Type:  LogRecordNormal,
	}

	// 对 Delete 情况的测试
	res2, n2 := EncodeLogRecord(rec2)
	//t.Log(res2) // [9 252 88 14 0 8 0 110 97 109 101]
	//t.Log(n2)   // 11
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}

	res3, n3 := EncodeLogRecord(rec3)
	t.Log(res3) // [43 153 86 17 1 8 20 110 97 109 101 98 105 116 99 97 115 107 45 103 111]
	t.Log(n3)   // 21
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecord(t *testing.T) {
	// normal case
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	//t.Log(h1)    // &{2532332136 0 4 10}: 分别对应上crc, type, keySize, valueSize
	//t.Log(size1) // 7 - 取前面7个字节即可
	assert.NotNil(t, h1)
	// 因为是 Decode 的是 header 部分，那么算上crc + Type + keySize + valueSize 部分在[]byte长度就是7
	assert.Equal(t, int64(7), size1)
	// 下面的长数字是通过读取 h1 也就是 &logRecordHeader 部分来提取得到的
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	// value is nil
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	//t.Log(h2) // &{240712713 0 4 0}
	//t.Log(size2) // 7

	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := decodeLogRecordHeader(headerBuf3)
	//t.Log(h3) // &{290887979 1 4 10}
	//t.Log(size3) // 7
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

// 虽然和上一个函数测试的内容大致相同，只是，这个函数是为了测试 getLogRecordCRC 方法
func TestGetLogRecordCRC(t *testing.T) {
	// normal case
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:]) // 其中需要注意就是，计算crc的时候，不包含header部分的[:4]的内容！
	assert.Equal(t, uint32(2532332136), crc1)

	// value is nil
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte(nil),
		Type:  LogRecordNormal,
	}

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	// deleted type
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:]) // crc32.Size is constant, which val is 4
	assert.Equal(t, uint32(290887979), crc3)
}
