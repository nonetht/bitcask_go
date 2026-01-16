package bitcask_gown

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	setup := DefaultSetup
	setup.DirPath = t.TempDir() // 返回临时文件夹，测试完成后自动删除
	db, err := Open(setup)
	require.NoError(t, err)
	return db, func() { destroyDB(db) }
}

func TestWriteBatch_PutAndCommit(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	batch := db.NewWriteBatch(DefaultWriteBatchSetup)
	err := batch.Put([]byte("key-a"), []byte("value-a"))
	require.NoError(t, err) // require 和 assert 有什么区别吗？前者 require 是 hard fail，如果断言失败，立即终止
	assert.Len(t, batch.pendingWrites, 1)

	err = batch.Commit()
	require.NoError(t, err)
	assert.Len(t, batch.pendingWrites, 0)

	val, err := db.Get([]byte("key-a"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value-a"), val)
}

func TestWriteBatch_DeletePendingWrite(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	batch := db.NewWriteBatch(DefaultWriteBatchSetup)
	key := []byte("ephemeral-key")
	require.NoError(t, batch.Put(key, []byte("temp-value")))
	assert.Len(t, batch.pendingWrites, 1)

	require.NoError(t, batch.Delete(key))
	assert.Len(t, batch.pendingWrites, 0)

	require.NoError(t, batch.Commit())
	_, err := db.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestWriteBatch_DeleteExistingKey(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	key := []byte("persisted-key")
	value := []byte("persisted-value")
	require.NoError(t, db.Put(key, value))

	batch := db.NewWriteBatch(DefaultWriteBatchSetup)
	require.NoError(t, batch.Delete(key))
	assert.Len(t, batch.pendingWrites, 1)

	require.NoError(t, batch.Commit())
	_, err := db.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestWriteBatch_DeleteMissingKeyNoop(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	batch := db.NewWriteBatch(DefaultWriteBatchSetup)
	require.NoError(t, batch.Delete([]byte("missing-key")))
	assert.Len(t, batch.pendingWrites, 0)
	require.NoError(t, batch.Commit())
}

func TestWriteBatch_CommitExceedMaxBatchNum(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	batch := db.NewWriteBatch(WriteBatchSetup{MaxBatchNum: 1})
	require.NoError(t, batch.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, batch.Put([]byte("k2"), []byte("v2")))

	err := batch.Commit()
	assert.Equal(t, ErrExceedMaxBatchNum, err)
	assert.Len(t, batch.pendingWrites, 2)

	_, err = db.Get([]byte("k1"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&db.seqNumber))
}

func TestWriteBatch_CommitIncrementsSeqNumber(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	batch1 := db.NewWriteBatch(DefaultWriteBatchSetup)
	require.NoError(t, batch1.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, batch1.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, batch1.Commit())
	assert.Equal(t, uint64(1), atomic.LoadUint64(&db.seqNumber))

	batch2 := db.NewWriteBatch(DefaultWriteBatchSetup)
	require.NoError(t, batch2.Put([]byte("k3"), []byte("v3")))
	require.NoError(t, batch2.Commit())
	assert.Equal(t, uint64(2), atomic.LoadUint64(&db.seqNumber))
}

func TestAddSeqToKeyAndParse(t *testing.T) {
	originalKey := []byte("key-with-seq")
	originalSeq := uint64(987654321)

	encoded := addSeqToKey(originalKey, originalSeq)
	realKey, parsedSeq := parseLogRecordKey(encoded)

	assert.Equal(t, originalKey, realKey)
	assert.Equal(t, originalSeq, parsedSeq)
}
