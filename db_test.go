package bitcask_gown

import (
	"bitcask-gown/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const smallDataFileSize = 128

// destroyDB closes the db and removes its directory so tests stay isolated.
func destroyDB(db *DB) {
	if db == nil {
		return
	}
	_ = db.Close()
	_ = os.RemoveAll(db.option.DirPath)
}

// newDB creates a DB in a temp directory and returns a cleanup function.
func newDB(t *testing.T, setup Options) (*DB, func()) {
	t.Helper()
	setup.DirPath = t.TempDir()
	db, err := Open(setup)
	require.NoError(t, err)
	return db, func() { destroyDB(db) }
}

// TestOpen ensures database opens with the default setup.
func TestOpen(t *testing.T) {
	setup := DefaultOptions
	setup.DirPath = t.TempDir()
	db, err := Open(setup)
	require.NoError(t, err)
	assert.NotNil(t, db)
	destroyDB(db)
}

// TestDB_PutAndGet covers normal put/get and overwriting a key.
func TestDB_PutAndGet(t *testing.T) {
	db, cleanup := newDB(t, DefaultOptions)
	defer cleanup()

	key := utils.GetTestKey(1)
	val1 := utils.RandomValue(16)
	val2 := utils.RandomValue(24)

	require.NoError(t, db.Put(key, val1))
	got1, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val1, got1)

	require.NoError(t, db.Put(key, val2))
	got2, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val2, got2)
}

// TestDB_PutEmptyKey verifies empty key is rejected.
func TestDB_PutEmptyKey(t *testing.T) {
	db, cleanup := newDB(t, DefaultOptions)
	defer cleanup()

	err := db.Put([]byte(""), utils.RandomValue(8))
	assert.Equal(t, ErrKeyIsEmpty, err)
}

// TestDB_DeleteFlow covers deleting existing/missing keys and reinserting after delete.
func TestDB_DeleteFlow(t *testing.T) {
	db, cleanup := newDB(t, DefaultOptions)
	defer cleanup()

	key := utils.GetTestKey(2)
	value := utils.RandomValue(12)

	require.NoError(t, db.Put(key, value))
	require.NoError(t, db.Delete(key))

	_, err := db.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)

	require.NoError(t, db.Delete([]byte("unknown key")))
	assert.Equal(t, ErrKeyIsEmpty, db.Delete(nil))

	require.NoError(t, db.Put(key, value))
	got, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, got)
}

// TestDB_FileRotation ensures small DataFileSize triggers segment rollover.
func TestDB_FileRotation(t *testing.T) {
	setup := DefaultOptions
	setup.DataFileSize = smallDataFileSize

	db, cleanup := newDB(t, setup)
	defer cleanup()

	val := utils.RandomValue(32)
	require.NoError(t, db.Put(utils.GetTestKey(3), val))
	require.NoError(t, db.Put(utils.GetTestKey(4), val))

	assert.GreaterOrEqual(t, len(db.oldFiles), 1)
}

// TestDB_RestartPersistsData ensures data remains after closing and reopening.
func TestDB_RestartPersistsData(t *testing.T) {
	setup := DefaultOptions
	setup.DirPath = t.TempDir()

	db, err := Open(setup)
	require.NoError(t, err)

	key := utils.GetTestKey(5)
	val := utils.RandomValue(20)
	require.NoError(t, db.Put(key, val))

	require.NoError(t, db.Close())

	reopened, err := Open(setup)
	require.NoError(t, err)
	defer destroyDB(reopened)

	got, err := reopened.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val, got)
}
