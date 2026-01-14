package bitcask_gown

import "errors"

var (
	ErrKeyIsEmpty           = errors.New("key is empty")
	ErrIndexUpdateFailed    = errors.New("index update failed")
	ErrIndexNotFound        = errors.New("index not found")
	ErrDataFileNotFound     = errors.New("data file not found")
	ErrDirPathIsEmpty       = errors.New("directory path is empty")
	ErrInvalidDataFileSize  = errors.New("invalid data file size, database file size must be greater than 0")
	ErrKeyNotFound          = errors.New("key not found")
	ErrIndexDeleteFailed    = errors.New("index delete failed")
	ErrPendingWritesInvalid = errors.New("pending writes unvalid")
	ErrExceedMaxBatchNum    = errors.New("exceed max batch num")
)
