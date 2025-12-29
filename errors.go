package bitcask_gown

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("key is empty")
	ErrIndexUpdateFailed = errors.New("index update failed")
	ErrIndexNotFound     = errors.New("index not found")
)
