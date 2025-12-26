package fio

import "os"

type FileIO struct {
	f *os.File // 为什么选择 *os.File 而不是 os.File
}

func (fio FileIO) Read(key []byte, offset int64) (int, error) {
	return fio.f.ReadAt(key, offset)
}

func (fio FileIO) Write(key []byte) (int, error) {
	return fio.f.WriteAt(key, 0)
}

func (fio FileIO) Sync() error {
	return fio.f.Sync()
}

func (fio FileIO) Close() error {
	return fio.f.Close()
}
