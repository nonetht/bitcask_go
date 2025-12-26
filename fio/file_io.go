package fio

import "os"

type FileIO struct {
	f *os.File // 为什么选择 *os.File 而不是 os.File
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	f, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{f: f}, nil
}

func (fio *FileIO) Read(key []byte, offset int64) (int, error) {
	return fio.f.ReadAt(key, offset)
}

func (fio *FileIO) Write(key []byte) (int, error) {
	return fio.f.Write(key)
}

func (fio *FileIO) Sync() error {
	return fio.f.Sync()
}

func (fio *FileIO) Close() error {
	return fio.f.Close()
}
