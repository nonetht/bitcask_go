package fio

import "os"

// FileIO 创建负责文件输入输出的结构体
type FileIO struct {
	f *os.File
}

// NewFileIOManager 创建新的文件IO管理器
func NewFileIOManager(fileName string) (*FileIO, error) {
	f, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // os.O_APPEND 尤其重要，因为我们是采用的追加写入的方式！
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{f: f}, nil
}

// Read 从文件的 offset 处读取数据到 b 中
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.f.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.f.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.f.Sync()
}

func (fio *FileIO) Close() error {
	return fio.f.Close()
}

func (fio *FileIO) Size() int64 {
	return 0
}
