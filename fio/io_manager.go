package fio

// IOManager 通过实现下面四种方法，表现像一个 IOManager。此外，就是可让其他（除了文件IO）实现了这些方式也可以作为 IOManager
type IOManager interface {
	Read(key []byte, offset int64) (int, error)
	Write(key []byte) (int, error)
	// Sync 将数据持久化到文件之中
	Sync() error
	// Close 关闭文件句柄。
	Close() error
}
