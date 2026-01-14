package bitcask_gown

type Options struct {
	DirPath      string // 文件路径信息
	DataFileSize int64  // 数据文件最大的大小
	SyncWrites   bool   // 是否选择执行持久化
}

var DefaultOptions = Options{
	DirPath:      ".",
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
}

type WriteBatchSetup struct {
	// 单批次最大数据量
	MaxBatchNum uint
	// 每一次事务是否要持久化
	SyncWrites bool
}

var DefaultWriteBatchSetup = WriteBatchSetup{
	MaxBatchNum: 4,
	SyncWrites:  false,
}

func checkOptions(opt Options) error {
	if len(opt.DirPath) == 0 {
		return ErrDirPathIsEmpty
	}
	if opt.DataFileSize <= 0 {
		return ErrInvalidDataFileSize
	}

	return nil
}
