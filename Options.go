package bitcask_gown

type Options struct {
	DirPath      string // 文件路径信息
	DataFileSize int64  // 数据文件最大的大小
	SyncWrites   bool   // 是否选择执行持久化
}
