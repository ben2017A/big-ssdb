package store

// LogStore 是一种特殊的数据库, 数据以滑动窗口的方式保存和淘汰.
// 日志保存在多个文件中, 淘汰时以文件为单位.
type LogStore struct {
	
}
