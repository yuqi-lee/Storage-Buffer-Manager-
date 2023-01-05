package buffer_manager

func Hash(pageID int32) int32 {
	return pageID % BufferSize
}
