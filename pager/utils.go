package pager

func PageFileOffset(pageId uint32) uint32 {
	return DATABASE_HEADER_SIZE + (pageId * PAGE_SIZE)
}
