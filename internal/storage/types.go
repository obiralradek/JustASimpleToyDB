package storage

const (
	PageTypeHeap  = 1
	PageTypeIndex = 2
	PageTypeMeta  = 3
)

const (
	TupleFlagNormal  = 0
	TupleFlagDeleted = 1
)

type TID struct {
	PageID uint64
	SlotID uint32
}
