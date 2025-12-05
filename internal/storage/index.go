package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	IndexPageHdrSize = 8 // 4 root flag + 4 num keys
	MaxKeysPerNode   = 64
)

type IndexKey []byte

// IndexNode represents a single B-Tree node
type IndexNode struct {
	IsLeaf   bool
	Keys     []IndexKey
	TIDs     [][]TID  // Only for leaf nodes
	Children []uint64 // Page IDs for internal nodes
	PageID   uint64
}

// Index represents the B-Tree index for a table
type Index struct {
	Pager      *Pager
	RootPageID uint64
}

// NewIndex initializes a new B-Tree index (root page created if needed)
func NewIndex(pager *Pager) (*Index, error) {
	idx := &Index{
		Pager: pager,
	}
	// If no pages, allocate root
	numPages, err := pager.NumPages()
	if err != nil {
		return nil, err
	}
	if numPages == 0 {
		root := &IndexNode{
			IsLeaf: true,
			PageID: 0,
			Keys:   []IndexKey{},
			TIDs:   [][]TID{},
		}
		if err := idx.writeNode(root); err != nil {
			return nil, err
		}
		idx.RootPageID = 0
	} else {
		// assume first page is root
		idx.RootPageID = 0
	}
	return idx, nil
}

// Insert a key + TID into the index
func (idx *Index) Insert(key IndexKey, tid TID) error {
	root, err := idx.readNode(idx.RootPageID)
	if err != nil {
		return err
	}
	newRoot, err := idx.insertRecursive(root, key, tid)
	if err != nil {
		return err
	}
	if newRoot != nil {
		// root split, assign new root page
		idx.RootPageID = newRoot.PageID
	}
	return nil
}

// insertRecursive returns new node if split occurs
func (idx *Index) insertRecursive(node *IndexNode, key IndexKey, tid TID) (*IndexNode, error) {
	if node.IsLeaf {
		// find position to insert
		i := 0
		for i < len(node.Keys) && bytes.Compare(node.Keys[i], key) < 0 {
			i++
		}
		// Check if key already exists (duplicate key)
		if i < len(node.Keys) && bytes.Equal(node.Keys[i], key) {
			// Append TID to existing list
			node.TIDs[i] = append(node.TIDs[i], tid)
			return nil, idx.writeNode(node)
		}
		// Insert new key & TID
		node.Keys = append(node.Keys[:i], append([]IndexKey{key}, node.Keys[i:]...)...)
		node.TIDs = append(node.TIDs[:i], append([][]TID{{tid}}, node.TIDs[i:]...)...)
		if len(node.Keys) <= MaxKeysPerNode {
			return nil, idx.writeNode(node)
		}
		// split
		return idx.splitLeaf(node)
	}

	// internal node: find child to recurse
	i := 0
	for i < len(node.Keys) && bytes.Compare(node.Keys[i], key) <= 0 {
		i++
	}
	child, err := idx.readNode(node.Children[i])
	if err != nil {
		return nil, err
	}
	newChild, err := idx.insertRecursive(child, key, tid)
	if err != nil {
		return nil, err
	}
	if newChild == nil {
		return nil, idx.writeNode(node)
	}
	// child split, insert new key & child
	midKey := newChild.Keys[0]
	node.Keys = append(node.Keys[:i], append([]IndexKey{midKey}, node.Keys[i:]...)...)
	node.Children = append(node.Children[:i+1], append([]uint64{newChild.PageID}, node.Children[i+1:]...)...)
	if len(node.Keys) > MaxKeysPerNode {
		return idx.splitInternal(node)
	}
	return nil, idx.writeNode(node)
}

// splitLeaf splits a leaf node
func (idx *Index) splitLeaf(node *IndexNode) (*IndexNode, error) {
	mid := len(node.Keys) / 2
	right := &IndexNode{
		IsLeaf: true,
		PageID: 0, // assign later
		Keys:   append([]IndexKey{}, node.Keys[mid:]...),
		TIDs:   append([][]TID{}, node.TIDs[mid:]...),
	}
	node.Keys = node.Keys[:mid]
	node.TIDs = node.TIDs[:mid]

	// allocate page
	numPages, _ := idx.Pager.NumPages()
	right.PageID = numPages
	if err := idx.writeNode(node); err != nil {
		return nil, err
	}
	if err := idx.writeNode(right); err != nil {
		return nil, err
	}
	// new root
	newRoot := &IndexNode{
		IsLeaf:   false,
		PageID:   numPages + 1,
		Keys:     []IndexKey{right.Keys[0]},
		Children: []uint64{node.PageID, right.PageID},
	}
	if err := idx.writeNode(newRoot); err != nil {
		return nil, err
	}
	return newRoot, nil
}

// splitInternal splits an internal node
func (idx *Index) splitInternal(node *IndexNode) (*IndexNode, error) {
	mid := len(node.Keys) / 2
	right := &IndexNode{
		IsLeaf:   false,
		PageID:   0, // assign later
		Keys:     append([]IndexKey{}, node.Keys[mid+1:]...),
		Children: append([]uint64{}, node.Children[mid+1:]...),
	}
	upKey := node.Keys[mid]

	node.Keys = node.Keys[:mid]
	node.Children = node.Children[:mid+1]

	numPages, _ := idx.Pager.NumPages()
	right.PageID = numPages

	if err := idx.writeNode(node); err != nil {
		return nil, err
	}
	if err := idx.writeNode(right); err != nil {
		return nil, err
	}

	// new root
	newRoot := &IndexNode{
		IsLeaf:   false,
		PageID:   numPages + 1,
		Keys:     []IndexKey{upKey},
		Children: []uint64{node.PageID, right.PageID},
	}
	if err := idx.writeNode(newRoot); err != nil {
		return nil, err
	}
	return newRoot, nil
}

// Search for a key, returns empty slice if not found
func (idx *Index) Search(key IndexKey) ([]TID, error) {
	node, err := idx.readNode(idx.RootPageID)
	if err != nil {
		return nil, err
	}
	for !node.IsLeaf {
		i := 0
		for i < len(node.Keys) && bytes.Compare(node.Keys[i], key) <= 0 {
			i++
		}
		node, err = idx.readNode(node.Children[i])
		if err != nil {
			return nil, err
		}
	}
	// Binary search in leaf node
	for i, k := range node.Keys {
		if bytes.Equal(k, key) {
			return node.TIDs[i], nil
		}
	}
	// Key not found, return empty slice
	return []TID{}, nil
}

// ---------------- Page I/O -----------------

func (idx *Index) writeNode(node *IndexNode) error {
	buf := make([]byte, PageSize)
	if node.IsLeaf {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	buf[1] = byte(len(node.Keys))
	offset := 2
	for i, k := range node.Keys {
		// Write key length (4 bytes) followed by key data
		keyLen := uint32(len(k))
		if offset+4+int(keyLen) > PageSize {
			return fmt.Errorf("key too large for page")
		}
		binary.LittleEndian.PutUint32(buf[offset:], keyLen)
		offset += 4
		copy(buf[offset:], k)
		offset += int(keyLen)

		if node.IsLeaf {
			tids := node.TIDs[i]
			if offset+4 > PageSize {
				return fmt.Errorf("not enough space for TID count")
			}
			binary.LittleEndian.PutUint32(buf[offset:], uint32(len(tids)))
			offset += 4
			for _, tid := range tids {
				if offset+12 > PageSize {
					return fmt.Errorf("not enough space for TID")
				}
				binary.LittleEndian.PutUint64(buf[offset:], tid.PageID)
				offset += 8
				binary.LittleEndian.PutUint32(buf[offset:], tid.SlotID)
				offset += 4
			}
		}
	}
	page := &Page{ID: node.PageID, Data: buf}
	return idx.Pager.WritePage(page)
}

func (idx *Index) readNode(pageID uint64) (*IndexNode, error) {
	page, err := idx.Pager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	node := &IndexNode{
		PageID: pageID,
	}
	node.IsLeaf = page.Data[0] == 1
	numKeys := int(page.Data[1])
	offset := 2
	node.Keys = make([]IndexKey, numKeys)
	if node.IsLeaf {
		node.TIDs = make([][]TID, numKeys)
	}
	for i := 0; i < numKeys; i++ {
		// Read key length (4 bytes) followed by key data
		if offset+4 > PageSize {
			return nil, fmt.Errorf("corrupt index node: key length out of bounds")
		}
		keyLen := binary.LittleEndian.Uint32(page.Data[offset:])
		offset += 4
		if offset+int(keyLen) > PageSize {
			return nil, fmt.Errorf("corrupt index node: key data out of bounds")
		}
		node.Keys[i] = make(IndexKey, keyLen)
		copy(node.Keys[i], page.Data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		if node.IsLeaf {
			if offset+4 > PageSize {
				return nil, fmt.Errorf("corrupt index node: TID count out of bounds")
			}
			n := binary.LittleEndian.Uint32(page.Data[offset:])
			offset += 4
			tids := make([]TID, n)
			for j := 0; j < int(n); j++ {
				if offset+12 > PageSize {
					return nil, fmt.Errorf("corrupt index node: TID out of bounds")
				}
				tids[j].PageID = binary.LittleEndian.Uint64(page.Data[offset:])
				offset += 8
				tids[j].SlotID = binary.LittleEndian.Uint32(page.Data[offset:])
				offset += 4
			}
			node.TIDs[i] = tids
		}
	}
	return node, nil
}
