package storage

import (
	"path/filepath"
	"testing"
)

func setupTestIndex(t *testing.T) (*Index, string) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")
	pager := NewPager(indexPath)
	idx, err := NewIndex(pager)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	return idx, indexPath
}

func TestNewIndex_CreatesEmptyIndex(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	if idx.RootPageID != 0 {
		t.Errorf("Expected RootPageID to be 0, got %d", idx.RootPageID)
	}

	// Verify root page exists
	root, err := idx.readNode(idx.RootPageID)
	if err != nil {
		t.Fatalf("Failed to read root node: %v", err)
	}

	if !root.IsLeaf {
		t.Error("Root node should be a leaf for empty index")
	}

	if len(root.Keys) != 0 {
		t.Errorf("Expected 0 keys in empty index, got %d", len(root.Keys))
	}
}

func TestIndex_InsertAndSearch_SingleKey(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	key := IndexKey{1, 2, 3, 4, 5, 6, 7, 8}
	tid := TID{PageID: 0, SlotID: 0}

	err := idx.Insert(key, tid)
	if err != nil {
		t.Fatalf("Failed to insert key: %v", err)
	}

	results, err := idx.Search(key)
	if err != nil {
		t.Fatalf("Failed to search for key: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 TID, got %d", len(results))
	}

	if results[0] != tid {
		t.Errorf("Expected TID %v, got %v", tid, results[0])
	}
}

func TestIndex_InsertAndSearch_MultipleKeys(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	keys := []IndexKey{
		{1, 0, 0, 0, 0, 0, 0, 0},
		{2, 0, 0, 0, 0, 0, 0, 0},
		{3, 0, 0, 0, 0, 0, 0, 0},
		{4, 0, 0, 0, 0, 0, 0, 0},
		{5, 0, 0, 0, 0, 0, 0, 0},
	}

	// Insert keys
	for i, key := range keys {
		tid := TID{PageID: uint64(i), SlotID: uint32(i)}
		err := idx.Insert(key, tid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Search for each key
	for i, key := range keys {
		results, err := idx.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 TID for key %d, got %d", i, len(results))
		}

		expectedTID := TID{PageID: uint64(i), SlotID: uint32(i)}
		if results[0] != expectedTID {
			t.Errorf("Key %d: Expected TID %v, got %v", i, expectedTID, results[0])
		}
	}
}

func TestIndex_InsertDuplicateKeys(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	key := IndexKey{1, 2, 3, 4, 5, 6, 7, 8}
	tid1 := TID{PageID: 0, SlotID: 0}
	tid2 := TID{PageID: 1, SlotID: 1}
	tid3 := TID{PageID: 2, SlotID: 2}

	// Insert same key multiple times
	err := idx.Insert(key, tid1)
	if err != nil {
		t.Fatalf("Failed to insert first TID: %v", err)
	}

	err = idx.Insert(key, tid2)
	if err != nil {
		t.Fatalf("Failed to insert second TID: %v", err)
	}

	err = idx.Insert(key, tid3)
	if err != nil {
		t.Fatalf("Failed to insert third TID: %v", err)
	}

	// Search should return all TIDs
	results, err := idx.Search(key)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 TIDs, got %d", len(results))
	}

	expectedTIDs := []TID{tid1, tid2, tid3}
	for i, expected := range expectedTIDs {
		found := false
		for _, result := range results {
			if result == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("TID %d (%v) not found in results", i, expected)
		}
	}
}

func TestIndex_Search_NonExistentKey(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	// Insert some keys
	key1 := IndexKey{1, 0, 0, 0, 0, 0, 0, 0}
	key2 := IndexKey{2, 0, 0, 0, 0, 0, 0, 0}
	idx.Insert(key1, TID{PageID: 0, SlotID: 0})
	idx.Insert(key2, TID{PageID: 1, SlotID: 1})

	// Search for non-existent key
	nonExistentKey := IndexKey{99, 0, 0, 0, 0, 0, 0, 0}
	results, err := idx.Search(nonExistentKey)
	if err != nil {
		t.Fatalf("Search should not return error for non-existent key: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected empty results for non-existent key, got %d results", len(results))
	}
}

func TestIndex_VariableLengthKeys(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	// Test with different key lengths (simulating text keys)
	keys := []IndexKey{
		{0, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'},           // 5 bytes
		{0, 0, 0, 0, 'w', 'o', 'r', 'l', 'd'},           // 5 bytes
		{0, 0, 0, 0, 't', 'e', 's', 't'},                // 4 bytes
		{0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g'}, // 7 bytes
	}

	for i, key := range keys {
		tid := TID{PageID: uint64(i), SlotID: uint32(i)}
		err := idx.Insert(key, tid)
		if err != nil {
			t.Fatalf("Failed to insert variable length key %d: %v", i, err)
		}
	}

	// Verify all keys can be found
	for i, key := range keys {
		results, err := idx.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 TID for key %d, got %d", i, len(results))
		}

		expectedTID := TID{PageID: uint64(i), SlotID: uint32(i)}
		if results[0] != expectedTID {
			t.Errorf("Key %d: Expected TID %v, got %v", i, expectedTID, results[0])
		}
	}
}

func TestIndex_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "persist.idx")

	// Create index and insert data
	{
		pager := NewPager(indexPath)
		idx, err := NewIndex(pager)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}

		keys := []IndexKey{
			{1, 0, 0, 0, 0, 0, 0, 0},
			{2, 0, 0, 0, 0, 0, 0, 0},
			{3, 0, 0, 0, 0, 0, 0, 0},
		}

		for i, key := range keys {
			tid := TID{PageID: uint64(i), SlotID: uint32(i)}
			err := idx.Insert(key, tid)
			if err != nil {
				t.Fatalf("Failed to insert key %d: %v", i, err)
			}
		}

		pager.Close()
	}

	// Reopen index and verify data
	{
		pager := NewPager(indexPath)
		idx, err := NewIndex(pager)
		if err != nil {
			t.Fatalf("Failed to reopen index: %v", err)
		}
		defer pager.Close()

		keys := []IndexKey{
			{1, 0, 0, 0, 0, 0, 0, 0},
			{2, 0, 0, 0, 0, 0, 0, 0},
			{3, 0, 0, 0, 0, 0, 0, 0},
		}

		for i, key := range keys {
			results, err := idx.Search(key)
			if err != nil {
				t.Fatalf("Failed to search for key %d: %v", i, err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 TID for key %d, got %d", i, len(results))
			}

			expectedTID := TID{PageID: uint64(i), SlotID: uint32(i)}
			if results[0] != expectedTID {
				t.Errorf("Key %d: Expected TID %v, got %v", i, expectedTID, results[0])
			}
		}
	}
}

func TestIndex_ManyKeys_TriggersSplit(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	// Insert more keys than MaxKeysPerNode to trigger splits
	numKeys := MaxKeysPerNode * 3
	for i := 0; i < numKeys; i++ {
		key := IndexKey{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24), 0, 0, 0, 0}
		tid := TID{PageID: uint64(i), SlotID: uint32(i)}
		err := idx.Insert(key, tid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Verify all keys can be found
	for i := 0; i < numKeys; i++ {
		key := IndexKey{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24), 0, 0, 0, 0}
		results, err := idx.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 TID for key %d, got %d", i, len(results))
		}

		expectedTID := TID{PageID: uint64(i), SlotID: uint32(i)}
		if results[0] != expectedTID {
			t.Errorf("Key %d: Expected TID %v, got %v", i, expectedTID, results[0])
		}
	}
}

func TestIndex_InsertOrder_DoesNotMatter(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	// Insert keys in reverse order
	keys := []IndexKey{
		{5, 0, 0, 0, 0, 0, 0, 0},
		{4, 0, 0, 0, 0, 0, 0, 0},
		{3, 0, 0, 0, 0, 0, 0, 0},
		{2, 0, 0, 0, 0, 0, 0, 0},
		{1, 0, 0, 0, 0, 0, 0, 0},
	}

	for i, key := range keys {
		tid := TID{PageID: uint64(i), SlotID: uint32(i)}
		err := idx.Insert(key, tid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// All keys should still be findable
	for i, key := range keys {
		results, err := idx.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 TID for key %d, got %d", i, len(results))
		}
	}
}

func TestIndex_WriteNode_ReadNode_RoundTrip(t *testing.T) {
	idx, _ := setupTestIndex(t)
	defer idx.Pager.Close()

	// Create a test node
	node := &IndexNode{
		IsLeaf: true,
		PageID: 1,
		Keys: []IndexKey{
			{1, 2, 3, 4, 5, 6, 7, 8},
			{9, 10, 11, 12, 13, 14, 15, 16},
		},
		TIDs: [][]TID{
			{{PageID: 0, SlotID: 0}},
			{{PageID: 1, SlotID: 1}},
		},
	}

	// Write node
	err := idx.writeNode(node)
	if err != nil {
		t.Fatalf("Failed to write node: %v", err)
	}

	// Read node back
	readNode, err := idx.readNode(node.PageID)
	if err != nil {
		t.Fatalf("Failed to read node: %v", err)
	}

	// Verify node properties
	if readNode.IsLeaf != node.IsLeaf {
		t.Errorf("IsLeaf mismatch: expected %v, got %v", node.IsLeaf, readNode.IsLeaf)
	}

	if len(readNode.Keys) != len(node.Keys) {
		t.Fatalf("Key count mismatch: expected %d, got %d", len(node.Keys), len(readNode.Keys))
	}

	for i, key := range node.Keys {
		if len(readNode.Keys[i]) != len(key) {
			t.Errorf("Key %d length mismatch: expected %d, got %d", i, len(key), len(readNode.Keys[i]))
		}
		for j, b := range key {
			if readNode.Keys[i][j] != b {
				t.Errorf("Key %d byte %d mismatch: expected %d, got %d", i, j, b, readNode.Keys[i][j])
			}
		}
	}

	if len(readNode.TIDs) != len(node.TIDs) {
		t.Fatalf("TID count mismatch: expected %d, got %d", len(node.TIDs), len(readNode.TIDs))
	}

	for i, tids := range node.TIDs {
		if len(readNode.TIDs[i]) != len(tids) {
			t.Errorf("TID list %d length mismatch: expected %d, got %d", i, len(tids), len(readNode.TIDs[i]))
		}
		for j, tid := range tids {
			if readNode.TIDs[i][j] != tid {
				t.Errorf("TID list %d item %d mismatch: expected %v, got %v", i, j, tid, readNode.TIDs[i][j])
			}
		}
	}
}

func TestIndex_VariableLengthKey_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "varlen.idx")

	// Create index with variable length keys
	{
		pager := NewPager(indexPath)
		idx, err := NewIndex(pager)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}

		// Insert keys of different lengths
		keys := []IndexKey{
			{0, 0, 0, 4, 't', 'e', 's', 't'},                                     // 4 bytes
			{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'},                                // 5 bytes
			{0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'}, // 11 bytes
		}

		for i, key := range keys {
			tid := TID{PageID: uint64(i), SlotID: uint32(i)}
			err := idx.Insert(key, tid)
			if err != nil {
				t.Fatalf("Failed to insert key %d: %v", i, err)
			}
		}

		pager.Close()
	}

	// Reopen and verify
	{
		pager := NewPager(indexPath)
		idx, err := NewIndex(pager)
		if err != nil {
			t.Fatalf("Failed to reopen index: %v", err)
		}
		defer pager.Close()

		keys := []IndexKey{
			{0, 0, 0, 4, 't', 'e', 's', 't'},
			{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'},
			{0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		}

		for i, key := range keys {
			results, err := idx.Search(key)
			if err != nil {
				t.Fatalf("Failed to search for key %d: %v", i, err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 TID for key %d, got %d", i, len(results))
			}

			expectedTID := TID{PageID: uint64(i), SlotID: uint32(i)}
			if results[0] != expectedTID {
				t.Errorf("Key %d: Expected TID %v, got %v", i, expectedTID, results[0])
			}
		}
	}
}
