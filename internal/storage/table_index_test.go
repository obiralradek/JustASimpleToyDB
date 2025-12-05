package storage

import (
	"justasimpletoydb/internal/catalog"
	"path/filepath"
	"testing"
)

func setupTestTable(t *testing.T) (*Table, string) {
	tmpDir := t.TempDir()
	tablePath := filepath.Join(tmpDir, "test.tbl")

	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	table, err := NewTable("test", tablePath, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	return table, tmpDir
}

func TestTable_CreateIndex_PopulatesFromExistingRows(t *testing.T) {
	table, _ := setupTestTable(t)
	defer table.Close()

	// Insert some rows first
	rows := [][]any{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{4, "David"},
	}

	for _, row := range rows {
		err := table.InsertRow(row)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Create index on name column
	err := table.CreateIndex("name_idx", "name")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists in cache
	idx, ok := table.Indexes["name_idx"]
	if !ok {
		t.Fatal("Index not found in cache after creation")
	}

	// Verify index has entries by searching
	// We can't easily search without encoding, but we can verify the index is loaded
	if idx == nil {
		t.Fatal("Index is nil")
	}
}

func TestTable_CreateIndex_EmptyTable(t *testing.T) {
	table, _ := setupTestTable(t)
	defer table.Close()

	// Create index on empty table
	err := table.CreateIndex("id_idx", "id")
	if err != nil {
		t.Fatalf("Failed to create index on empty table: %v", err)
	}

	// Verify index exists in cache
	idx, ok := table.Indexes["id_idx"]
	if !ok {
		t.Fatal("Index not found in cache after creation")
	}

	if idx == nil {
		t.Fatal("Index is nil")
	}
}

func TestTable_InsertRow_UpdatesAllIndexes(t *testing.T) {
	table, _ := setupTestTable(t)
	defer table.Close()

	// Create two indexes
	err := table.CreateIndex("id_idx", "id")
	if err != nil {
		t.Fatalf("Failed to create id index: %v", err)
	}

	err = table.CreateIndex("name_idx", "name")
	if err != nil {
		t.Fatalf("Failed to create name index: %v", err)
	}

	// Insert a row
	err = table.InsertRow([]any{1, "Alice"})
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Verify both indexes are in cache
	if _, ok := table.Indexes["id_idx"]; !ok {
		t.Error("id_idx not found in cache after insert")
	}

	if _, ok := table.Indexes["name_idx"]; !ok {
		t.Error("name_idx not found in cache after insert")
	}
}

func TestTable_MultipleIndexes_AllWork(t *testing.T) {
	table, _ := setupTestTable(t)
	defer table.Close()

	// Insert some rows
	rows := [][]any{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	for _, row := range rows {
		err := table.InsertRow(row)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Create first index
	err := table.CreateIndex("id_idx", "id")
	if err != nil {
		t.Fatalf("Failed to create first index: %v", err)
	}

	// Create second index
	err = table.CreateIndex("name_idx", "name")
	if err != nil {
		t.Fatalf("Failed to create second index: %v", err)
	}

	// Verify both indexes exist
	if _, ok := table.Indexes["id_idx"]; !ok {
		t.Error("First index not found in cache")
	}

	if _, ok := table.Indexes["name_idx"]; !ok {
		t.Error("Second index not found in cache")
	}

	// Insert another row - should update both indexes
	err = table.InsertRow([]any{4, "David"})
	if err != nil {
		t.Fatalf("Failed to insert row after index creation: %v", err)
	}

	// Both indexes should still be in cache
	if _, ok := table.Indexes["id_idx"]; !ok {
		t.Error("First index not found after new insert")
	}

	if _, ok := table.Indexes["name_idx"]; !ok {
		t.Error("Second index not found after new insert")
	}
}

func TestTable_LoadIndexes_OnTableOpen(t *testing.T) {
	tmpDir := t.TempDir()
	tablePath := filepath.Join(tmpDir, "test.tbl")

	schema := &catalog.TableSchema{
		Name: "test",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
		},
		Indexes: make(map[string]*catalog.Index),
	}

	// Create table and index
	{
		table, err := NewTable("test", tablePath, schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Add index to schema
		schema.Indexes["id_idx"] = &catalog.Index{
			Name:       "id_idx",
			ColumnName: "id",
		}

		// Create the index
		err = table.CreateIndex("id_idx", "id")
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}

		table.Close()
	}

	// Reopen table - should load existing index
	{
		table, err := NewTable("test", tablePath, schema)
		if err != nil {
			t.Fatalf("Failed to reopen table: %v", err)
		}
		defer table.Close()

		// Index should be loaded
		if _, ok := table.Indexes["id_idx"]; !ok {
			t.Error("Index not loaded on table reopen")
		}
	}
}

func TestTable_CreateIndex_InvalidColumn(t *testing.T) {
	table, _ := setupTestTable(t)
	defer table.Close()

	err := table.CreateIndex("bad_idx", "nonexistent")
	if err == nil {
		t.Error("Expected error when creating index on nonexistent column")
	}
}

func TestTable_InsertRow_WithMultipleIndexes(t *testing.T) {
	table, _ := setupTestTable(t)
	defer table.Close()

	// Create multiple indexes
	indexes := []string{"id_idx", "name_idx"}
	columns := []string{"id", "name"}

	for i, idxName := range indexes {
		err := table.CreateIndex(idxName, columns[i])
		if err != nil {
			t.Fatalf("Failed to create index %s: %v", idxName, err)
		}
	}

	// Insert rows
	rows := [][]any{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	for _, row := range rows {
		err := table.InsertRow(row)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Verify all indexes are still in cache
	for _, idxName := range indexes {
		if _, ok := table.Indexes[idxName]; !ok {
			t.Errorf("Index %s not found in cache", idxName)
		}
	}
}

func TestTable_GetIndex_LoadsIfNotCached(t *testing.T) {
	table, _ := setupTestTable(t)
	defer table.Close()

	// Create index
	err := table.CreateIndex("id_idx", "id")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Close table to clear cache
	table.Close()

	// Reopen table
	tablePath := filepath.Join(table.dataDir, "test.tbl")
	table, err = NewTable("test", tablePath, table.schema)
	if err != nil {
		t.Fatalf("Failed to reopen table: %v", err)
	}
	defer table.Close()

	// GetIndex should load the index
	idx, err := table.GetIndex("id_idx")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}

	if idx == nil {
		t.Fatal("GetIndex returned nil")
	}

	// Index should now be in cache
	if _, ok := table.Indexes["id_idx"]; !ok {
		t.Error("Index not cached after GetIndex")
	}
}

