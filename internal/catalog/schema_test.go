package catalog

import (
	"testing"
)

func TestTableSchema_Initialization(t *testing.T) {
	schema := &TableSchema{
		Name:    "test",
		Columns: []Column{},
		Indexes: make(map[string]*Index),
	}

	if schema.Name != "test" {
		t.Errorf("Expected name 'test', got '%s'", schema.Name)
	}

	if schema.Columns == nil {
		t.Error("Columns slice is nil")
	}

	if schema.Indexes == nil {
		t.Error("Indexes map is nil")
	}
}

func TestTableSchema_WithColumns(t *testing.T) {
	schema := &TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
			{Name: "name", Type: TypeText},
			{Name: "email", Type: TypeText},
		},
		Indexes: make(map[string]*Index),
	}

	if len(schema.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(schema.Columns))
	}

	// Verify column types
	if schema.Columns[0].Type != TypeInt {
		t.Error("First column should be TypeInt")
	}

	if schema.Columns[1].Type != TypeText {
		t.Error("Second column should be TypeText")
	}

	if schema.Columns[2].Type != TypeText {
		t.Error("Third column should be TypeText")
	}
}

func TestTableSchema_WithIndexes(t *testing.T) {
	schema := &TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
			{Name: "name", Type: TypeText},
		},
		Indexes: make(map[string]*Index),
	}

	// Add indexes
	schema.Indexes["id_idx"] = &Index{
		Name:       "id_idx",
		ColumnName: "id",
	}

	schema.Indexes["name_idx"] = &Index{
		Name:       "name_idx",
		ColumnName: "name",
	}

	if len(schema.Indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(schema.Indexes))
	}

	// Verify index details
	idIdx, ok := schema.Indexes["id_idx"]
	if !ok {
		t.Fatal("id_idx not found")
	}

	if idIdx.Name != "id_idx" {
		t.Errorf("Expected index name 'id_idx', got '%s'", idIdx.Name)
	}

	if idIdx.ColumnName != "id" {
		t.Errorf("Expected column name 'id', got '%s'", idIdx.ColumnName)
	}

	nameIdx, ok := schema.Indexes["name_idx"]
	if !ok {
		t.Fatal("name_idx not found")
	}

	if nameIdx.ColumnName != "name" {
		t.Errorf("Expected column name 'name', got '%s'", nameIdx.ColumnName)
	}
}

func TestIndex_Initialization(t *testing.T) {
	idx := &Index{
		Name:       "test_idx",
		ColumnName: "test_col",
	}

	if idx.Name != "test_idx" {
		t.Errorf("Expected name 'test_idx', got '%s'", idx.Name)
	}

	if idx.ColumnName != "test_col" {
		t.Errorf("Expected column name 'test_col', got '%s'", idx.ColumnName)
	}
}

func TestColumn_Initialization(t *testing.T) {
	col := Column{
		Name: "id",
		Type: TypeInt,
	}

	if col.Name != "id" {
		t.Errorf("Expected name 'id', got '%s'", col.Name)
	}

	if col.Type != TypeInt {
		t.Errorf("Expected type TypeInt, got %d", col.Type)
	}
}

func TestColumnType_Constants(t *testing.T) {
	if TypeInt != 0 {
		t.Errorf("Expected TypeInt to be 0, got %d", TypeInt)
	}

	if TypeText != 1 {
		t.Errorf("Expected TypeText to be 1, got %d", TypeText)
	}
}

func TestTableSchema_MultipleIndexesOnSameColumn(t *testing.T) {
	schema := &TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
		},
		Indexes: make(map[string]*Index),
	}

	// Create multiple indexes on the same column
	schema.Indexes["idx1"] = &Index{
		Name:       "idx1",
		ColumnName: "id",
	}

	schema.Indexes["idx2"] = &Index{
		Name:       "idx2",
		ColumnName: "id",
	}

	schema.Indexes["idx3"] = &Index{
		Name:       "idx3",
		ColumnName: "id",
	}

	if len(schema.Indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(schema.Indexes))
	}

	// All should reference the same column
	for name, idx := range schema.Indexes {
		if idx.ColumnName != "id" {
			t.Errorf("Index %s should reference column 'id', got '%s'", name, idx.ColumnName)
		}
	}
}

func TestTableSchema_EmptyIndexes(t *testing.T) {
	schema := &TableSchema{
		Name:    "test",
		Columns: []Column{{Name: "id", Type: TypeInt}},
		Indexes: make(map[string]*Index),
	}

	if len(schema.Indexes) != 0 {
		t.Errorf("Expected 0 indexes, got %d", len(schema.Indexes))
	}

	// Should be able to add index
	schema.Indexes["idx"] = &Index{
		Name:       "idx",
		ColumnName: "id",
	}

	if len(schema.Indexes) != 1 {
		t.Errorf("Expected 1 index after adding, got %d", len(schema.Indexes))
	}
}

func TestTableSchema_EmptyColumns(t *testing.T) {
	schema := &TableSchema{
		Name:    "test",
		Columns: []Column{},
		Indexes: make(map[string]*Index),
	}

	if len(schema.Columns) != 0 {
		t.Errorf("Expected 0 columns, got %d", len(schema.Columns))
	}
}

