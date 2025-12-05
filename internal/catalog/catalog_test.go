package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func setupTestCatalog(t *testing.T) (*Catalog, string) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "catalog.json")
	catalog := NewCatalog(catalogPath)
	return catalog, tmpDir
}

func TestNewCatalog_CreatesEmptyCatalog(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	if catalog.Tables == nil {
		t.Fatal("Tables map is nil")
	}

	if len(catalog.Tables) != 0 {
		t.Errorf("Expected empty catalog, got %d tables", len(catalog.Tables))
	}
}

func TestCatalog_CreateTable(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	schema := &TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
			{Name: "name", Type: TypeText},
		},
		Indexes: make(map[string]*Index),
	}

	err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	if _, ok := catalog.Tables["users"]; !ok {
		t.Error("Table not found in catalog")
	}

	// Verify table schema
	retrieved, err := catalog.GetTable("users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if retrieved.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", retrieved.Name)
	}

	if len(retrieved.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(retrieved.Columns))
	}
}

func TestCatalog_CreateTable_Duplicate(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	schema := &TableSchema{
		Name:    "users",
		Columns: []Column{},
		Indexes: make(map[string]*Index),
	}

	err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to create duplicate
	err = catalog.CreateTable(schema)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

func TestCatalog_GetTable_NotFound(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	_, err := catalog.GetTable("nonexistent")
	if err == nil {
		t.Error("Expected error when getting nonexistent table")
	}
}

func TestCatalog_CreateIndex(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	// Create table first
	schema := &TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
			{Name: "name", Type: TypeText},
		},
		Indexes: make(map[string]*Index),
	}

	err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	err = catalog.CreateIndex("users", "id_idx", "id")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists in schema
	table, err := catalog.GetTable("users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if _, ok := table.Indexes["id_idx"]; !ok {
		t.Error("Index not found in table schema")
	}

	index := table.Indexes["id_idx"]
	if index.Name != "id_idx" {
		t.Errorf("Expected index name 'id_idx', got '%s'", index.Name)
	}

	if index.ColumnName != "id" {
		t.Errorf("Expected column name 'id', got '%s'", index.ColumnName)
	}
}

func TestCatalog_CreateIndex_MultipleIndexes(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	// Create table
	schema := &TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
			{Name: "name", Type: TypeText},
			{Name: "email", Type: TypeText},
		},
		Indexes: make(map[string]*Index),
	}

	err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create multiple indexes
	indexes := []struct {
		name   string
		column string
	}{
		{"id_idx", "id"},
		{"name_idx", "name"},
		{"email_idx", "email"},
	}

	for _, idx := range indexes {
		err := catalog.CreateIndex("users", idx.name, idx.column)
		if err != nil {
			t.Fatalf("Failed to create index %s: %v", idx.name, err)
		}
	}

	// Verify all indexes exist
	table, err := catalog.GetTable("users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if len(table.Indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(table.Indexes))
	}

	for _, idx := range indexes {
		if _, ok := table.Indexes[idx.name]; !ok {
			t.Errorf("Index %s not found", idx.name)
		}
	}
}

func TestCatalog_CreateIndex_Duplicate(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	// Create table
	schema := &TableSchema{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
		},
		Indexes: make(map[string]*Index),
	}

	err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	err = catalog.CreateIndex("users", "id_idx", "id")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Try to create duplicate
	err = catalog.CreateIndex("users", "id_idx", "id")
	if err == nil {
		t.Error("Expected error when creating duplicate index")
	}
}

func TestCatalog_CreateIndex_TableNotFound(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	err := catalog.CreateIndex("nonexistent", "idx", "col")
	if err == nil {
		t.Error("Expected error when creating index on nonexistent table")
	}
}

func TestCatalog_ListTables(t *testing.T) {
	catalog, _ := setupTestCatalog(t)

	// Create multiple tables
	tables := []string{"users", "posts", "comments"}
	for _, name := range tables {
		schema := &TableSchema{
			Name:    name,
			Columns: []Column{},
			Indexes: make(map[string]*Index),
		}
		err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table %s: %v", name, err)
		}
	}

	// List tables
	list := catalog.ListTables()
	if len(list) != len(tables) {
		t.Errorf("Expected %d tables, got %d", len(tables), len(list))
	}

	// Verify all tables are in list
	for _, name := range tables {
		found := false
		for _, listed := range list {
			if listed == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Table %s not found in list", name)
		}
	}
}

func TestCatalog_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "catalog.json")

	// Create catalog and add data
	{
		catalog := NewCatalog(catalogPath)

		schema := &TableSchema{
			Name: "users",
			Columns: []Column{
				{Name: "id", Type: TypeInt},
				{Name: "name", Type: TypeText},
			},
			Indexes: make(map[string]*Index),
		}

		err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		err = catalog.CreateIndex("users", "id_idx", "id")
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}
	}

	// Reload catalog
	{
		catalog := NewCatalog(catalogPath)

		// Verify table exists
		table, err := catalog.GetTable("users")
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		if table.Name != "users" {
			t.Errorf("Expected table name 'users', got '%s'", table.Name)
		}

		if len(table.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(table.Columns))
		}

		// Verify index exists
		if _, ok := table.Indexes["id_idx"]; !ok {
			t.Error("Index not found after reload")
		}
	}
}

func TestCatalog_Persistence_MultipleTablesAndIndexes(t *testing.T) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "catalog.json")

	// Create catalog with multiple tables and indexes
	{
		catalog := NewCatalog(catalogPath)

		// Create first table
		schema1 := &TableSchema{
			Name: "users",
			Columns: []Column{
				{Name: "id", Type: TypeInt},
				{Name: "name", Type: TypeText},
			},
			Indexes: make(map[string]*Index),
		}
		catalog.CreateTable(schema1)
		catalog.CreateIndex("users", "id_idx", "id")
		catalog.CreateIndex("users", "name_idx", "name")

		// Create second table
		schema2 := &TableSchema{
			Name: "posts",
			Columns: []Column{
				{Name: "id", Type: TypeInt},
				{Name: "title", Type: TypeText},
			},
			Indexes: make(map[string]*Index),
		}
		catalog.CreateTable(schema2)
		catalog.CreateIndex("posts", "id_idx", "id")
	}

	// Reload catalog
	{
		catalog := NewCatalog(catalogPath)

		// Verify users table
		users, err := catalog.GetTable("users")
		if err != nil {
			t.Fatalf("Failed to get users table: %v", err)
		}

		if len(users.Indexes) != 2 {
			t.Errorf("Expected 2 indexes on users, got %d", len(users.Indexes))
		}

		// Verify posts table
		posts, err := catalog.GetTable("posts")
		if err != nil {
			t.Fatalf("Failed to get posts table: %v", err)
		}

		if len(posts.Indexes) != 1 {
			t.Errorf("Expected 1 index on posts, got %d", len(posts.Indexes))
		}
	}
}

func TestCatalog_Load_NonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "nonexistent.json")

	// Should not panic when file doesn't exist
	catalog := NewCatalog(catalogPath)

	if catalog.Tables == nil {
		t.Fatal("Tables map should be initialized")
	}

	if len(catalog.Tables) != 0 {
		t.Errorf("Expected empty catalog, got %d tables", len(catalog.Tables))
	}
}

func TestCatalog_Save_Load_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "catalog.json")

	catalog := NewCatalog(catalogPath)

	// Create table with index
	schema := &TableSchema{
		Name: "test",
		Columns: []Column{
			{Name: "id", Type: TypeInt},
		},
		Indexes: make(map[string]*Index),
	}

	err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = catalog.CreateIndex("test", "idx", "id")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		t.Fatal("Catalog file was not created")
	}

	// Create new catalog instance to test loading
	catalog2 := NewCatalog(catalogPath)

	table, err := catalog2.GetTable("test")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if len(table.Indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(table.Indexes))
	}
}

