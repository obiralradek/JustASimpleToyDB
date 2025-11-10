package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Catalog struct {
	path   string
	Tables map[string]*TableSchema
}

func NewCatalog(path string) *Catalog {
	c := &Catalog{
		path:   path,
		Tables: make(map[string]*TableSchema),
	}
	c.load()
	return c
}

func (c *Catalog) load() {
	data, err := os.ReadFile(c.path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		panic(fmt.Sprintf("failed to read catalog: %v", err))
	}
	_ = json.Unmarshal(data, &c.Tables)
}

func (c *Catalog) save() error {
	data, err := json.MarshalIndent(c.Tables, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(c.path)
	_ = os.MkdirAll(dir, 0755)
	return os.WriteFile(c.path, data, 0644)
}

func (c *Catalog) CreateTable(schema *TableSchema) error {
	if _, exists := c.Tables[schema.Name]; exists {
		return fmt.Errorf("table %s already exists", schema.Name)
	}
	c.Tables[schema.Name] = schema
	return c.save()
}

func (c *Catalog) GetTable(name string) (*TableSchema, error) {
	schema, ok := c.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return schema, nil
}

func (c *Catalog) CreateIndex(tableName string, indexName string, indexColumn string) error {
	schema, ok := c.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %s not found", tableName)
	}
	if _, exists := schema.Indexes[indexName]; exists {
		return fmt.Errorf("index %s already exists on table %s", indexName, tableName)
	}
	schema.Indexes[indexName] = &Index{
		Name:       indexName,
		ColumnName: indexColumn,
	}
	c.save()
	return nil
}

func (c *Catalog) ListTables() []string {
	names := make([]string, 0, len(c.Tables))
	for name := range c.Tables {
		names = append(names, name)
	}
	return names
}
