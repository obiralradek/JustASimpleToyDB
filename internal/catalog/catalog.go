package catalog

import (
	"encoding/json"
	"fmt"
	"os"
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

func (c *Catalog) save() {
	data, _ := json.MarshalIndent(c.Tables, "", "  ")
	_ = os.WriteFile(c.path, data, 0644)
}

func (c *Catalog) CreateTable(schema *TableSchema) {
	if _, exists := c.Tables[schema.Name]; exists {
		panic(fmt.Sprintf("table %s already exists", schema.Name))
	}
	c.Tables[schema.Name] = schema
	c.save()
}

func (c *Catalog) GetTable(name string) *TableSchema {
	return c.Tables[name]
}
