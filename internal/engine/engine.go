package engine

import (
	"fmt"
	"path/filepath"

	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/storage"
)

type Engine struct {
	DataDir string
	Catalog *catalog.Catalog
}

func NewEngine(dataDir string) *Engine {
	catPath := filepath.Join(dataDir, "catalog.json")
	return &Engine{
		DataDir: dataDir,
		Catalog: catalog.NewCatalog(catPath),
	}
}

func (e *Engine) CreateTable(schema *catalog.TableSchema) error {
	if err := e.Catalog.CreateTable(schema); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	tablePath := filepath.Join(e.DataDir, schema.Name+".tbl")
	table, err := storage.NewTable(schema.Name, tablePath, schema)
	if err != nil {
		return fmt.Errorf("create table file: %w", err)
	}
	defer table.Close()
	return nil
}

func (e *Engine) CreateIndex(tableName, columnName, indexName string) error {
	if err := e.Catalog.CreateIndex(tableName, indexName, columnName); err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	table, err := e.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("get table for index creation: %w", err)
	}
	if err := table.CreateIndex(indexName, columnName); err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	return nil
}

func (e *Engine) GetTable(name string) (*storage.Table, error) {
	schema, err := e.Catalog.GetTable(name)
	if err != nil {
		return nil, fmt.Errorf("table %s not found in catalog: %w", name, err)
	}

	tablePath := filepath.Join(e.DataDir, name+".tbl")

	table, err := storage.NewTable(name, tablePath, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %w", name, err)
	}

	return table, nil
}
