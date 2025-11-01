package executor

import "justasimpletoydb/internal/catalog"

type CreateTableStmt struct {
	Name    string
	Columns []catalog.Column
}

func (s *CreateTableStmt) Execute(ex *Executor) error {
	schema := &catalog.TableSchema{
		Name:    s.Name,
		Columns: s.Columns,
	}
	return ex.engine.CreateTable(schema)
}
