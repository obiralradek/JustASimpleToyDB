package executor

import "justasimpletoydb/internal/catalog"

type CreateTableStmt struct {
	Name    string
	Columns []catalog.Column
}

func (s *CreateTableStmt) Execute(ex *Executor) (*ExecResult, error) {
	schema := &catalog.TableSchema{
		Name:    s.Name,
		Columns: s.Columns,
	}
	err := ex.engine.CreateTable(schema)
	if err != nil {
		return nil, err
	}
	return &ExecResult{
		Message: "OK",
	}, nil
}
