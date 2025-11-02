package executor

import (
	"fmt"
)

type CreateIndexStmt struct {
	Name      string
	TableName string
	Column    string
}

func (s *CreateIndexStmt) Execute(ex *Executor) (*ExecResult, error) {
	// Check table exists
	table, err := ex.engine.GetTable(s.TableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", s.TableName)
	}

	// Create index on the table
	if err := table.CreateIndex(s.Name, s.Column); err != nil {
		return nil, fmt.Errorf("failed to create index: %v", err)
	}

	return &ExecResult{
		Message:  fmt.Sprintf("Index %s created on table %s(column %s)\n", s.Name, s.TableName, s.Column),
		Affected: 0,
	}, nil
}
