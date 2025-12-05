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
	err := ex.engine.CreateIndex(s.TableName, s.Column, s.Name)
	if err != nil {
		return nil, fmt.Errorf("create index: %w", err)
	}

	return &ExecResult{
		Message:  fmt.Sprintf("Index %s created on table %s(column %s)\n", s.Name, s.TableName, s.Column),
		Affected: 0,
	}, nil
}
