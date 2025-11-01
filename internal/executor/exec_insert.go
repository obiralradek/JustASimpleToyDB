package executor

import "fmt"

type InsertStmt struct {
	Table  string
	Values []any
}

func (s *InsertStmt) Execute(ex *Executor) (*ExecResult, error) {
	table, err := ex.engine.GetTable(s.Table)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", s.Table)
	}
	err = table.InsertRow(s.Values)
	if err != nil {
		return nil, err
	}
	return &ExecResult{
		Message:  "OK",
		Affected: 1,
	}, nil
}
