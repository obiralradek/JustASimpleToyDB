package executor

import "fmt"

type InsertStmt struct {
	Table  string
	Values []any
}

func (s *InsertStmt) Execute(ex *Executor) error {
	table, err := ex.engine.GetTable(s.Table)
	if err != nil {
		return fmt.Errorf("table not found: %s", s.Table)
	}
	return table.InsertRow(s.Values)
}
