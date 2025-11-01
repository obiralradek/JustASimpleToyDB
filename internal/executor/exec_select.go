package executor

import (
	"fmt"
)

type SelectStmt struct {
	Table   string
	Columns []string
}

func (s *SelectStmt) Execute(ex *Executor) (*ExecResult, error) {
	table, err := ex.engine.GetTable(s.Table)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", s.Table)
	}

	rows, err := table.ReadAllRows()
	if err != nil {
		return nil, err
	}
	colIndexes, colNames, err := table.ResolveColumns(s.Columns)

	result := make([][]any, 0, len(rows))
	for _, row := range rows {
		selected := make([]any, len(colIndexes))
		for i, idx := range colIndexes {
			selected[i] = row[idx]
		}
		result = append(result, selected)
	}
	return &ExecResult{
		Columns:  colNames,
		Rows:     result,
		Affected: 0,
		Message:  "OK",
	}, nil
}
