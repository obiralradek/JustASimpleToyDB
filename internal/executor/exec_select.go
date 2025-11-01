package executor

import (
	"fmt"
)

type Condition struct {
	Column   string
	Operator string
	Value    any
}

type SelectStmt struct {
	Table   string
	Columns []string
	Where   *Condition // nil if no WHERE
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

		// Handle conditions
		include := true
		if s.Where != nil {
			idx, err := table.ResolveColumn(s.Where.Column)
			if err != nil {
				return nil, err
			}
			include = row[idx] == s.Where.Value
		}
		if !include {
			continue
		}

		// Handle selecting specific columns
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
