package executor

import (
	"fmt"
	"strings"
)

type SelectStmt struct {
	Table   string
	Columns []string
}

func (s *SelectStmt) Execute(ex *Executor) error {
	table, err := ex.engine.GetTable(s.Table)
	if err != nil {
		return fmt.Errorf("table not found: %s", s.Table)
	}

	rows, err := table.ReadAllRows()
	if err != nil {
		return err
	}
	colIndexes, colNames, err := table.ResolveColumns(s.Columns)

	fmt.Printf("Results from %s:\n", s.Table)
	fmt.Println(strings.Join(colNames, " | "))

	for _, row := range rows {
		r := row.([]any)
		selected := make([]any, len(colIndexes))
		for i, idx := range colIndexes {
			if idx < len(r) {
				selected[i] = r[idx]
			}
		}
		fmt.Println(selected)
	}

	return nil
}
