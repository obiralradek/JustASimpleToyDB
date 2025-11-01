package executor

import "fmt"

type SelectStmt struct {
	Table string
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

	fmt.Printf("Results from %s:\n", s.Table)
	for _, row := range rows {
		fmt.Println(row)
	}
	return nil
}
