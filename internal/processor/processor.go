package processor

import (
	"justasimpletoydb/internal/executor"
	"justasimpletoydb/internal/parser"
)

type QueryProcessor struct {
	Exec *executor.Executor
}

func (qp *QueryProcessor) RunQuery(sql string) error {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return err
	}
	return stmt.Execute(qp.Exec)
}
