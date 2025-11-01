package processor

import (
	"justasimpletoydb/internal/executor"
	"justasimpletoydb/internal/parser"
)

type QueryProcessor struct {
	Exec *executor.Executor
}

func (qp *QueryProcessor) RunQuery(sql string) (*executor.ExecResult, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return stmt.Execute(qp.Exec)
}
