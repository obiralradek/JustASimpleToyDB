package executor

import (
	"encoding/json"
	"justasimpletoydb/internal/engine"
)

type Executor struct {
	engine *engine.Engine
}

func NewExecutor(e *engine.Engine) *Executor {
	return &Executor{engine: e}
}

type ExecResult struct {
	Columns  []string // names of columns (empty for INSERT/CREATE)
	Rows     [][]any  // data rows (empty for non-SELECT)
	Affected int      // number of affected rows (INSERT/UPDATE)
	Message  string   // optional message, e.g., "OK" or error
}

func (r *ExecResult) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

type Statement interface {
	Execute(ex *Executor) (*ExecResult, error)
}
