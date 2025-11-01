package executor

import (
	"justasimpletoydb/internal/engine"
)

type Executor struct {
	engine *engine.Engine
}

func NewExecutor(e *engine.Engine) *Executor {
	return &Executor{engine: e}
}

type Statement interface {
	Execute(ex *Executor) error
}
