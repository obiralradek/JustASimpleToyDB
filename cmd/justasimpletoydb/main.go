package main

import (
	"fmt"

	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/engine"
	"justasimpletoydb/internal/executor"
)

func main() {
	fmt.Println("Starting JustASimpleToyDB...")

	e := engine.NewEngine("data")
	exec := executor.NewExecutor(e)

	// CREATE TABLE
	create := &executor.CreateTableStmt{
		Name: "users",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
		},
	}
	_ = create.Execute(exec)

	// INSERT rows
	_ = (&executor.InsertStmt{Table: "users", Values: []any{1, "Alice"}}).Execute(exec)
	_ = (&executor.InsertStmt{Table: "users", Values: []any{2, "Bob"}}).Execute(exec)
	_ = (&executor.InsertStmt{Table: "users", Values: []any{3, "Radek"}}).Execute(exec)

	// SELECT *
	_ = (&executor.SelectStmt{Table: "users"}).Execute(exec)

	fmt.Println("Done.")
}
