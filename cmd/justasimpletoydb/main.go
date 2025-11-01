package main

import (
	"fmt"

	"justasimpletoydb/internal/engine"
	"justasimpletoydb/internal/executor"
	"justasimpletoydb/internal/processor"
)

func main() {
	fmt.Println("Starting JustASimpleToyDB...")

	e := engine.NewEngine("data")
	exec := executor.NewExecutor(e)
	processor := processor.QueryProcessor{
		Exec: exec,
	}

	_ = processor.RunQuery("CREATE TABLE users (id INT, name TEXT, surname TEXT);")
	_ = processor.RunQuery("INSERT INTO users VALUES (1, 'Alice', 'Surname');")
	_ = processor.RunQuery("INSERT INTO users VALUES (2, 'Bob', 'Surname');")
	_ = processor.RunQuery("INSERT INTO users VALUES (3, 'Radek', 'Surname');")
	_ = processor.RunQuery("SELECT * FROM users;")

	fmt.Println("Done.")
}
