package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"justasimpletoydb/internal/engine"
	"justasimpletoydb/internal/executor"
	"justasimpletoydb/internal/processor"
)

func main() {
	fmt.Println("JustASimpleToyDB starting... (type 'exit' to quit)")

	e := engine.NewEngine("data")
	exec := executor.NewExecutor(e)
	qp := processor.QueryProcessor{Exec: exec}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("db> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "exit") || strings.EqualFold(line, "quit") {
			fmt.Println("Bye!")
			break
		}

		if err := qp.RunQuery(line); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}
