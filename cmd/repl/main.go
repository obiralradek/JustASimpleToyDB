package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"justasimpletoydb/internal/executor"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:4000")
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	serverReader := bufio.NewReader(conn)

	fmt.Println("Connected to JustASimpleToyDB. Type queries, or 'exit' to quit.")

	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "exit" {
			break
		}

		// Send query to server
		conn.Write([]byte(line + "\n"))

		// Read response
		resp, _ := serverReader.ReadString('\n')
		var result executor.ExecResult
		if err := json.Unmarshal([]byte(resp), &result); err == nil {
			fmt.Printf("Message: %s    Affected: %d\n", result.Message, result.Affected)
			if len(result.Rows) > 0 {
				prettyPrintTable(result.Columns, result.Rows)
			}
		} else {
			fmt.Println(resp)
		}
	}
}

func prettyPrintTable(columns []string, rows [][]any) {
	if len(columns) == 0 {
		fmt.Println("No columns")
		return
	}

	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	for _, row := range rows {
		for i, cell := range row {
			s := fmt.Sprintf("%v", cell)
			if len(s) > colWidths[i] {
				colWidths[i] = len(s)
			}
		}
	}

	// Helper: print separator line
	sep := func() {
		for i, w := range colWidths {
			fmt.Print("+")
			fmt.Print(strings.Repeat("-", w+2))
			if i == len(colWidths)-1 {
				fmt.Println("+")
			}
		}
	}

	sep()
	fmt.Print("|")
	for i, col := range columns {
		fmt.Printf(" %-*s |", colWidths[i], col)
	}
	fmt.Println()
	sep()

	for _, row := range rows {
		fmt.Print("|")
		for i, cell := range row {
			fmt.Printf(" %-*v |", colWidths[i], cell)
		}
		fmt.Println()
	}
	sep()
}
