package main

import (
	"bufio"
	"fmt"
	"justasimpletoydb/internal/engine"
	"justasimpletoydb/internal/executor"
	"justasimpletoydb/internal/processor"
	"log"
	"net"
	"strings"
)

func handleConnection(conn net.Conn, qp *processor.QueryProcessor) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Println("client disconnected")
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "exit" {
			return
		}

		err = qp.RunQuery(line)
		if err != nil {
			writer.WriteString(fmt.Sprintf("parse error: %v\n", err))
			writer.Flush()
			continue
		} else {
			writer.WriteString("OK\n")
		}
		writer.Flush()
	}
}

func main() {
	fmt.Println("Starting JustASimpleToyDB server on :4000...")
	ln, err := net.Listen("tcp", ":4000")
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	defer ln.Close()

	e := engine.NewEngine("data")
	exec := executor.NewExecutor(e)
	qp := processor.QueryProcessor{Exec: exec}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("failed to accept connection:", err)
			continue
		}
		go handleConnection(conn, &qp)
	}
}
