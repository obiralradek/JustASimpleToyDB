package main

import (
	"fmt"
	"log"

	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/engine"
)

func main() {
	fmt.Println("JustASimpleToyDB starting...")

	db := engine.NewEngine("data")

	usersSchema := &catalog.TableSchema{
		Name: "users",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
		},
	}

	if err := db.CreateTable(usersSchema); err != nil {
		log.Printf("Create table: %v (might already exist)", err)
	}

	rows := [][]any{
		{1, "alice"},
		{2, "bob"},
		{3, "radek"},
	}

	for i, r := range rows {
		if err := db.InsertRow("users", r); err != nil {
			log.Fatalf("insert %d: %v", i, err)
		}
		fmt.Printf("Inserted row %d (len=%d)\n", i, len(r))
	}

	all, err := db.SelectAll("users")
	if err != nil {
		log.Fatalf("read all: %v", err)
	}

	fmt.Printf("\nRead %d rows back:\n", len(all))
	for _, r := range all {
		fmt.Printf("Decoded row: %+v\n", r)
	}
}
