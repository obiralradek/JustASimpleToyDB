package main

import (
	"fmt"
	"log"

	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/storage"
)

func main() {
	fmt.Println("JustASimpleToyDB starting...")

	schema := catalog.NewCatalog("data/catalog.json")

	schema.CreateTable(&catalog.TableSchema{
		Name: "users",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
		},
	})

	table, err := storage.NewTable("test_table", "data/test_table.tbl")
	if err != nil {
		log.Fatalf("new table: %v", err)
	}
	defer table.Close()

	rows := [][]byte{
		[]byte("hello world"),
		[]byte("hello world two"),
		[]byte("different hello world"),
	}

	for i, r := range rows {
		if err := table.InsertRow(r); err != nil {
			log.Fatalf("insert %d: %v", i, err)
		}
		fmt.Printf("Inserted row %d (len=%d)\n", i, len(r))
	}

	all, err := table.ReadAllRows()
	if err != nil {
		log.Fatalf("read all: %v", err)
	}
	fmt.Printf("\nRead %d rows back:\n", len(all))
	for i, r := range all {
		fmt.Printf(" %d: %q\n", i, string(r))
	}
}
