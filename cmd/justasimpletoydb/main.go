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

	test_table_schema := catalog.TableSchema{
		Name: "users",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt},
			{Name: "name", Type: catalog.TypeText},
		},
	}

	schema.CreateTable(&test_table_schema)

	table, err := storage.NewTable("test_table", "data/test_table.tbl", &test_table_schema)
	if err != nil {
		log.Fatalf("new table: %v", err)
	}
	defer table.Close()

	rows := [][]any{
		[]any{1, "hello"},
		[]any{2, "world"},
		[]any{3, "test"},
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
	for _, r := range all {
		fmt.Printf("Decoded row: %+v\n", r)
	}
}
