package parser

import (
	"fmt"
	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/executor"
	"regexp"
	"strconv"
	"strings"
)

// Very simple naive parsing only supports: CREATE TABLE, INSERT, SELECT *
func Parse(query string) (executor.Statement, error) {
	query = strings.TrimSpace(query)
	queryUpper := strings.ToUpper(query)

	if strings.HasPrefix(queryUpper, "CREATE TABLE") {
		re := regexp.MustCompile(`(?i)CREATE TABLE (\w+) \((.+)\);?`)
		m := re.FindStringSubmatch(query)
		if len(m) != 3 {
			return nil, fmt.Errorf("invalid CREATE TABLE syntax")
		}
		name := m[1]
		colsStr := m[2]
		colDefs := []catalog.Column{}
		for _, col := range strings.Split(colsStr, ",") {
			parts := strings.Fields(strings.TrimSpace(col))
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid column definition: %s", col)
			}
			typ := catalog.TypeText
			if strings.ToUpper(parts[1]) == "INT" {
				typ = catalog.TypeInt
			}
			colDefs = append(colDefs, catalog.Column{Name: parts[0], Type: typ})
		}
		return &executor.CreateTableStmt{Name: name, Columns: colDefs}, nil
	}

	if strings.HasPrefix(queryUpper, "INSERT INTO") {
		re := regexp.MustCompile(`(?i)INSERT INTO (\w+) VALUES \((.+)\);?`)
		m := re.FindStringSubmatch(query)
		if len(m) != 3 {
			return nil, fmt.Errorf("invalid INSERT syntax")
		}
		table := m[1]
		valuesStr := m[2]
		values := []any{}
		for _, v := range strings.Split(valuesStr, ",") {
			v = strings.TrimSpace(v)
			if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
				values = append(values, v[1:len(v)-1])
			} else {
				iv, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("invalid int value: %s", v)
				}
				values = append(values, iv)
			}
		}
		return &executor.InsertStmt{Table: table, Values: values}, nil
	}

	if strings.HasPrefix(queryUpper, "SELECT") {
		re := regexp.MustCompile(`(?i)SELECT \* FROM (\w+);?`)
		m := re.FindStringSubmatch(query)
		if len(m) != 2 {
			return nil, fmt.Errorf("invalid SELECT syntax")
		}
		return &executor.SelectStmt{Table: m[1]}, nil
	}

	return nil, fmt.Errorf("unsupported query: %s", query)
}
