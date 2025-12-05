package parser

import (
	"fmt"
	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/executor"
	"strings"
)

func (p *Parser) ParseCreate() (executor.Statement, error) {
	if err := p.expect(KEYWORD, "CREATE"); err != nil {
		return nil, err
	}

	next := p.eat()
	if next.Type != KEYWORD {
		return nil, fmt.Errorf("expected keyword after CREATE, got %v", next)
	}

	switch strings.ToUpper(next.Literal) {
	case "TABLE":
		return p.parseCreateTable()
	case "INDEX":
		return p.parseCreateIndex()
	default:
		return nil, fmt.Errorf("unexpected CREATE target: %s", next.Literal)
	}
}

// internal helper for table
func (p *Parser) parseCreateTable() (*executor.CreateTableStmt, error) {
	nameTok := p.eat()
	if nameTok.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	name := nameTok.Literal

	if err := p.expect(SYMBOL, "("); err != nil {
		return nil, err
	}

	cols := []catalog.Column{}
	for {
		colNameTok := p.eat()
		if colNameTok.Type != IDENT {
			return nil, fmt.Errorf("expected column name")
		}
		typeTok := p.eat()
		if typeTok.Type != KEYWORD {
			return nil, fmt.Errorf("expected column type")
		}

		typ := catalog.TypeText
		if strings.ToUpper(typeTok.Literal) == "INT" {
			typ = catalog.TypeInt
		}

		cols = append(cols, catalog.Column{Name: colNameTok.Literal, Type: typ})

		cur := p.cur()
		if cur.Type == SYMBOL && cur.Literal == ")" {
			p.eat()
			break
		} else if cur.Type == SYMBOL && cur.Literal == "," {
			p.eat()
		} else {
			return nil, fmt.Errorf("unexpected token in column list: %v", cur)
		}
	}

	if cur := p.cur(); cur.Type == SYMBOL && cur.Literal == ";" {
		p.eat()
	}

	return &executor.CreateTableStmt{Name: name, Columns: cols}, nil
}

// internal helper for index
func (p *Parser) parseCreateIndex() (*executor.CreateIndexStmt, error) {
	idxTok := p.eat()
	if idxTok.Type != IDENT {
		return nil, fmt.Errorf("expected index name")
	}
	indexName := idxTok.Literal

	if err := p.expect(KEYWORD, "ON"); err != nil {
		return nil, err
	}

	tableTok := p.eat()
	if tableTok.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	tableName := tableTok.Literal

	if err := p.expect(SYMBOL, "("); err != nil {
		return nil, err
	}

	colTok := p.eat()
	if colTok.Type != IDENT {
		return nil, fmt.Errorf("expected column name")
	}
	column := colTok.Literal

	if err := p.expect(SYMBOL, ")"); err != nil {
		return nil, err
	}

	if cur := p.cur(); cur.Type == SYMBOL && cur.Literal == ";" {
		p.eat()
	}

	return &executor.CreateIndexStmt{
		Name:      indexName,
		TableName: tableName,
		Column:    column,
	}, nil
}
