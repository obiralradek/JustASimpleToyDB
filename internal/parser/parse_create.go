package parser

import (
	"fmt"
	"justasimpletoydb/internal/catalog"
	"justasimpletoydb/internal/executor"
	"strings"
)

func (p *Parser) ParseCreateTable() (*executor.CreateTableStmt, error) {
	if err := p.expect(KEYWORD, "CREATE"); err != nil {
		return nil, err
	}
	if err := p.expect(KEYWORD, "TABLE"); err != nil {
		return nil, err
	}

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
