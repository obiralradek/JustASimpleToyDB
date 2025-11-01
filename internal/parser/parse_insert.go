package parser

import (
	"fmt"
	"justasimpletoydb/internal/executor"
)

func (p *Parser) ParseInsert() (*executor.InsertStmt, error) {
	if err := p.expect(KEYWORD, "INSERT"); err != nil {
		return nil, err
	}
	if err := p.expect(KEYWORD, "INTO"); err != nil {
		return nil, err
	}

	tableTok := p.eat()
	if tableTok.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	if err := p.expect(KEYWORD, "VALUES"); err != nil {
		return nil, err
	}
	if err := p.expect(SYMBOL, "("); err != nil {
		return nil, err
	}

	vals := []any{}
	for {
		tok := p.eat()
		switch tok.Type {
		case INT:
			// convert to int
			var v int
			fmt.Sscanf(tok.Literal, "%d", &v)
			vals = append(vals, v)
		case STRING:
			vals = append(vals, tok.Literal)
		default:
			return nil, fmt.Errorf("unexpected token in VALUES: %v", tok)
		}

		cur := p.cur()
		if cur.Type == SYMBOL && cur.Literal == ")" {
			p.eat()
			break
		} else if cur.Type == SYMBOL && cur.Literal == "," {
			p.eat()
		} else {
			return nil, fmt.Errorf("unexpected token in VALUES list: %v", cur)
		}
	}

	if cur := p.cur(); cur.Type == SYMBOL && cur.Literal == ";" {
		p.eat()
	}

	return &executor.InsertStmt{Table: tableTok.Literal, Values: vals}, nil
}
