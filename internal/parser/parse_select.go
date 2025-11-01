package parser

import (
	"fmt"
	"justasimpletoydb/internal/executor"
)

func (p *Parser) ParseSelect() (*executor.SelectStmt, error) {
	if err := p.expect(KEYWORD, "SELECT"); err != nil {
		return nil, err
	}

	// Handle '*' or explicit column list
	var columns []string
	cur := p.cur()

	if cur.Type == SYMBOL && cur.Literal == "*" {
		p.eat()
		columns = []string{"*"}
	} else if cur.Type == IDENT {
		// parse list of identifiers separated by commas
		for {
			tok := p.eat()
			if tok.Type != IDENT {
				return nil, fmt.Errorf("expected column name, got %s '%s'", tok.Type, tok.Literal)
			}
			columns = append(columns, tok.Literal)

			if p.cur().Type == SYMBOL && p.cur().Literal == "," {
				p.eat() // consume comma
				continue
			}
			break
		}
	} else {
		return nil, fmt.Errorf("expected '*' or column name, got %s '%s'", cur.Type, cur.Literal)
	}

	// Expect FROM
	if err := p.expect(KEYWORD, "FROM"); err != nil {
		return nil, err
	}

	// Expect table name
	tableTok := p.eat()
	if tableTok.Type != IDENT {
		return nil, fmt.Errorf("expected table name, got %s '%s'", tableTok.Type, tableTok.Literal)
	}

	// Optional semicolon
	if cur := p.cur(); cur.Type == SYMBOL && cur.Literal == ";" {
		p.eat()
	}

	cond, err := p.parseWhere()
	if err != nil {
		return nil, err
	}

	return &executor.SelectStmt{
		Table:   tableTok.Literal,
		Columns: columns,
		Where:   cond,
	}, nil
}
