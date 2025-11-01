package parser

import (
	"fmt"
	"justasimpletoydb/internal/executor"
)

func (p *Parser) ParseSelect() (*executor.SelectStmt, error) {
	if err := p.expect(KEYWORD, "SELECT"); err != nil {
		return nil, err
	}
	if err := p.expect(SYMBOL, "*"); err != nil {
		return nil, err
	}
	if err := p.expect(KEYWORD, "FROM"); err != nil {
		return nil, err
	}

	tableTok := p.eat()
	if tableTok.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	if cur := p.cur(); cur.Type == SYMBOL && cur.Literal == ";" {
		p.eat()
	}

	return &executor.SelectStmt{Table: tableTok.Literal}, nil
}
