package parser

import (
	"fmt"
	"justasimpletoydb/internal/executor"
	"strconv"
	"strings"
)

type Parser struct {
	tokens []Token
	pos    int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

func Parse(sql string) (executor.Statement, error) {
	tokens, err := Tokenize(sql)
	if err != nil {
		return nil, err
	}

	p := NewParser(tokens)
	first := strings.ToUpper(p.cur().Literal)

	switch first {
	case "CREATE":
		return p.ParseCreateTable()
	case "INSERT":
		return p.ParseInsert()
	case "SELECT":
		return p.ParseSelect()
	default:
		return nil, fmt.Errorf("unsupported statement: %s", first)
	}
}

func (p *Parser) cur() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: EOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) eat() Token {
	t := p.cur()
	p.pos++
	return t
}

func (p *Parser) expect(t TokenType, lit string) error {
	cur := p.cur()
	if cur.Type != t || (lit != "" && strings.ToUpper(cur.Literal) != lit) {
		return fmt.Errorf("expected %s '%s', got %s '%s'", t.String(), lit, cur.Type.String(), cur.Literal)
	}
	p.eat()
	return nil
}

func (p *Parser) parseWhere() (*executor.Condition, error) {
	if err := p.expect(KEYWORD, "WHERE"); err != nil {
		return nil, nil
	}

	colTok := p.eat()
	if colTok.Type != IDENT {
		return nil, fmt.Errorf("expected column name in WHERE")
	}

	opTok := p.eat()
	if opTok.Type != SYMBOL || opTok.Literal != "=" {
		return nil, fmt.Errorf("only '=' operator supported for now")
	}

	valTok := p.eat()
	if valTok.Type != INT && valTok.Type != STRING {
		return nil, fmt.Errorf("expected literal value in WHERE")
	}

	var val any
	if valTok.Type == INT {
		int, err := strconv.ParseInt(valTok.Literal, 6, 12)
		if err != nil {
			return nil, fmt.Errorf("can't convert value to number")
		}
		val = int
	} else {
		val = valTok.Literal
	}

	return &executor.Condition{
		Column:   colTok.Literal,
		Operator: "=",
		Value:    val,
	}, nil
}
