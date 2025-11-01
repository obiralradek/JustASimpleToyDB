package parser

import (
	"fmt"
	"justasimpletoydb/internal/executor"
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
