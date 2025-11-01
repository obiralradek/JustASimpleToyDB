package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int

const (
	ILLEGAL TokenType = iota
	EOF
	IDENT   // table, column names
	INT     // integer literals
	STRING  // string literals
	KEYWORD // SQL keyword
	SYMBOL  // punctuation like (, ), ; *
)

func (t TokenType) String() string {
	switch t {
	case ILLEGAL:
		return "ILLEGAL"
	case EOF:
		return "EOF"
	case IDENT:
		return "IDENT"
	case INT:
		return "INT"
	case STRING:
		return "STRING"
	case KEYWORD:
		return "KEYWORD"
	case SYMBOL:
		return "SYMBOL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

type Token struct {
	Type    TokenType
	Literal string
}

var keywords = map[string]struct{}{
	"CREATE": {}, "TABLE": {}, "INSERT": {}, "INTO": {}, "VALUES": {},
	"SELECT": {}, "FROM": {}, "INT": {}, "TEXT": {}, "WHERE": {},
}

func Tokenize(input string) ([]Token, error) {
	tokens := []Token{}
	i := 0
	for i < len(input) {
		ch := input[i]

		switch {
		case unicode.IsSpace(rune(ch)):
			i++

		case isLetter(ch):
			start := i
			for i < len(input) && (isLetter(input[i]) || isDigit(input[i]) || input[i] == '_') {
				i++
			}
			lit := input[start:i]
			tokType := IDENT
			if _, ok := keywords[strings.ToUpper(lit)]; ok {
				tokType = KEYWORD
			}
			tokens = append(tokens, Token{Type: tokType, Literal: lit})

		case isDigit(ch):
			start := i
			for i < len(input) && isDigit(input[i]) {
				i++
			}
			tokens = append(tokens, Token{Type: INT, Literal: input[start:i]})

		case ch == '\'':
			i++
			start := i
			for i < len(input) {
				if input[i] == '\'' {
					if i+1 < len(input) && input[i+1] == '\'' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			if i >= len(input) {
				return nil, fmt.Errorf("unterminated string literal")
			}
			literal := input[start:i]
			literal = strings.ReplaceAll(literal, "''", "'")
			tokens = append(tokens, Token{Type: STRING, Literal: literal})
			i++

		case ch == '"':
			i++
			start := i
			for i < len(input) {
				if input[i] == '"' {
					if i+1 < len(input) && input[i+1] == '"' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			if i >= len(input) {
				return nil, fmt.Errorf("unterminated quoted identifier")
			}
			literal := input[start:i]
			literal = strings.ReplaceAll(literal, `""`, `"`)
			tokens = append(tokens, Token{Type: STRING, Literal: literal})
			i++

		case strings.ContainsRune("(),;*=", rune(ch)):
			tokens = append(tokens, Token{Type: SYMBOL, Literal: string(ch)})
			i++

		default:
			return nil, fmt.Errorf("illegal character: %c", ch)
		}
	}
	tokens = append(tokens, Token{Type: EOF, Literal: ""})
	return tokens, nil
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}
func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
}
