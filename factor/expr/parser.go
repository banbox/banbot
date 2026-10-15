// Package expr compiles a restricted, declarative factor language to native DAGs.
package expr

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
)

const (
	maxText        = 16 * 1024
	maxTotalText   = 256 * 1024
	maxNodes       = 8192
	maxDepth       = 64
	maxDefinitions = 512
	maxWindow      = 10_000
)

type expression struct {
	kind rune
	text string
	pos  scanner.Position
	args []*expression
}

type parser struct {
	s     scanner.Scanner
	token rune
	text  string
	pos   scanner.Position
	path  string
	count *int
	err   error
}

func parse(path, source string, count *int) (*expression, error) {
	if len(source) > maxText {
		return nil, fmt.Errorf("%s: expression exceeds %d bytes", path, maxText)
	}
	p := &parser{path: path, count: count}
	p.s.Init(strings.NewReader(source))
	p.s.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats | scanner.ScanStrings
	p.s.Error = func(s *scanner.Scanner, message string) {
		if p.err == nil {
			p.err = fmt.Errorf("%s:%d:%d: %s", path, s.Pos().Line, s.Pos().Column, message)
		}
	}
	p.next()
	e := p.binary(0, 0)
	if p.err == nil && p.token != scanner.EOF {
		p.fail("unexpected trailing token %q", p.text)
	}
	return e, p.err
}

func (p *parser) next() { p.token = p.s.Scan(); p.text = p.s.TokenText(); p.pos = p.s.Position }
func (p *parser) fail(format string, args ...any) {
	if p.err == nil {
		p.err = fmt.Errorf("%s:%d:%d: %s", p.path, p.pos.Line, p.pos.Column, fmt.Sprintf(format, args...))
	}
}
func (p *parser) make(kind rune, text string, pos scanner.Position, args ...*expression) *expression {
	(*p.count)++
	if *p.count > maxNodes {
		p.fail("AST exceeds %d nodes", maxNodes)
	}
	return &expression{kind, text, pos, args}
}
func precedence(token rune) int {
	switch token {
	case '+', '-':
		return 1
	case '*', '/':
		return 2
	}
	return 0
}
func (p *parser) binary(minimum, depth int) *expression {
	if depth > maxDepth {
		p.fail("expression exceeds depth %d", maxDepth)
		return nil
	}
	left := p.primary(depth + 1)
	for p.err == nil && precedence(p.token) > minimum {
		token, text, pos := p.token, p.text, p.pos
		p.next()
		right := p.binary(precedence(token), depth+1)
		left = p.make(token, text, pos, left, right)
	}
	return left
}
func (p *parser) primary(depth int) *expression {
	if p.err != nil {
		return nil
	}
	if depth > maxDepth {
		p.fail("expression exceeds depth %d", maxDepth)
		return nil
	}
	token, text, pos := p.token, p.text, p.pos
	p.next()
	switch token {
	case '+', '-':
		return p.make('u', text, pos, p.primary(depth+1))
	case scanner.Int, scanner.Float:
		if strings.ContainsAny(text, "xXpP_") {
			p.fail("only decimal numbers without separators are supported")
		}
		if _, err := strconv.ParseFloat(text, 64); err != nil {
			p.fail("invalid decimal number %q", text)
		}
		return p.make('n', text, pos)
	case scanner.String:
		value, err := strconv.Unquote(text)
		if err != nil {
			p.fail("invalid string")
		}
		return p.make('s', value, pos)
	case '(':
		e := p.binary(0, depth+1)
		if p.token != ')' {
			p.fail("expected closing parenthesis")
			return e
		}
		p.next()
		return e
	case scanner.Ident:
		for p.token == '.' && p.err == nil {
			p.next()
			if p.token != scanner.Ident {
				p.fail("expected name after dot")
				break
			}
			text += "." + p.text
			p.next()
		}
		if p.token != '(' {
			return p.make('r', text, pos)
		}
		p.next()
		var args []*expression
		if p.token != ')' {
			for p.err == nil {
				args = append(args, p.binary(0, depth+1))
				if p.token != ',' {
					break
				}
				p.next()
			}
		}
		if p.token != ')' {
			p.fail("expected closing parenthesis")
			return nil
		}
		p.next()
		return p.make('c', text, pos, args...)
	default:
		p.fail("expected expression, got %q", text)
		return nil
	}
}
