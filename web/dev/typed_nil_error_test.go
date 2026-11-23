package dev

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// config.GetConfig returns *errs.Error; collectSeparateBtTask must never
// `return nil, cfgErr` (a nil pointer becomes a non-nil error) in a branch that
// can be reached with cfgErr == nil.
func TestCollectSeparateBtTaskNoBareCfgErrReturn(t *testing.T) {
	f, perr := parser.ParseFile(token.NewFileSet(), "common.go", nil, 0)
	if perr != nil {
		t.Fatal(perr)
	}
	found := false
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "collectSeparateBtTask" {
			continue
		}
		found = true
		var walk func(n ast.Node, guarded bool)
		walk = func(n ast.Node, guarded bool) {
			ast.Inspect(n, func(c ast.Node) bool {
				switch s := c.(type) {
				case *ast.IfStmt:
					if b, ok := s.Cond.(*ast.BinaryExpr); ok && b.Op == token.NEQ {
						if id, ok := b.X.(*ast.Ident); ok && id.Name == "cfgErr" {
							walk(s.Body, true)
							if s.Else != nil {
								walk(s.Else, guarded)
							}
							return false
						}
					}
				case *ast.ReturnStmt:
					for _, r := range s.Results {
						if id, ok := r.(*ast.Ident); ok && id.Name == "cfgErr" && !guarded {
							t.Errorf("return of cfgErr outside a `cfgErr != nil` branch (typed-nil)")
						}
					}
				}
				return true
			})
		}
		walk(fn.Body, false)
	}
	if !found {
		t.Fatal("collectSeparateBtTask not found; guard is stale")
	}
}
