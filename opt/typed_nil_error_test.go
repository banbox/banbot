package opt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// btFactorsWithReportDeps must declare `err` as *errs.Error because every value
// assigned to it is *errs.Error; with `var err error` a nil pointer becomes a
// non-nil interface and every `if err != nil` fires on success.
func TestBtFactorsWithReportDepsErrIsConcrete(t *testing.T) {
	f, perr := parser.ParseFile(token.NewFileSet(), "tools.go", nil, 0)
	if perr != nil {
		t.Fatal(perr)
	}
	found := false
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "btFactorsWithReportDeps" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok || len(vs.Names) != 1 || vs.Names[0].Name != "err" {
				return true
			}
			found = true
			if id, ok := vs.Type.(*ast.Ident); ok && id.Name == "error" {
				t.Errorf("err declared as builtin error (typed-nil hazard)")
			}
			return true
		})
	}
	if !found {
		t.Fatal("err declaration not found; guard is stale")
	}
}
