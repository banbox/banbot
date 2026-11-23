package entry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// NewFactorLiveProvider returns *errs.Error. Assigning its result to a variable
// declared as the builtin `error` (conventionally `err`) turns a nil pointer
// into a non-nil error interface (typed-nil), which made the robot exit right
// after startup with an empty error message. The result must be captured in a
// variable of the concrete type (e.g. `providerErr`). This guard fails if any
// call to NewFactorLiveProvider assigns its second result to an identifier `err`.
func TestNewFactorLiveProviderErrorNotAssignedToErr(t *testing.T) {
	files := []string{"time_series_hub.go", "time_series_execution.go"}
	fset := token.NewFileSet()
	calls := 0
	for _, name := range files {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 || len(as.Lhs) < 2 {
				return true
			}
			call, ok := as.Rhs[0].(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NewFactorLiveProvider" {
				return true
			}
			calls++
			if id, ok := as.Lhs[1].(*ast.Ident); ok && id.Name == "err" {
				t.Errorf("%s: NewFactorLiveProvider error assigned to `err` (typed-nil *errs.Error hazard)",
					fset.Position(as.Pos()))
			}
			return true
		})
	}
	if calls == 0 {
		t.Fatal("no NewFactorLiveProvider assignments found; guard is stale")
	}
}

// BindFactorsLive and provider.Stop return *errs.Error. Its result must not be
// stored in the function-level `err` (error), and Stop's result must not be
// passed to errors.Join unguarded (Join keeps non-nil interfaces holding nil
// pointers).
func TestFactorHubBindAndStopTypedNil(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "factor_hub.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	bind := 0
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if len(x.Rhs) != 1 || len(x.Lhs) < 3 {
				return true
			}
			if call, ok := x.Rhs[0].(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "BindFactorsLive" {
					bind++
					if id, ok := x.Lhs[2].(*ast.Ident); ok && id.Name == "err" {
						t.Errorf("%s: BindFactorsLive error assigned to `err`", fset.Position(x.Pos()))
					}
				}
			}
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Join" {
				for _, a := range x.Args {
					if c, ok := a.(*ast.CallExpr); ok {
						if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "Stop" {
							if id, ok := s.X.(*ast.Ident); ok && id.Name == "provider" {
								t.Errorf("%s: errors.Join(provider.Stop()) is typed-nil unsafe", fset.Position(x.Pos()))
							}
						}
					}
				}
			}
		}
		return true
	})
	if bind == 0 {
		t.Fatal("BindFactorsLive assignment not found; guard is stale")
	}
}
