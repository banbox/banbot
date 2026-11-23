package entry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Source-level guard: startExecutionRobotCrons must hand the delay monitor a
// state that this runtime really writes. The trader's own state is never
// written on the execution-backed path (permanent timeout -> close_on_stuck
// force-closes every open order).
func TestStartExecutionRobotCronsWiresReceivedState(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "execution_robot.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var decl *ast.FuncDecl
	for _, d := range file.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "startExecutionRobotCrons" {
			decl = fd
		}
	}
	if decl == nil {
		t.Fatal("startExecutionRobotCrons not found")
	}
	var stateVar string
	var seeded, observerSet, updatedInObserver, optsReceived bool
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if len(x.Lhs) == 1 && len(x.Rhs) == 1 {
				if call, ok := x.Rhs[0].(*ast.CallExpr); ok && selName(call.Fun) == "NewPairCopiedState" {
					if id, ok := x.Lhs[0].(*ast.Ident); ok {
						stateVar = id.Name
					}
				}
			}
		case *ast.CallExpr:
			switch selName(x.Fun) {
			case "Touch":
				if recvIs(x.Fun, stateVar) {
					seeded = true
				}
			case "SetSeriesObserver":
				observerSet = true
				if len(x.Args) == 1 {
					if lit, ok := x.Args[0].(*ast.FuncLit); ok {
						ast.Inspect(lit.Body, func(m ast.Node) bool {
							if c, ok := m.(*ast.CallExpr); ok && selName(c.Fun) == "SetPairMsAt" && recvIs(c.Fun, stateVar) {
								updatedInObserver = true
							}
							return true
						})
					}
				}
			}
		case *ast.KeyValueExpr:
			if id, ok := x.Key.(*ast.Ident); ok && id.Name == "Received" {
				if v, ok := x.Value.(*ast.Ident); ok && stateVar != "" && v.Name == stateVar {
					optsReceived = true
				}
			}
		}
		return true
	})
	if stateVar == "" {
		t.Fatal("no com.NewPairCopiedState() created in startExecutionRobotCrons")
	}
	if !seeded {
		t.Error("the state must be seeded with Touch(now) so startup is not reported as a timeout")
	}
	if !observerSet || !updatedInObserver {
		t.Errorf("series observer must update the state via SetPairMsAt (set=%v updates=%v)", observerSet, updatedInObserver)
	}
	if !optsReceived {
		t.Error("ExecutionRobotCronOptions.Received must be the created, non-nil state")
	}
}

func selName(e ast.Expr) string {
	if s, ok := e.(*ast.SelectorExpr); ok {
		return s.Sel.Name
	}
	return ""
}

func recvIs(e ast.Expr, name string) bool {
	if s, ok := e.(*ast.SelectorExpr); ok {
		if id, ok := s.X.(*ast.Ident); ok {
			return name != "" && id.Name == name
		}
	}
	return false
}
