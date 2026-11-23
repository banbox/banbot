package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/rpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The real prepare path (no sink override) must not disable notifications when
// the rpc session starts cleanly: (*rpc.Session).Start returns *errs.Error and a
// nil pointer converted to error is non-nil.
func TestNotifierRealPrepareWithEmptySessionStaysEnabled(t *testing.T) {
	obsCore, logs := observer.New(zap.WarnLevel)
	rt := &Runtime{
		Core:          &core.State{Logger: zap.New(obsCore)},
		Notifications: rpc.NewSession(nil, nil),
	}
	n := rt.Notifier()
	if n == nil {
		t.Fatal("notifier must exist for a runtime with a session")
	}
	t.Cleanup(n.Stop)
	if !n.Post(map[string]interface{}{"type": rpc.MsgTypeStatus, "status": "x"}) {
		t.Fatal("post dropped")
	}
	// run() executes prepare before draining the queue, so a completed flush
	// means prepare has finished.
	if !n.Flush(5 * time.Second) {
		t.Fatal("flush timed out")
	}
	for _, e := range logs.All() {
		t.Fatalf("unexpected warning %q %v (notifications were disabled by a typed-nil error)", e.Message, e.ContextMap())
	}
}

func TestOrderNotifyNoErrorFuncReturnsStartDirectly(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "order_notify.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		lit, ok := node.(*ast.FuncLit)
		if !ok || lit.Type.Results == nil || len(lit.Type.Params.List) != 0 || len(lit.Type.Results.List) != 1 {
			return true
		}
		if id, ok := lit.Type.Results.List[0].Type.(*ast.Ident); !ok || id.Name != "error" {
			return true
		}
		ast.Inspect(lit.Body, func(inner ast.Node) bool {
			if _, nested := inner.(*ast.FuncLit); nested {
				return false
			}
			ret, ok := inner.(*ast.ReturnStmt)
			if !ok {
				return true
			}
			for _, r := range ret.Results {
				if call, ok := r.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Start" {
						t.Errorf("%s: func() error returns %s.Start() directly; guard the *errs.Error against nil", fset.Position(ret.Pos()), "x")
					}
				}
			}
			return true
		})
		return true
	})
}
