package entry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/banbox/banbot/config"
)

// funcCalls collects, for one function declaration, whether it assigns
// `<x>.APIServer = accountDashboardConfig(...)` and whether it calls
// StartWebAPI outside any `if` statement (it must not depend on `startup`).
func funcWiring(t *testing.T, file, name string) (assignsAPIServer, startsWebAPI, startsUnconditionally bool) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var decl *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name {
			decl = fd
		}
	}
	if decl == nil {
		t.Fatalf("%s: func %s not found", file, name)
	}
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == 1 && len(as.Rhs) == 1 {
			if sel, ok := as.Lhs[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "APIServer" {
				if call, ok := as.Rhs[0].(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "accountDashboardConfig" {
						assignsAPIServer = true
					}
				}
			}
		}
		return true
	})
	// Top-level statements only: a call nested in an if block is conditional.
	for _, stmt := range decl.Body.List {
		ifs, ok := stmt.(*ast.IfStmt)
		if !ok {
			continue
		}
		init, ok := ifs.Init.(*ast.AssignStmt)
		if !ok || len(init.Rhs) != 1 {
			continue
		}
		if call, ok := init.Rhs[0].(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "StartWebAPI" {
				startsWebAPI, startsUnconditionally = true, true
			}
		}
	}
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "StartWebAPI" {
				startsWebAPI = true
			}
		}
		return true
	})
	return
}

func TestExecutionEntriesPassDashboardConfigAndStartAPI(t *testing.T) {
	for _, c := range []struct {
		file, fn string
		starts   bool
	}{
		{"time_series_hub.go", "runTimeSeriesHubAccount", true},
		{"time_series_execution.go", "runTimeSeriesExecutionAccount", true},
		{"factor_hub.go", "runFactorHubAccount", true},
		{"factor_live.go", "runFactorsLiveWithStartup", true},
	} {
		assigns, starts, unconditional := funcWiring(t, c.file, c.fn)
		if !assigns {
			t.Errorf("%s %s does not set APIServer from accountDashboardConfig", c.file, c.fn)
		}
		if c.starts && (!starts || !unconditional) {
			t.Errorf("%s %s must call trader.StartWebAPI() unconditionally (not inside `if startup != nil`): starts=%v unconditional=%v", c.file, c.fn, starts, unconditional)
		}
	}
}

// Config.Clone drops api_server, so mixedLiveAccountConfig alone never carries
// it; the entries must set it explicitly and only for single-account robots.
func TestMixedLiveAccountConfigNeedsExplicitAPIServer(t *testing.T) {
	root := dashboardTestRoot(true, "a")
	cfg := mixedLiveAccountConfig(root, "a", nil)
	if cfg.APIServer != nil {
		t.Fatal("Clone now copies APIServer; the explicit assignment in the entries can be reconsidered")
	}
	cfg.APIServer = accountDashboardConfig(root, "a")
	if cfg.APIServer == nil || !cfg.APIServer.Enable || cfg.APIServer.Port != 8001 {
		t.Fatalf("single account must get the api_server section, got %+v", cfg.APIServer)
	}
	multi := dashboardTestRoot(true, "a", "b")
	multi.Accounts["a"], multi.Accounts["b"] = &config.AccountConfig{}, &config.AccountConfig{}
	if accountDashboardConfig(multi, "a") != nil {
		t.Fatal("multi-account robots must not bind the dashboard")
	}
}
