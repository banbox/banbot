package entry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// realVerifyEmbeddedAccounts is the production verifier, captured before the
// package-wide test substitution below. Package-level variables initialise
// before any init function runs.
var realVerifyEmbeddedAccounts = verifyEmbeddedAccounts

// Existing embedded-entry tests use exchange fakes without an account-access
// endpoint; they exercise ledger and quote wiring, not the account check, which
// is covered by this file and by the live package tests.
func init() {
	verifyEmbeddedAccounts = func(biz.RuntimeDeps) error { return nil }
}

type deniedAccessExchange struct{ banexg.BanExchange }

func (deniedAccessExchange) FetchBalance(map[string]interface{}) (*banexg.Balances, *errs.Error) {
	return &banexg.Balances{}, nil
}

func (deniedAccessExchange) FetchAccountAccess(map[string]interface{}) (*banexg.AccountAccess, *errs.Error) {
	return &banexg.AccountAccess{TradeKnown: true, TradeAllowed: false}, nil
}

func TestProductionVerifierFailsClosedAndIsLiveVerifier(t *testing.T) {
	cfg := &config.AccountConfig{}
	err := realVerifyEmbeddedAccounts(biz.RuntimeDeps{
		Core:     &core.State{EnvReal: true},
		Exchange: deniedAccessExchange{},
		Accounts: map[string]*config.AccountConfig{"acc": cfg},
		Config:   config.NewSnapshot(&config.Config{}),
	})
	if err == nil || !strings.Contains(err.Error(), "acc") {
		t.Fatalf("a failed account check must be a startup error, got %v", err)
	}
	if cfg.NoTrade {
		t.Fatal("the embedded verifier must not rely on NoTrade")
	}
	f, perr := parser.ParseFile(token.NewFileSet(), "execution_robot.go", nil, 0)
	if perr != nil {
		t.Fatal(perr)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		if vs, ok := n.(*ast.ValueSpec); ok && len(vs.Names) == 1 && vs.Names[0].Name == "verifyEmbeddedAccounts" && len(vs.Values) == 1 {
			if sel, ok := vs.Values[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "VerifyLiveAccountsWithRuntimeDeps" {
				found = true
			}
		}
		return true
	})
	if !found {
		t.Fatal("verifyEmbeddedAccounts must default to live.VerifyLiveAccountsWithRuntimeDeps")
	}
}
