package entry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestHourOnlySymbolsFromSubs(t *testing.T) {
	a := &orm.ExSymbol{ID: 1, Symbol: "A"}
	b := &orm.ExSymbol{ID: 2, Symbol: "B"}
	c := &orm.ExSymbol{ID: 3, Symbol: "C"}
	subs := []*strat.DataSub{
		{Source: orm.SeriesSourceKline, ExSymbol: a, TimeFrame: "1h"},
		{Source: orm.SeriesSourceKline, ExSymbol: a, TimeFrame: "4h"},
		{Source: orm.SeriesSourceKline, ExSymbol: b, TimeFrame: "1h"},
		{Source: orm.SeriesSourceKline, ExSymbol: b, TimeFrame: "15m"}, // sub-hour wins
		{Source: "", ExSymbol: c, TimeFrame: "1d"},                     // default source is kline
		{Source: "funding_rate", ExSymbol: b, TimeFrame: "1m"},         // non-kline ignored
		nil,
		{ExSymbol: nil, TimeFrame: "1h"},
	}
	got := hourOnlySymbolsFromSubs(subs)()
	if len(got) != 2 || got[1] != a || got[3] != c || got[2] != nil {
		t.Fatalf("hour-only = %v", got)
	}
	if len(hourOnlySymbolsFromSubs(nil)()) != 0 {
		t.Fatal("no subscriptions must yield no hour-only symbols")
	}
}

func TestIgnoredExecutionConfigKeys(t *testing.T) {
	filters := []*config.CommonPairFilter{{Name: "VolumePairFilter"}}
	tests := []struct {
		name string
		cfg  *config.Config
		want []string
	}{
		{"nil", nil, nil},
		{"defaults", &config.Config{}, nil},
		{"bundled pairlists with static pairs", &config.Config{Pairs: []string{"BTC/USDT"}, PairFilters: filters}, nil},
		{"pairlists with static policy pairs", &config.Config{PairFilters: filters, RunPolicy: []*config.RunPolicyConfig{{Pairs: []string{"BTC/USDT"}}}}, nil},
		{"pairlists select dynamically", &config.Config{PairFilters: filters}, []string{"pairlists"}},
		{"pairlists forced over static pairs", &config.Config{Pairs: []string{"BTC/USDT"}, PairFilters: filters, PairMgr: &config.PairMgrConfig{ForceFilters: true}}, []string{"pairlists"}},
		{"empty pairmgr", &config.Config{PairMgr: &config.PairMgrConfig{}}, nil},
		{"pairmgr cron", &config.Config{PairMgr: &config.PairMgrConfig{Cron: "0 0 * * * *"}}, []string{"pairmgr.cron"}},
		{"wallet knobs", &config.Config{MinOpenRate: 0.4, ChargeOnBomb: true, MarginAddRate: 0.5, AccountPullSecs: 30}, []string{"min_open_rate", "charge_on_bomb", "margin_add_rate", "account_pull_secs"}},
		{"close_on_stuck is honoured", &config.Config{CloseOnStuck: 5}, nil},
		{"bt_in_live without cron", &config.Config{BTInLive: &config.BtInLiveConfig{}}, nil},
		{"bt_in_live", &config.Config{BTInLive: &config.BtInLiveConfig{Cron: "0 0 * * * *"}}, []string{"bt_in_live"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ignoredExecutionConfigKeys(tt.cfg); !slices.Equal(got, tt.want) {
				t.Fatalf("keys = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRejectExecutionDump(t *testing.T) {
	if err := rejectExecutionDump(nil); err != nil {
		t.Fatal(err)
	}
	if err := rejectExecutionDump(&config.CmdArgs{}); err != nil {
		t.Fatal(err)
	}
	if err := rejectExecutionDump(&config.CmdArgs{OutPath: "x.dump"}); err == nil {
		t.Fatal("--out must be rejected on the execution-backed path")
	}
}

func funcDecl(t *testing.T, file, name string) *ast.FuncDecl {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name {
			return fd
		}
	}
	t.Fatalf("%s: func %s not found", file, name)
	return nil
}

func funcCalls(fd *ast.FuncDecl, name string) bool {
	found := false
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				found = found || fun.Name == name
			case *ast.SelectorExpr:
				found = found || fun.Sel.Name == name
			}
		}
		return true
	})
	return found
}

func funcAssignsField(fd *ast.FuncDecl, field string) bool {
	found := false
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok {
			for _, lhs := range as.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == field {
					found = true
				}
			}
		}
		return true
	})
	return found
}

var executionRobotEntries = []struct{ file, fn string }{
	{"time_series_hub.go", "runTimeSeriesHubAccount"},
	{"time_series_execution.go", "runTimeSeriesExecutionAccount"},
	{"factor_hub.go", "runFactorHubAccount"},
	{"factor_live.go", "runFactorsLiveWithStartup"},
}

// Every execution-backed runtime must start its maintenance jobs, expose the
// log file to the dashboard and warn about ignored legacy keys.
func TestExecutionRobotEntriesWireCronsLogFileAndConfigWarning(t *testing.T) {
	for _, e := range executionRobotEntries {
		fd := funcDecl(t, e.file, e.fn)
		if !funcCalls(fd, "startExecutionRobotCrons") {
			t.Errorf("%s.%s does not start the execution robot crons", e.file, e.fn)
		}
		if !funcAssignsField(fd, "LogFile") {
			t.Errorf("%s.%s does not set rt.Core.LogFile", e.file, e.fn)
		}
		if !funcCalls(fd, "warnIgnoredExecutionConfig") {
			t.Errorf("%s.%s does not warn about ignored config keys", e.file, e.fn)
		}
	}
}

// runMixedLiveTSAccount delegates prod to the embedded TS entry (covered above)
// and refuses every other env: the legacy CryptoTrader.Run it used for them
// was removed.
func TestMixedLiveTSAccountDelegatesToExecutionEntry(t *testing.T) {
	fd := funcDecl(t, "mixed_factor_live.go", "runMixedLiveTSAccount")
	if !funcCalls(fd, "runTimeSeriesExecutionAccount") || !funcCalls(fd, "unsupportedLiveEnvMessage") || funcCalls(fd, "Run") {
		t.Fatal("runMixedLiveTSAccount must delegate prod to runTimeSeriesExecutionAccount and refuse other envs without trader.Run")
	}
}

func TestExecutionRobotEntriesStartProfiles(t *testing.T) {
	for _, e := range []struct{ file, fn string }{
		{"time_series_hub.go", "runTimeSeriesHubSpec"},
		{"time_series_execution.go", "runTimeSeriesExecution"},
		{"factor_hub.go", "runFactorHubSpec"},
		{"factor_live.go", "runFactorLiveSpecWithArgs"},
	} {
		if !funcCalls(funcDecl(t, e.file, e.fn), "startProfiles") {
			t.Errorf("%s.%s does not start --cpu-profile/--mem-profile", e.file, e.fn)
		}
	}
}

// Embedded entries hold account credentials and must fail closed on the
// startup account check (the execution-backed order managers ignore NoTrade).
func TestEmbeddedEntriesVerifyAccounts(t *testing.T) {
	for _, e := range []struct{ file, fn string }{
		{"time_series_execution.go", "runTimeSeriesExecutionAccount"},
		{"factor_live.go", "runFactorsLiveWithStartup"},
	} {
		if !funcCalls(funcDecl(t, e.file, e.fn), "verifyEmbeddedAccounts") {
			t.Errorf("%s.%s does not verify the account at startup", e.file, e.fn)
		}
	}
	for _, e := range []struct{ file, fn string }{
		{"time_series_hub.go", "runTimeSeriesHubAccount"},
		{"factor_hub.go", "runFactorHubAccount"},
	} {
		if funcCalls(funcDecl(t, e.file, e.fn), "verifyEmbeddedAccounts") {
			t.Errorf("%s.%s must not call private account checks in Hub mode", e.file, e.fn)
		}
	}
}

func TestExecutionLiveEntriesRejectDump(t *testing.T) {
	for _, e := range []struct{ file, fn string }{
		{"time_series_execution.go", "runTimeSeriesExecution"},
		{"factor_live.go", "runFactorLiveSpecWithArgs"},
	} {
		if !funcCalls(funcDecl(t, e.file, e.fn), "rejectExecutionDump") {
			t.Errorf("%s.%s does not reject --out", e.file, e.fn)
		}
	}
}
