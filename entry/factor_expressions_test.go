package entry

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/runner"
)

func TestExpressionYAMLResearchAndBacktest(t *testing.T) {
	_, path := factorYAMLFixture(t)
	formula := `      expressions:
        schema_version: 1
        timeframe: 1h
        bindings:
          kline: {source: kline, timeframe: 1h}
        params: {window: 2}
        lets:
          mom: "ts.return(kline.close, param.window)"
        outputs:
          momentum: "cs.zscore(factor.mom)"
          risk_adjusted: "cs.zscore(factor.mom / max(ts.std(ts.return(kline.close, 1), param.window, 0), 1e-8))"
        combine:
          method: fixed
          weights: {momentum: 0.5, risk_adjusted: 0.5}
`
	raw, _ := os.ReadFile(path)
	body := strings.Replace(string(raw), "name: MomentumVol", "name: MyFormula", 1) + formula
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"research", "--no-default", "--config", path}, {"backtest", "--no-default", "--config", path}} {
		command := NewRootCommand()
		var out bytes.Buffer
		command.SetOut(&out)
		command.SetArgs(args)
		if err := command.Execute(); err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte{'\n'})
		var result runner.Result
		if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil {
			t.Fatal(err)
		}
		if result.Decisions != 8 || result.Manifest.FactorPlanHash == "" || len(result.Manifest.Combo.Columns) != 2 {
			t.Fatalf("expression pipeline missing: %+v", result)
		}
	}
	for _, invalid := range []string{
		strings.Replace(body, "schema_version: 1", "schema_version: 1\n        typo: 1", 1),
		strings.Replace(body, "      expressions:", "      definition: momentum-vol\n      expressions:", 1),
		strings.Replace(body, "timeframe: 1h", "timeframe: 1d", 1),
		strings.Replace(body, "cs.zscore(factor.mom)", "cs.zscore(label.future)", 1),
	} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		spec, err := loadFactorYAMLSpec([]string{path})
		if err == nil {
			_, err = buildFactorConfigs(spec, runner.Weights)
		}
		if err == nil {
			t.Fatal("invalid expression YAML accepted")
		}
	}
}

func TestExpressionsNeverUseFactorFieldAsPrice(t *testing.T) {
	c := runner.Config{Expressions: &expr.Spec{TimeFrame: "1h"}}
	c.Factor.Source, c.Factor.Field = "funding", "rate"
	c.Snapshot.Schemas = map[string]string{"funding": "v1"}
	if err := deriveArchivePrice(&c); err == nil {
		t.Fatal("expression factor field inferred as execution price")
	}
}

func TestExpressionCompileCommandsWithoutData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "expression.yml")
	body := "schema_version: 1\ntimeframe: 1h\nbindings:\n  kline: {source: kline, timeframe: 1h}\noutputs:\n  momentum: 'cs.zscore(ts.return(kline.close, 3))'\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"validate", "explain"} {
		cmd := NewRootCommand()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs([]string{name, "--spec", path})
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		var result struct {
			Hash      string
			Warmup    int
			TimeFrame string
		}
		if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.Hash == "" || result.Warmup != 3 || result.TimeFrame != "1h" {
			t.Fatalf("compile output: %s (%v)", out.Bytes(), err)
		}
	}
	for _, invalid := range []string{body + "typo: 1\n", body + "---\n{}\n", strings.ReplaceAll(body, "timeframe:", "frequency:"), strings.Repeat(" ", (1<<20)+1)} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := NewRootCommand()
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{"validate", "--spec", path})
		if err := cmd.Execute(); err == nil {
			t.Fatal("invalid standalone spec accepted")
		}
	}
}
