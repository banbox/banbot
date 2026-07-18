package opt

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"gopkg.in/yaml.v3"
)

func TestSortOptLogs(t *testing.T) {
	sortOptLogs("E:\\trade\\go\\bandata\\backtest\\opt_bearMacd.log")
}

func TestParseSectionTitlePreservesSlashPairs(t *testing.T) {
	name, dirt, tfStr, pairStr := parseSectionTitle("Demo:l/1h|4h/BTC/USDT:USDT|ETH/USDT:USDT")
	if name != "Demo" || dirt != "long" || tfStr != "1h|4h" || pairStr != "BTC/USDT:USDT|ETH/USDT:USDT" {
		t.Fatalf("parsed title = %q, %q, %q, %q", name, dirt, tfStr, pairStr)
	}
}

func TestOptInfoToPolPreservesSourcePolicy(t *testing.T) {
	source := &config.RunPolicyConfig{
		Name:          "Demo",
		Filters:       []*config.CommonPairFilter{{Name: "VolumePairFilter", Items: map[string]interface{}{"limit": 12}}},
		TimeFrames:    "1h,4h",
		RunTimeframes: []string{"1h", "4h"},
		RefineTF:      "15m",
		MaxPair:       8,
		MaxOpen:       7,
		MaxSimulOpen:  3,
		OrderBarMax:   500,
		StakeRate:     1.75,
		StopLoss:      "2%",
		StratPerf:     &config.StratPerfConfig{Enable: true, MinOdNum: 6},
		Pairs:         []string{"BTC/USDT:USDT", "ETH/USDT:USDT"},
		Params:        map[string]float64{"threshold": 0.1},
		PairParams:    map[string]map[string]float64{"BTC/USDT:USDT": {"threshold": 0.2}},
		More:          map[string]interface{}{"custom_flag": true},
	}
	info := &OptInfo{Dirt: "short", Params: map[string]float64{"threshold": 0.003938123456789}, Score: 4.2}

	got := info.ToPol(source, 1, "Demo", "long", "4h", "BTC/USDT:USDT")
	if got.Index != 1 || got.Name != "Demo" || got.Dirt != "short" || got.Score != 4.2 {
		t.Fatalf("optimized identity not applied: %#v", got)
	}
	if got.RefineTF != "15m" || got.OrderBarMax != 500 || got.StakeRate != 1.75 || got.StopLoss != "2%" {
		t.Fatalf("source execution fields lost: %#v", got)
	}
	if !reflect.DeepEqual(got.Filters, source.Filters) || !reflect.DeepEqual(got.StratPerf, source.StratPerf) || !reflect.DeepEqual(got.More, source.More) {
		t.Fatal("source filter/performance/inline fields lost")
	}
	if !reflect.DeepEqual(got.RunTimeframes, []string{"4h"}) || !reflect.DeepEqual(got.Pairs, []string{"BTC/USDT:USDT"}) {
		t.Fatalf("optimized scope not applied: %#v", got)
	}
	if got.Params["threshold"] != 0.003938123456789 || got.PairParams != nil {
		t.Fatalf("optimized parameters not authoritative: %#v", got)
	}
	got.Filters[0].Items["limit"] = 99
	if source.Filters[0].Items["limit"] != 12 {
		t.Fatal("winner mutation changed source policy")
	}
}

func TestOptInfoLineRoundTripPreservesWinnerReplayParams(t *testing.T) {
	want := map[string]float64{
		"entry_threshold": 0.003938123456789,
		"huge":            math.MaxFloat64,
		"tiny":            math.SmallestNonzeroFloat64,
		"z_threshold":     1.7934,
	}
	info := &OptInfo{
		Score:    4.2,
		Params:   want,
		Ints:     map[string]bool{},
		BTResult: &BTResult{OrderNum: 7},
		ID:       "replay",
	}

	line := info.ToLine()
	for i := 0; i < 10; i++ {
		if got := info.ToLine(); got != line {
			t.Fatalf("opt line is not deterministic:\nfirst: %s\nnext:  %s", line, got)
		}
	}
	wantText := "entry_threshold: 0.003938123456789, huge: 1.7976931348623157e+308, tiny: 5e-324, z_threshold: 1.7934"
	if !strings.Contains(line, wantText) {
		t.Fatalf("opt params are not shortest round-trip values:\n%s", line)
	}

	parsed := parseOptLine(line)
	if !reflect.DeepEqual(parsed.Params, want) {
		t.Fatalf("opt.log changed replay params:\n got: %#v\nwant: %#v\nline: %s", parsed.Params, want, line)
	}

	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "detail"), 0o755); err != nil {
		t.Fatal(err)
	}
	info.PairGrps = []*RowItem{{}}
	info.dumpDetail(filepath.Join(dir, "detail", info.ID+".json"))
	logText := strings.Join([]string{
		"# run hyper optimize: bayes, rounds: 1",
		"# date range: test",
		"",
		"============== ReplayProbe/1h/BTC/USDT:USDT =============",
		line,
		"[score] " + line,
		"",
	}, "\n")
	logPath := filepath.Join(dir, "opt.log")
	if err := os.WriteFile(logPath, []byte(logText), 0o644); err != nil {
		t.Fatal(err)
	}
	winnerYAML, collectErr := collectOptLog([]string{logPath}, 0, "score", "", []*config.RunPolicyConfig{{
		Name:          "ReplayProbe",
		RunTimeframes: []string{"1h"},
		Pairs:         []string{"BTC/USDT:USDT"},
	}})
	if collectErr != nil {
		t.Fatal(collectErr)
	}
	var wrapper struct {
		RunPolicy []*config.RunPolicyConfig `yaml:"run_policy"`
	}
	if err := yaml.Unmarshal([]byte(winnerYAML), &wrapper); err != nil {
		t.Fatal(err)
	}
	if len(wrapper.RunPolicy) != 1 || !reflect.DeepEqual(wrapper.RunPolicy[0].Params, want) {
		t.Fatalf("collected winner YAML changed replay params:\n got: %#v\nwant: %#v\nYAML:\n%s", wrapper.RunPolicy, want, winnerYAML)
	}
}

func TestParseOptLineAcceptsLegacyRoundedParams(t *testing.T) {
	line := "loss:   -4.20 \tentry_threshold: 0.00, z_threshold: 1.79\t \todNum: 7, profit: 3.0%, drawDown: 1.0%, sharpe: 0.50, id: legacy"
	got := parseOptLine(line)
	want := map[string]float64{"entry_threshold": 0, "z_threshold": 1.79}
	if !reflect.DeepEqual(got.Params, want) || got.Score != 4.2 || got.OrderNum != 7 || got.ID != "legacy" {
		t.Fatalf("legacy opt line parsed incorrectly: %#v", got)
	}
}

func TestOptInfoLineNonFinitePolicy(t *testing.T) {
	info := &OptInfo{
		Params: map[string]float64{
			"nan":     math.NaN(),
			"neg_inf": math.Inf(-1),
			"pos_inf": math.Inf(1),
		},
		Ints:     map[string]bool{},
		BTResult: &BTResult{},
	}
	line := info.ToLine()
	if !strings.Contains(line, "nan: NaN, neg_inf: -Inf, pos_inf: +Inf") {
		t.Fatalf("unexpected non-finite encoding: %s", line)
	}
	got := parseOptLine(line).Params
	if !math.IsNaN(got["nan"]) || !math.IsInf(got["neg_inf"], -1) || !math.IsInf(got["pos_inf"], 1) {
		t.Fatalf("non-finite values did not preserve their classification: %#v", got)
	}
}

func TestMarshalOptimizeChildConfigPreservesPolicyAndOverrides(t *testing.T) {
	args := &config.CmdArgs{
		StakeAmount: 123.456789,
		StakePct:    0.123456789,
		TimeFrames:  []string{"15m", "1h"},
		Pairs:       []string{"BTC/USDT:USDT"},
		MaxPoolSize: 17,
	}
	pol := &config.RunPolicyConfig{
		Name:          "Demo",
		RunTimeframes: []string{"1h"},
		RefineTF:      "15m",
		OrderBarMax:   500,
		StakeRate:     1.75,
		StopLoss:      "2%",
		Params:        map[string]float64{"threshold": 0.003938123456789},
		More:          map[string]interface{}{"custom_flag": true},
	}
	data, err := marshalOptimizeChildConfig(args, pol, "1700000000", "1701000000")
	if err != nil {
		t.Fatal(err)
	}
	var got config.Config
	if err = yaml.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeStart != "1700000000" || got.TimeEnd != "1701000000" || got.StakeAmount != args.StakeAmount || got.StakePct != args.StakePct {
		t.Fatalf("child root overrides lost:\n%s", data)
	}
	if !reflect.DeepEqual(got.RunTimeframes, args.TimeFrames) || !reflect.DeepEqual(got.Pairs, args.Pairs) {
		t.Fatalf("child scope overrides lost:\n%s", data)
	}
	if got.Database == nil || got.Database.MaxPoolSize != 17 || len(got.RunPolicy) != 1 {
		t.Fatalf("child database/policy lost:\n%s", data)
	}
	childPol := got.RunPolicy[0]
	if childPol.RefineTF != "15m" || childPol.OrderBarMax != 500 || childPol.StakeRate != 1.75 || childPol.StopLoss != "2%" || childPol.Params["threshold"] != 0.003938123456789 {
		t.Fatalf("child policy changed:\n%s", data)
	}
}

func TestOptimizeChildBaseArgsForwardsConfigSourcesWithoutInlineSecrets(t *testing.T) {
	args := &config.CmdArgs{
		OptRounds:  12,
		Sampler:    "bayes",
		EachPairs:  true,
		NoDefault:  true,
		DataDir:    t.TempDir(),
		Configs:    config.ArrString{"one.yml", "two.yml"},
		ConfigData: "stake_amount: 42\nsecret_probe: do-not-log-inline\n",
		Picker:     "score",
	}
	cmds, cleanup, err := optimizeChildBaseArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	joined := strings.Join(cmds, " ")
	if !strings.Contains(joined, "-no-default") || !strings.Contains(joined, "-datadir "+args.DataDir) || !strings.Contains(joined, "-config one.yml -config two.yml") {
		t.Fatalf("config source flags missing: %v", cmds)
	}
	if strings.Contains(joined, "do-not-log-inline") {
		t.Fatalf("inline config leaked into child command: %v", cmds)
	}
	var inlinePath string
	for i := 0; i+1 < len(cmds); i++ {
		if cmds[i] == "-config" && strings.Contains(cmds[i+1], "ban_opt_parent_config") {
			inlinePath = cmds[i+1]
		}
	}
	data, readErr := os.ReadFile(inlinePath)
	if readErr != nil || string(data) != args.ConfigData {
		t.Fatalf("forwarded inline config = %q, err=%v", data, readErr)
	}
}
