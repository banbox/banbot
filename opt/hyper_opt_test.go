package opt

import (
	"os"
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
