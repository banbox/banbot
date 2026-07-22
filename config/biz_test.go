package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/llm"
	"gopkg.in/yaml.v3"
)

func TestLoadConfig(t *testing.T) {
	args := CmdArgs{}
	err := LoadConfig(&args)
	if err != nil {
		fmt.Printf("load data error: %s", err)
		return
	}
	data, err2 := yaml.Marshal(Data)
	if err2 != nil {
		fmt.Printf("dump data error: %s", err2)
		return
	}
	fmt.Println("result: \n", string(data))
}

func TestParseConfigsTimerangeOverridesEarlierStartEnd(t *testing.T) {
	cfg := parseLayeredTimeConfig(t,
		"time_start: '20240101'\ntime_end: '20260101'\n",
		"timerange: 20240201-20240301\n",
	)

	wantStart, wantEnd, err := ParseTimeRange("20240201-20240301")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TimeStart != "" || cfg.TimeEnd != "" {
		t.Fatalf("legacy time fields survived timerange override: %q - %q", cfg.TimeStart, cfg.TimeEnd)
	}
	if cfg.TimeRange.StartMS != wantStart || cfg.TimeRange.EndMS != wantEnd {
		t.Fatalf("resolved range = %d-%d, want %d-%d", cfg.TimeRange.StartMS, cfg.TimeRange.EndMS, wantStart, wantEnd)
	}
}

func TestParseConfigsStartEndOverrideEarlierTimerange(t *testing.T) {
	cfg := parseLayeredTimeConfig(t,
		"timerange: 20240101-20260101\n",
		"time_start: '20240201'\ntime_end: '20240301'\n",
	)

	wantStart, wantEnd, err := ParseTimeRange("20240201-20240301")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TimeRangeRaw != "" {
		t.Fatalf("timerange survived time_start/time_end override: %q", cfg.TimeRangeRaw)
	}
	if cfg.TimeRange.StartMS != wantStart || cfg.TimeRange.EndMS != wantEnd {
		t.Fatalf("resolved range = %d-%d, want %d-%d", cfg.TimeRange.StartMS, cfg.TimeRange.EndMS, wantStart, wantEnd)
	}
}

func parseLayeredTimeConfig(t *testing.T, layers ...string) *Config {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, 0, len(layers))
	for i, layer := range layers {
		path := filepath.Join(dir, fmt.Sprintf("%d.yml", i))
		if err := os.WriteFile(path, []byte(layer), 0644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	cfg, err := ParseConfigs(paths, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Apply(&CmdArgs{}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestRunPolicyClonePreservesFieldsWithoutSharingMutableState(t *testing.T) {
	orig := &RunPolicyConfig{
		Index:         2,
		Name:          "clone_probe",
		Filters:       []*CommonPairFilter{{Name: "VolumePairFilter", Items: map[string]interface{}{"limit": 12}}},
		TimeFrames:    "1h,4h",
		RunTimeframes: []string{"1h", "4h"},
		RefineTF:      "15m",
		MaxPair:       8,
		MaxOpen:       7,
		MaxSimulOpen:  3,
		OrderBarMax:   500,
		StakeRate:     1.75,
		Dirt:          "long",
		StopLoss:      "2%",
		StratPerf:     &StratPerfConfig{Enable: true, MinOdNum: 6, MidWeight: 0.4},
		Pairs:         []string{"BTC/USDT:USDT", "ETH/USDT:USDT"},
		Params:        map[string]float64{"threshold": 0.003938123456789},
		PairParams:    map[string]map[string]float64{"BTC/USDT:USDT": {"threshold": 0.0042}},
		More:          map[string]interface{}{"custom": map[string]interface{}{"enabled": true}},
		defs:          map[string]*core.Param{"threshold": {Name: "threshold", Min: 0.001, Max: 0.01}},
		Score:         9.5,
	}

	clone := orig.Clone()
	if !reflect.DeepEqual(clone, orig) {
		t.Fatalf("clone lost fields:\n got: %#v\nwant: %#v", clone, orig)
	}

	clone.Filters[0].Items["limit"] = 99
	clone.RunTimeframes[0] = "5m"
	clone.StratPerf.MinOdNum = 99
	clone.Pairs[0] = "XRP/USDT:USDT"
	clone.Params["threshold"] = 1
	clone.PairParams["BTC/USDT:USDT"]["threshold"] = 1
	clone.More["custom"].(map[string]interface{})["enabled"] = false
	clone.defs["threshold"].Min = 1

	if got := orig.Filters[0].Items["limit"]; got != 12 {
		t.Fatalf("clone filter mutation changed original: %v", got)
	}
	if orig.RunTimeframes[0] != "1h" || orig.StratPerf.MinOdNum != 6 || orig.Pairs[0] != "BTC/USDT:USDT" {
		t.Fatal("clone slice or struct mutation changed original")
	}
	if orig.Params["threshold"] != 0.003938123456789 || orig.PairParams["BTC/USDT:USDT"]["threshold"] != 0.0042 {
		t.Fatal("clone parameter mutation changed original")
	}
	if got := orig.More["custom"].(map[string]interface{})["enabled"]; got != true {
		t.Fatalf("clone inline config mutation changed original: %v", got)
	}
	if orig.defs["threshold"].Min != 0.001 {
		t.Fatal("clone hyperparameter definition mutation changed original")
	}
}

func TestRunPolicyToYamlRoundTrip(t *testing.T) {
	want := &RunPolicyConfig{
		Name:          "yaml_probe",
		Filters:       []*CommonPairFilter{{Name: "VolumePairFilter", Items: map[string]interface{}{"limit": 12}}},
		TimeFrames:    "1h,4h",
		RunTimeframes: []string{"1h", "4h"},
		RefineTF:      "15m",
		MaxPair:       8,
		MaxOpen:       7,
		MaxSimulOpen:  3,
		OrderBarMax:   500,
		StakeRate:     1.75,
		Dirt:          "long",
		StopLoss:      "2%",
		StratPerf:     &StratPerfConfig{Enable: true, MinOdNum: 6, MidWeight: 0.4},
		Pairs:         []string{"BTC/USDT:USDT"},
		Params:        map[string]float64{"threshold": 0.003938123456789},
		PairParams:    map[string]map[string]float64{"BTC/USDT:USDT": {"threshold": 0.0042}},
		More:          map[string]interface{}{"custom_flag": true},
		Index:         4,
		Score:         9.5,
	}

	text := want.ToYaml()
	if text != want.ToYaml() {
		t.Fatal("policy YAML is not deterministic")
	}
	if strings.Contains(text, "index:") || strings.Contains(text, "score:") {
		t.Fatalf("runtime fields leaked into policy YAML:\n%s", text)
	}
	var wrapper struct {
		RunPolicy []*RunPolicyConfig `yaml:"run_policy"`
	}
	if err := yaml.Unmarshal([]byte("run_policy:\n"+text), &wrapper); err != nil {
		t.Fatal(err)
	}
	if len(wrapper.RunPolicy) != 1 {
		t.Fatalf("round trip policy count = %d", len(wrapper.RunPolicy))
	}
	got := wrapper.RunPolicy[0]
	got.Index, got.Score = want.Index, want.Score
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("policy YAML lost fields:\n got: %#v\nwant: %#v\nYAML:\n%s", got, want, text)
	}
}

func TestConfigClonePreservesOmittedFieldsAndDesensitizesLLMKeys(t *testing.T) {
	temperature := 0.25
	orig := &Config{
		CloseOnStuck:     7,
		BTLegacyIntrabar: true,
		BTLegacyWallet:   true,
		NTPLangCode:      "en-US",
		ShowLangCode:     "zh-CN",
		BTInLive: &BtInLiveConfig{
			Cron: "0 3 * * *", Acount: "primary", MailTo: []string{"ops@example.com"},
		},
		TimeFrames: "1h,4h",
		LLMModels: map[string]*llm.LLMModelConfig{
			"primary": {
				Name: "gpt-probe", APIType: "openai", APIKey: "llm-secret", BaseURL: "https://llm.example",
				Temperature: &temperature,
				Payload:     map[string]interface{}{"response_format": map[string]interface{}{"type": "json_object"}},
			},
		},
	}

	clone := orig.Clone()
	if clone.CloseOnStuck != orig.CloseOnStuck || !clone.BTLegacyIntrabar || !clone.BTLegacyWallet ||
		clone.NTPLangCode != orig.NTPLangCode ||
		clone.ShowLangCode != orig.ShowLangCode || clone.TimeFrames != orig.TimeFrames ||
		!reflect.DeepEqual(clone.BTInLive, orig.BTInLive) || !reflect.DeepEqual(clone.LLMModels, orig.LLMModels) {
		t.Fatalf("config clone lost fields:\n got: %#v\nwant: %#v", clone, orig)
	}
	clone.BTInLive.MailTo[0] = "changed@example.com"
	clone.LLMModels["primary"].BaseURL = "https://changed.example"
	*clone.LLMModels["primary"].Temperature = 1
	clone.LLMModels["primary"].Payload["response_format"].(map[string]interface{})["type"] = "text"
	if orig.BTInLive.MailTo[0] != "ops@example.com" || orig.LLMModels["primary"].BaseURL != "https://llm.example" ||
		*orig.LLMModels["primary"].Temperature != 0.25 ||
		orig.LLMModels["primary"].Payload["response_format"].(map[string]interface{})["type"] != "json_object" {
		t.Fatal("config clone shares omitted mutable fields")
	}

	sanitized := orig.Desensitize()
	model := sanitized.LLMModels["primary"]
	if model == nil || model.APIKey != "" || model.Name != "gpt-probe" || model.BaseURL != "https://llm.example" {
		t.Fatalf("LLM model secret retained or non-sensitive fields lost: %#v", model)
	}
	if orig.LLMModels["primary"].APIKey != "llm-secret" {
		t.Fatal("desensitizing LLM config mutated the source")
	}
}

func TestDesensitizeDoesNotMutateOrShareSensitiveConfig(t *testing.T) {
	oldData := Data
	oldExchange := Exchange
	t.Cleanup(func() {
		Data = oldData
		Exchange = oldExchange
	})
	Exchange = &ExchangeConfig{Name: "binance"}

	Data = Config{
		Accounts: map[string]*AccountConfig{
			"primary": {
				StakeRate: 1.5,
				RPCChannels: []map[string]interface{}{{
					"name":    "alerts",
					"options": map[string]interface{}{"format": "compact"},
				}},
				APIServer: &AccPwdRole{Pwd: "account-password", Role: "admin"},
				Exchanges: map[string]*ExgApiSecrets{
					"binance": {
						Prod: &ApiSecretConfig{APIKey: "prod-key", APISecret: "prod-secret", Password: "prod-password"},
						Test: &ApiSecretConfig{APIKey: "test-key", APISecret: "test-secret", Password: "test-password"},
					},
				},
			},
		},
		Database: &DatabaseConfig{
			Url: "postgresql://db-user:db-password@localhost/banbot", Retention: "all", DbType: "questdb",
		},
		RPCChannels: map[string]map[string]interface{}{
			"alerts": {
				"type": "telegram", "token": "telegram-token", "chat_id": "telegram-chat",
				"options": map[string]interface{}{"format": "compact"},
			},
		},
		APIServer: &APIServerConfig{
			Enable: true, JWTSecretKey: "jwt-secret", CORSOrigins: []string{"https://dashboard.example"},
			Users: []*UserConfig{{
				Username: "operator", Password: "operator-password", AllowIPs: []string{"127.0.0.1"},
				AccRoles: map[string]string{"primary": "admin"}, ExpireHours: 24,
			}},
		},
		Mail: &MailConfig{Username: "mailer", Password: "mail-password"},
		LLMModels: map[string]*llm.LLMModelConfig{
			"primary": {Name: "gpt-probe", APIKey: "llm-secret", BaseURL: "https://llm.example"},
		},
	}

	first, err := DumpYaml(true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DumpYaml(true)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("repeated desensitized dumps differ")
	}
	for _, secret := range []string{
		"prod-key", "prod-secret", "prod-password", "test-key", "test-secret", "test-password",
		"account-password", "db-password", "telegram-token", "telegram-chat", "jwt-secret",
		"operator-password", "mail-password", "llm-secret",
	} {
		if strings.Contains(string(first), secret) {
			t.Fatalf("desensitized YAML contains %q:\n%s", secret, first)
		}
	}

	account := Data.Accounts["primary"]
	if account.GetApiSecret().APIKey != "prod-key" || account.APIServer.Pwd != "account-password" {
		t.Fatal("desensitizing mutated the live account credentials")
	}

	sanitized := Data.Desensitize()
	sanitized.Accounts["secondary"] = &AccountConfig{}
	sanitized.Accounts["primary"].StakeRate = 2
	sanitized.Accounts["primary"].RPCChannels[0]["options"].(map[string]interface{})["format"] = "verbose"
	sanitized.RPCChannels["alerts"]["options"].(map[string]interface{})["format"] = "verbose"
	sanitized.APIServer.CORSOrigins[0] = "https://changed.example"
	sanitized.APIServer.Users[0].AllowIPs[0] = "10.0.0.1"
	sanitized.APIServer.Users[0].AccRoles["primary"] = "viewer"

	if len(Data.Accounts) != 1 || account.StakeRate != 1.5 {
		t.Fatal("desensitized account map or value shares mutable state")
	}
	if got := account.RPCChannels[0]["options"].(map[string]interface{})["format"]; got != "compact" {
		t.Fatalf("desensitized account RPC config mutation reached source: %v", got)
	}
	if got := Data.RPCChannels["alerts"]["options"].(map[string]interface{})["format"]; got != "compact" {
		t.Fatalf("desensitized global RPC config mutation reached source: %v", got)
	}
	if Data.APIServer.CORSOrigins[0] != "https://dashboard.example" ||
		Data.APIServer.Users[0].AllowIPs[0] != "127.0.0.1" ||
		Data.APIServer.Users[0].AccRoles["primary"] != "admin" {
		t.Fatal("desensitized API server config shares mutable state")
	}
	if sanitized.Accounts["primary"].Exchanges != nil || sanitized.Accounts["primary"].APIServer != nil {
		t.Fatal("desensitized account still contains credentials")
	}
	if sanitized.Database.Url != "" || sanitized.Database.DbType != "questdb" {
		t.Fatal("database secret was retained or non-sensitive fields were dropped")
	}
	if sanitized.APIServer.JWTSecretKey != "" || sanitized.APIServer.Users[0].Password != "" ||
		sanitized.APIServer.Users[0].AllowIPs[0] != "10.0.0.1" {
		t.Fatal("API server secret was retained or non-sensitive fields were dropped")
	}
	if model := sanitized.LLMModels["primary"]; model == nil || model.APIKey != "" || model.BaseURL != "https://llm.example" {
		t.Fatal("LLM secret was retained or non-sensitive fields were dropped")
	}
}

func TestRuntimeAccountUpdatesDoNotRaceWithConfigDump(t *testing.T) {
	runtimeAccount := &AccountConfig{
		RPCChannels: []map[string]interface{}{{"options": map[string]interface{}{"format": "compact"}}},
		APIServer:   &AccPwdRole{Pwd: "password", Role: "admin"},
		Exchanges: map[string]*ExgApiSecrets{
			"binance": {Prod: &ApiSecretConfig{APIKey: "key", APISecret: "secret"}},
		},
	}
	runtimeAccounts := map[string]*AccountConfig{"primary": runtimeAccount}
	snapshot := &Config{Accounts: cloneAccountConfigs(runtimeAccounts)}

	snapshotAccount := snapshot.Accounts["primary"]
	if snapshotAccount == runtimeAccount || snapshotAccount.APIServer == runtimeAccount.APIServer ||
		snapshotAccount.Exchanges["binance"] == runtimeAccount.Exchanges["binance"] ||
		snapshotAccount.Exchanges["binance"].Prod == runtimeAccount.Exchanges["binance"].Prod {
		t.Fatal("config snapshot shares account secrets with runtime state")
	}
	snapshotAccount.RPCChannels[0]["options"].(map[string]interface{})["format"] = "verbose"
	if runtimeAccount.RPCChannels[0]["options"].(map[string]interface{})["format"] != "compact" {
		t.Fatal("config snapshot shares nested account RPC state")
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				runtimeAccount.NoTrade = !runtimeAccount.NoTrade
				runtimeAccount.StakePctAmt++
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		if _, err := snapshot.Desensitize().DumpYaml(); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	wg.Wait()
}
