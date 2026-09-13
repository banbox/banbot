package config

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func LoadRuntimeSnapshot(args *CmdArgs) (*Snapshot, *errs.Error) {
	if args == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "command arguments are required")
	}
	input := *args
	dataDir := input.DataDir
	if dataDir == "" {
		dataDir = os.Getenv("BanDataDir")
	}
	if dataDir == "" {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime data directory is required")
	}
	dataDir, pathErr := filepath.Abs(dataDir)
	if pathErr != nil {
		return nil, errs.New(core.ErrBadConfig, pathErr)
	}
	snapshot := NewSnapshotWithDirs(nil, dataDir, os.Getenv("BanStratDir"))
	input.Configs = nil
	if !input.NoDefault {
		for _, name := range []string{"config.yml", "config.local.yml"} {
			path := filepath.Join(dataDir, name)
			if _, err := os.Stat(path); err == nil {
				input.Configs = append(input.Configs, path)
			}
		}
	}
	for _, path := range args.Configs {
		input.Configs = append(input.Configs, snapshot.ParsePath(path))
	}
	input.NoDefault, input.Inited = true, true
	if input.RawPairs != "" {
		input.Pairs = utils.SplitSolid(input.RawPairs, ",", true)
	}
	if input.RawTimeFrames != "" {
		input.TimeFrames = utils.SplitSolid(input.RawTimeFrames, ",", true)
	}
	cfg, err := GetConfig(&input, false)
	if err != nil {
		return nil, err
	}
	if err := cfg.NormalizeRuntime(); err != nil {
		return nil, err
	}
	snapshot.value = cloneSnapshotConfig(cfg)
	snapshot.location, err = input.parseTimeZone()
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *Snapshot) ParsePath(path string) string {
	if strings.HasPrefix(path, "$") || strings.HasPrefix(path, "@") {
		return filepath.Join(s.DataDir, strings.TrimLeft(path, "$@\\/"))
	}
	return path
}

func (s *Snapshot) DefaultAccount() string {
	if s == nil || s.value == nil || s.value.Env != core.RunEnvProd {
		return "default"
	}
	for _, name := range slices.Sorted(maps.Keys(s.value.Accounts)) {
		if account := s.value.Accounts[name]; account != nil && !account.NoTrade {
			return name
		}
	}
	return ""
}

func (s *Snapshot) Location() *time.Location {
	if s != nil && s.location != nil {
		return s.location
	}
	if s != nil && s.value != nil && s.value.Exchange != nil {
		if location := ExchangeDefaultLocation(s.value.Exchange.Name); location != nil {
			return location
		}
	}
	return time.UTC
}

func (c *Config) NormalizeRuntime() *errs.Error {
	if c.Exchange == nil || c.Exchange.Name == "" {
		return errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	if len(c.StakeCurrency) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "stake_currency is required")
	}
	if c.Env == "" {
		c.Env = core.RunEnvDryRun
	}
	if !banexg.IsContract(c.MarketType) {
		c.ContractType = ""
	} else if c.ContractType == "" {
		c.ContractType = banexg.MarketSwap
	}
	for _, item := range []struct {
		value    *int
		fallback int
	}{
		{&c.LimitVolSecs, 10}, {&c.PutLimitSecs, 180}, {&c.AccountPullSecs, 60},
		{&c.ConcurNum, 2}, {&c.CloseOnStuck, 20}, {&c.OrderBarMax, 500}, {&c.FatalStopHours, 8},
	} {
		if *item.value == 0 {
			*item.value = item.fallback
		}
	}
	for _, item := range []struct {
		value    *float64
		fallback float64
	}{
		{&c.MarginAddRate, 0.66}, {&c.OpenVolRate, 1}, {&c.MinOpenRate, 0.5}, {&c.BTNetCost, 15},
	} {
		if *item.value == 0 {
			*item.value = item.fallback
		}
	}
	if c.OdBookTtl == 0 {
		c.OdBookTtl = 500
	}
	if c.NTPLangCode == "" {
		c.NTPLangCode = "none"
	}
	if c.ShowLangCode == "" {
		c.ShowLangCode = "zh-CN"
	}
	if c.LowCostAction != "" {
		if _, ok := core.LowCostVals[c.LowCostAction]; !ok {
			return errs.NewMsg(core.ErrBadConfig, "invalid low_cost_action: %s", c.LowCostAction)
		}
	}
	if c.PairMgr == nil {
		c.PairMgr = &PairMgrConfig{}
	}
	if c.BTInLive == nil {
		c.BTInLive = &BtInLiveConfig{}
	}
	if c.StratPerf == nil {
		c.StratPerf = &StratPerfConfig{}
	}
	c.StratPerf.Validate()
	c.SpiderAddr = strings.ReplaceAll(c.SpiderAddr, "host.docker.internal", "127.0.0.1")
	if c.SpiderAddr == "" {
		c.SpiderAddr = "127.0.0.1:6789"
	}
	var err *errs.Error
	c.Pairs, err = parsePairs(c.Exchange.Name, c.MarketType, c.StakeCurrency, c.Pairs...)
	if err != nil {
		return err
	}
	return c.NormalizeRunPolicies()
}

// NormalizeRunPolicies prepares policy fields for an explicit runtime without
// installing configuration into package-level state.
func (c *Config) NormalizeRunPolicies() *errs.Error {
	var err *errs.Error
	counts := make(map[string]int)
	for _, policy := range c.RunPolicy {
		if policy == nil {
			return errs.NewMsg(core.ErrBadConfig, "nil run policy")
		}
		policy.Index = counts[policy.Name]
		counts[policy.Name]++
		if policy.TimeFrames != "" && len(policy.RunTimeframes) == 0 {
			policy.RunTimeframes = SplitTimeFrames(policy.TimeFrames)
		}
		policy.Pairs, err = parsePairs(c.Exchange.Name, c.MarketType, c.StakeCurrency, policy.Pairs...)
		if err != nil {
			return err
		}
		if policy.Params == nil {
			policy.Params = make(map[string]float64)
		}
		policy.defs = make(map[string]*core.Param)
		pairParams := make(map[string]map[string]float64, len(policy.PairParams))
		for pair, params := range policy.PairParams {
			pairs, parseErr := parsePairs(c.Exchange.Name, c.MarketType, c.StakeCurrency, pair)
			if parseErr != nil {
				return parseErr
			}
			pairParams[pairs[0]] = params
		}
		policy.PairParams = pairParams
	}
	return nil
}
