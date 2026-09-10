package config

import (
	"maps"
	"slices"
	"time"
)

// Snapshot is an owned, typed configuration copy for one Runtime. It is
// created at the boundary and should be treated as immutable afterwards.
type Snapshot struct {
	value       *Config
	DataDir     string
	StrategyDir string
	location    *time.Location
}

func NewSnapshot(cfg *Config) *Snapshot {
	return NewSnapshotWithDirs(cfg, DataDir, stratDir)
}

// NewSnapshotWithDirs creates an owned configuration snapshot with explicit
// filesystem roots. Empty directories remain empty; this constructor never
// reads the legacy package-level directory variables.
func NewSnapshotWithDirs(cfg *Config, dataDir, strategyDir string) *Snapshot {
	snapshot := &Snapshot{DataDir: dataDir, StrategyDir: strategyDir}
	if cfg == nil {
		return snapshot
	}
	snapshot.value = cloneSnapshotConfig(cfg)
	return snapshot
}

// View returns the owned config for read-only Runtime use. Callers must not
// mutate it after construction.
func (s *Snapshot) View() *Config {
	if s == nil {
		return nil
	}
	return s.value
}

func (s *Snapshot) Clone() *Snapshot {
	if s == nil {
		return &Snapshot{}
	}
	result := NewSnapshotWithDirs(s.value, s.DataDir, s.StrategyDir)
	result.location = s.location
	return result
}

func cloneSnapshotConfig(c *Config) *Config {
	res := c.Clone()
	if c.TimeRange != nil {
		res.TimeRange = c.TimeRange.Clone()
	}
	res.WalletAmounts = maps.Clone(c.WalletAmounts)
	res.StakeCurrency = slices.Clone(c.StakeCurrency)
	res.FatalStop = maps.Clone(c.FatalStop)
	res.RunTimeframes = slices.Clone(c.RunTimeframes)
	res.Pairs = slices.Clone(c.Pairs)
	res.WatchJobs = cloneWatchJobs(c.WatchJobs)
	res.RunPolicy = cloneRunPolicies(c.RunPolicy)
	res.PairFilters = clonePairFilters(c.PairFilters)
	if c.StratPerf != nil {
		item := *c.StratPerf
		res.StratPerf = &item
	}
	if c.PairMgr != nil {
		item := *c.PairMgr
		res.PairMgr = &item
	}
	if c.Database != nil {
		item := *c.Database
		res.Database = &item
	}
	if c.Exchange != nil {
		item := *c.Exchange
		item.Items = make(map[string]map[string]interface{}, len(c.Exchange.Items))
		for name, values := range c.Exchange.Items {
			item.Items[name] = cloneStringMap(values)
		}
		res.Exchange = &item
	}
	if c.APIServer != nil {
		item := *c.APIServer
		item.CORSOrigins = slices.Clone(c.APIServer.CORSOrigins)
		item.Users = make([]*UserConfig, len(c.APIServer.Users))
		for i, user := range c.APIServer.Users {
			if user == nil {
				continue
			}
			userCopy := *user
			userCopy.AllowIPs = slices.Clone(user.AllowIPs)
			userCopy.AccRoles = maps.Clone(user.AccRoles)
			item.Users[i] = &userCopy
		}
		res.APIServer = &item
	}
	if c.Mail != nil {
		item := *c.Mail
		res.Mail = &item
	}
	res.RPCChannels = cloneNestedConfigMap(c.RPCChannels)
	res.Webhook = cloneWebhook(c.Webhook)
	res.Accounts = cloneAccountConfigs(c.Accounts)
	return res
}

func cloneWatchJobs(src map[string][]string) map[string][]string {
	if src == nil {
		return nil
	}
	res := make(map[string][]string, len(src))
	for key, values := range src {
		res[key] = slices.Clone(values)
	}
	return res
}

func cloneRunPolicies(src []*RunPolicyConfig) []*RunPolicyConfig {
	if src == nil {
		return nil
	}
	res := make([]*RunPolicyConfig, len(src))
	for i, policy := range src {
		if policy != nil {
			res[i] = policy.Clone()
		}
	}
	return res
}

func clonePairFilters(src []*CommonPairFilter) []*CommonPairFilter {
	if src == nil {
		return nil
	}
	res := make([]*CommonPairFilter, len(src))
	for i, filter := range src {
		if filter != nil {
			item := *filter
			item.Items = cloneStringMap(filter.Items)
			res[i] = &item
		}
	}
	return res
}

func cloneNestedConfigMap(src map[string]map[string]interface{}) map[string]map[string]interface{} {
	if src == nil {
		return nil
	}
	res := make(map[string]map[string]interface{}, len(src))
	for key, values := range src {
		res[key] = cloneStringMap(values)
	}
	return res
}

func cloneWebhook(src map[string]map[string]string) map[string]map[string]string {
	if src == nil {
		return nil
	}
	res := make(map[string]map[string]string, len(src))
	for key, values := range src {
		res[key] = maps.Clone(values)
	}
	return res
}
