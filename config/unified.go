package config

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/llm"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/go-viper/mapstructure/v2"
	"gopkg.in/yaml.v3"
)

const (
	ConfigVersionV2  = 2
	EngineTimeSeries = "time_series"
	EngineFactor     = "factor"
)

// PolicyV2 keeps the existing strategy fields and their sizing semantics.
// Its historical type name does not require a version marker in YAML.
// CapitalWeight is an optional account budget, never an alias for StakeRate.
type PolicyV2 struct {
	*RunPolicyConfig `yaml:",inline"`
	Engine           string         `yaml:"engine,omitempty"`
	ID               string         `yaml:"id,omitempty"`
	Account          string         `yaml:"account,omitempty"`
	CapitalWeight    *float64       `yaml:"capital_weight,omitempty"`
	Factor           map[string]any `yaml:"factor,omitempty"`
	ExplicitEngine   bool           `yaml:"-"`
}

// MarshalYAML explicitly keeps the legacy inline More map. yaml.v3 does not
// promote an inline map nested inside another inline struct automatically.
func (p *PolicyV2) MarshalYAML() (any, error) {
	if p == nil || p.RunPolicyConfig == nil {
		return nil, fmt.Errorf("policy configuration is required")
	}
	raw, err := yaml.Marshal(p.RunPolicyConfig)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := yaml.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if p.Engine != "" && (p.Engine != EngineTimeSeries || p.ExplicitEngine || p.ID != "" || p.Account != "" || p.CapitalWeight != nil) {
		fields["engine"] = p.Engine
	}
	if p.ID != "" {
		fields["id"] = p.ID
	}
	if p.Account != "" {
		fields["account"] = p.Account
	}
	if p.CapitalWeight != nil {
		fields["capital_weight"] = *p.CapitalWeight
	}
	if p.Factor != nil {
		for key, value := range p.Factor {
			if _, exists := fields[key]; !exists {
				fields[key] = value
			}
		}
	}
	return fields, nil
}

// UnifiedConfig is a pure configuration boundary, not an assembled Runtime.
// Root holds existing root keys; its RunPolicy is empty. The one authoritative
// strategy list is RunPolicy here. Engine adapters must validate advanced
// overrides and resolve their paths using the source file before execution.
type UnifiedConfig struct {
	ConfigVersion int
	Root          *Config
	RunPolicy     []*PolicyV2
	Data          map[string]any
	Execution     map[string]any
	// AccountExecution contains factor execution overrides keyed by the
	// existing root account name. Account credentials remain in Root.Accounts.
	AccountExecution map[string]map[string]any
}

var v2PolicyKeys = []string{"engine", "id", "account", "capital_weight", "factor"}

func configDocument(raw []byte) (*yaml.Node, int, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var doc yaml.Node
	if err := decoder.Decode(&doc); err != nil && err != io.EOF {
		return nil, 0, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("configuration must contain one YAML document")
	}
	if len(doc.Content) == 0 {
		return nil, 1, nil
	}
	root := doc.Content[0]
	if root.Tag == "!!null" {
		return nil, 1, nil
	}
	if root.Kind != yaml.MappingNode {
		return nil, 0, fmt.Errorf("configuration root must be a mapping")
	}
	// Node decoding retains syntax; map decoding additionally rejects duplicates.
	var values map[string]any
	if err := root.Decode(&values); err != nil {
		return nil, 0, err
	}
	version := nodeValue(root, "config_version")
	if version == nil {
		if _, exists := values["config_version"]; exists {
			return nil, 0, fmt.Errorf("config_version must be an explicit root scalar, not a YAML merge")
		}
		return root, 1, nil
	}
	if version.Kind != yaml.ScalarNode || version.Tag != "!!int" || (version.Value != "1" && version.Value != "2") {
		return nil, 0, fmt.Errorf("config_version must be the integer 1 or 2")
	}
	if version.Value == "1" {
		return root, 1, nil
	}
	return root, ConfigVersionV2, nil
}

func nodeValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil {
		return nil
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			return mapping.Content[i+1]
		}
	}
	return nil
}

// ParseUnifiedYAML parses legacy and explicit-engine sources in memory.
// It never installs globals, opens services, or rewrites the source file.
func ParseUnifiedYAML(raw []byte, path string) (*UnifiedConfig, *errs.Error) {
	return parseUnifiedLayers([][]byte{raw}, []string{path})
}

// ParseUnifiedConfigs uses the existing file order and whole-block replacement
// rules. Unsupported source versions are checked before overlays can hide them.
func ParseUnifiedConfigs(paths []string, showLog bool) (*UnifiedConfig, *errs.Error) {
	raws := make([][]byte, len(paths))
	for i, path := range paths {
		if showLog {
			log.Info("Using " + path)
		}
		resolved := path
		if strings.HasPrefix(path, "$") || strings.HasPrefix(path, "@") {
			dataDir := DataDir
			if dataDir == "" {
				dataDir = ResolveDataDir("")
			}
			if dataDir == "" {
				return nil, errs.NewMsg(core.ErrBadConfig, "DataDir is required to resolve %s", path)
			}
			resolved = filepath.Join(dataDir, strings.TrimLeft(path, "$@\\/"))
		}
		raw, err := os.ReadFile(resolved)
		if err != nil {
			return nil, errs.NewFull(core.ErrIOReadFail, err, "Read %s Fail", path)
		}
		raws[i] = raw
	}
	return parseUnifiedLayers(raws, paths)
}

func parseUnifiedLayers(raws [][]byte, paths []string, metadata ...*loadedConfigMetadata) (*UnifiedConfig, *errs.Error) {
	merged := make(map[string]any)
	llmMerged := make(map[string]any)
	declaredAccounts, aliasedAccounts := make(map[string]bool), make(map[string]bool)
	for i, raw := range raws {
		_, version, err := configDocument(raw)
		if err != nil {
			return nil, errs.NewFull(core.ErrBadConfig, err, "%s", paths[i])
		}
		section, err := extractLLMSection(raw)
		if err != nil {
			return nil, errs.New(core.ErrBadConfig, err)
		}
		utils2.DeepCopyMap(llmMerged, section)
		var layer map[string]any
		if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), &layer); err != nil {
			return nil, errs.New(core.ErrBadConfig, err)
		}
		if accounts, ok := layer["accounts"].(map[string]any); ok {
			for name := range accounts {
				declaredAccounts[name] = true
			}
		}
		aliases, err := normalizeConfigLayer(layer, version == ConfigVersionV2)
		if err != nil {
			return nil, errs.NewFull(core.ErrBadConfig, err, "%s", paths[i])
		}
		for _, name := range aliases {
			aliasedAccounts[name] = true
		}
		if err := validateV2Fields(layer); err != nil {
			return nil, errs.NewFull(core.ErrBadConfig, err, "%s", paths[i])
		}
		for _, meta := range metadata {
			recordLayerOrigins(meta.origins, merged, layer, paths[i])
		}
		mergeConfigLayer(merged, layer)
	}
	for name := range aliasedAccounts {
		if !declaredAccounts[name] {
			return nil, errs.NewMsg(core.ErrBadConfig, "execution.accounts.%s refers to an unknown account", name)
		}
	}
	result, err := decodeUnified(merged)
	if err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	if err := applyLLMConfig(llmMerged, result.Root); err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	if err := llm.ResolveModels(result.Root.LLMModels); err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	if err := result.Validate(); err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	for _, meta := range metadata {
		for key, value := range merged {
			meta.effective[key] = cloneConfigValue(value)
		}
	}
	return result, nil
}

func mergeConfigLayer(merged, layer map[string]any) {
	if _, ok := layer["timerange"]; ok {
		delete(merged, "time_start")
		delete(merged, "time_end")
	} else if _, start := layer["time_start"]; start {
		delete(merged, "timerange")
	} else if _, end := layer["time_end"]; end {
		delete(merged, "timerange")
	}
	for key := range noExtends {
		if _, ok := layer[key]; ok {
			delete(merged, key)
		}
	}
	utils2.DeepCopyMap(merged, layer)
}

func validateV2Fields(layer map[string]any) error {
	for _, key := range []string{"data", "execution"} {
		if value, exists := layer[key]; exists && value != nil {
			fields, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("%s must be a mapping", key)
			}
			if err := validateAdvanced(key, fields); err != nil {
				return err
			}
		}
	}
	if accounts, ok := layer["accounts"].(map[string]any); ok {
		for name, raw := range accounts {
			fields, _ := raw.(map[string]any)
			overrides := make(map[string]any)
			for _, key := range accountExecutionKeys {
				if value, exists := fields[key]; exists {
					overrides[key] = value
				}
			}
			if err := validateAdvanced("accounts."+name, overrides); err != nil {
				return err
			}
		}
	}
	value := layer["run_policy"]
	if value == nil {
		return nil
	}
	policies, ok := value.([]any)
	if !ok {
		return fmt.Errorf("run_policy must be a list or null")
	}
	for i, value := range policies {
		policy, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("run_policy[%d] must be a mapping", i)
		}
		if !hasPolicyEngine(policy) {
			continue
		}
		for _, key := range []string{"engine", "id", "account"} {
			if value, exists := policy[key]; exists {
				text, ok := value.(string)
				if !ok || strings.TrimSpace(text) == "" {
					return fmt.Errorf("run_policy[%d].%s must be a nonempty string", i, key)
				}
			}
		}
		if value, exists := policy["engine"]; exists && value != EngineTimeSeries && value != EngineFactor {
			return fmt.Errorf("run_policy[%d]: unsupported engine %q", i, value)
		}
		if value, exists := policy["capital_weight"]; exists {
			switch value.(type) {
			case int, uint64, float64:
			default:
				return fmt.Errorf("run_policy[%d].capital_weight must be a number", i)
			}
		}
		if value, exists := policy["factor"]; exists {
			fields, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("run_policy[%d].factor must be a mapping", i)
			}
			if err := validateAdvanced(fmt.Sprintf("run_policy[%d].factor", i), fields); err != nil {
				return err
			}
			return fmt.Errorf("run_policy[%d]: factor overrides require engine: factor", i)
		}
		if err := validateAdvanced(fmt.Sprintf("run_policy[%d]", i), factorFields(policy)); err != nil {
			return err
		}
	}
	return nil
}

func decodeUnified(merged map[string]any) (*UnifiedConfig, error) {
	rootValues := cloneStringMap(merged)
	delete(rootValues, "config_version")
	delete(rootValues, "run_policy")
	delete(rootValues, "data")
	delete(rootValues, "execution")
	result := &UnifiedConfig{ConfigVersion: ConfigVersionV2, Root: &Config{}}
	result.AccountExecution = make(map[string]map[string]any)
	if accounts, ok := rootValues["accounts"].(map[string]any); ok {
		for name, raw := range accounts {
			fields, _ := raw.(map[string]any)
			overrides := make(map[string]any)
			for _, key := range accountExecutionKeys {
				if value, exists := fields[key]; exists {
					overrides[key] = value
					delete(fields, key)
				}
			}
			if len(overrides) > 0 {
				result.AccountExecution[name] = overrides
			}
		}
	}
	if err := mapstructure.Decode(rootValues, result.Root); err != nil {
		return nil, err
	}
	result.Data, _ = merged["data"].(map[string]any)
	result.Execution, _ = merged["execution"].(map[string]any)
	if policies, ok := merged["run_policy"].([]any); ok {
		result.RunPolicy = make([]*PolicyV2, 0, len(policies))
		for _, value := range policies {
			fields := cloneStringMap(value.(map[string]any))
			policy := &PolicyV2{RunPolicyConfig: &RunPolicyConfig{}, Engine: EngineTimeSeries}
			var extras struct {
				Engine, ID, Account string
				CapitalWeight       *float64 `mapstructure:"capital_weight"`
				Factor              map[string]any
			}
			if hasPolicyEngine(fields) {
				if err := mapstructure.Decode(fields, &extras); err != nil {
					return nil, err
				}
				policy.Engine, policy.ExplicitEngine = extras.Engine, true
				policy.ID, policy.Account, policy.CapitalWeight = extras.ID, extras.Account, extras.CapitalWeight
				policy.Factor = factorFields(fields)
				for key := range policy.Factor {
					delete(fields, key)
				}
				for _, key := range v2PolicyKeys {
					delete(fields, key)
				}
			}
			if err := mapstructure.Decode(fields, policy.RunPolicyConfig); err != nil {
				return nil, err
			}
			result.RunPolicy = append(result.RunPolicy, policy)
		}
	}
	return result, nil
}

// Validate checks only configuration semantics. Market, strategy-definition,
// data-source and execution capability validation belong to their adapters.
func (c *UnifiedConfig) Validate() error {
	if c == nil || c.Root == nil {
		return fmt.Errorf("a root configuration is required")
	}
	if len(c.Root.RunPolicy) != 0 {
		return fmt.Errorf("UnifiedConfig.RunPolicy is the sole strategy list; Root.RunPolicy must be empty")
	}
	if err := validateAdvanced("data", c.Data); err != nil {
		return err
	}
	if err := validateAdvanced("execution", c.Execution); err != nil {
		return err
	}
	for name := range c.AccountExecution {
		if c.Root.Accounts[name] == nil {
			return fmt.Errorf("account execution override %q refers to an unknown account", name)
		}
		if err := validateAdvanced("accounts."+name, c.AccountExecution[name]); err != nil {
			return err
		}
	}
	groups := make(map[string][]*PolicyV2)
	ids := make(map[string]bool)
	for i, policy := range c.RunPolicy {
		if policy == nil || policy.RunPolicyConfig == nil || strings.TrimSpace(policy.Name) == "" {
			return fmt.Errorf("run_policy[%d] requires a strategy name", i)
		}
		if policy.Engine != EngineTimeSeries && policy.Engine != EngineFactor {
			return fmt.Errorf("run_policy[%d]: unsupported engine %q", i, policy.Engine)
		}
		if err := validateAdvanced(fmt.Sprintf("run_policy[%d]", i), policy.Factor); err != nil {
			return err
		}
		if policy.ExplicitEngine {
			for _, key := range v2PolicyKeys {
				if _, exists := policy.More[key]; exists {
					return fmt.Errorf("run_policy[%d].More.%s conflicts with an engine field", i, key)
				}
			}
		}
		if policy.Engine != EngineFactor && policy.Factor != nil {
			return fmt.Errorf("run_policy[%d]: factor overrides require engine: factor", i)
		}
		if policy.ID != "" {
			if ids[policy.ID] {
				return fmt.Errorf("duplicate strategy id %q", policy.ID)
			}
			ids[policy.ID] = true
		}
		if policy.CapitalWeight != nil && (math.IsNaN(*policy.CapitalWeight) || math.IsInf(*policy.CapitalWeight, 0) || *policy.CapitalWeight < 0 || *policy.CapitalWeight > 1) {
			return fmt.Errorf("run_policy[%d].capital_weight must be finite and between 0 and 1", i)
		}
		accounts, err := c.policyAccounts(policy)
		if err != nil {
			return fmt.Errorf("run_policy[%d]: %w", i, err)
		}
		for _, account := range accounts {
			groups[account] = append(groups[account], policy)
		}
	}
	for account, policies := range groups {
		enabled := false
		for _, policy := range policies {
			enabled = enabled || policy.Engine == EngineFactor || policy.CapitalWeight != nil
		}
		if !enabled {
			continue
		} // Pure TS keeps legacy stake sizing, even with many strategies.
		sum := new(big.Rat)
		for _, policy := range policies {
			if policy.CapitalWeight == nil {
				if len(policies) > 1 {
					return fmt.Errorf("account %q: all participating strategies require explicit capital_weight", account)
				}
				sum.Add(sum, big.NewRat(1, 1)) // One budgeted strategy defaults to available capital without mutating the DTO.
			} else {
				weight, _ := new(big.Rat).SetString(strconv.FormatFloat(*policy.CapitalWeight, 'f', -1, 64))
				sum.Add(sum, weight)
			}
		}
		if sum.Cmp(big.NewRat(1, 1)) > 0 {
			return fmt.Errorf("account %q: capital_weight sum exceeds 1", account)
		}
	}
	return nil
}

func (c *UnifiedConfig) policyAccounts(policy *PolicyV2) ([]string, error) {
	if policy.Account != "" {
		account := c.Root.Accounts[policy.Account]
		if account == nil || account.NoTrade {
			return nil, fmt.Errorf("account %q is missing or not tradable", policy.Account)
		}
		return []string{policy.Account}, nil
	}
	var accounts []string
	for name, account := range c.Root.Accounts {
		if account != nil && !account.NoTrade {
			accounts = append(accounts, name)
		}
	}
	slices.Sort(accounts)
	if c.Root.Env != core.RunEnvProd && len(accounts) > 0 {
		if slices.Contains(accounts, "default") {
			return []string{"default"}, nil
		}
		return accounts[:1], nil
	}
	if len(accounts) == 0 {
		return []string{"default"}, nil
	} // Credential validation is outside the pure DTO.
	return accounts, nil
}

// TimeSeriesConfig is the explicit compatibility projection. It refuses any
// new execution semantics that the legacy TS path cannot honor.
func (c *UnifiedConfig) TimeSeriesConfig() (*Config, *errs.Error) {
	if err := c.Validate(); err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	if len(c.Data) != 0 || len(c.Execution) != 0 || len(c.AccountExecution) != 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "advanced data/execution overrides require unified runtime assembly")
	}
	result := cloneConfigValue(c.Root).(*Config)
	if c.RunPolicy != nil {
		result.RunPolicy = make([]*RunPolicyConfig, 0, len(c.RunPolicy))
	}
	for _, policy := range c.RunPolicy {
		if policy.Engine != EngineTimeSeries {
			return nil, errs.NewMsg(core.ErrBadConfig, "engine %q requires unified runtime assembly; cannot run as time_series", policy.Engine)
		}
		if policy.CapitalWeight != nil || policy.Account != "" || policy.ID != "" {
			return nil, errs.NewMsg(core.ErrBadConfig, "capital_weight/account/id require unified runtime assembly")
		}
		result.RunPolicy = append(result.RunPolicy, cloneConfigValue(policy.RunPolicyConfig).(*RunPolicyConfig))
	}
	return result, nil
}

// MarshalYAML emits existing root keys and one shallow strategy list.
func (c *UnifiedConfig) MarshalYAML() (any, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	rootBytes, err := yaml.Marshal(c.Root)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := yaml.Unmarshal(rootBytes, &fields); err != nil {
		return nil, err
	}
	if c.Root.LLMModels == nil {
		delete(fields, "llm_models")
	}
	if c.RunPolicy != nil {
		fields["run_policy"] = c.RunPolicy
	}
	if c.Data != nil {
		fields["data"] = c.Data
	}
	if c.Execution != nil {
		execution := cloneStringMap(c.Execution)
		if execution == nil {
			execution = make(map[string]interface{})
		}
		fields["execution"] = execution
	}
	if len(c.AccountExecution) > 0 {
		accounts, _ := fields["accounts"].(map[string]any)
		if accounts == nil {
			accounts = make(map[string]any)
		}
		for name, override := range c.AccountExecution {
			account, _ := accounts[name].(map[string]any)
			if account == nil {
				account = make(map[string]any)
			}
			for key, value := range override {
				account[key] = value
			}
			accounts[name] = account
		}
		fields["accounts"] = accounts
	}
	return fields, nil
}

func validateTimeSeriesPolicies(policies []*RunPolicyConfig) *errs.Error {
	for i, policy := range policies {
		if policy == nil {
			return errs.NewMsg(core.ErrBadConfig, "nil run_policy[%d]", i)
		}
		if engine, ok := policy.More["engine"]; ok && engine == EngineFactor {
			return errs.NewMsg(core.ErrBadConfig, "run_policy[%d]: factor requires unified runtime assembly", i)
		}
	}
	return nil
}
