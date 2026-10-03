package config

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
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
// CapitalWeight is an optional account budget, never an alias for StakeRate.
type PolicyV2 struct {
	*RunPolicyConfig `yaml:",inline"`
	Engine           string         `yaml:"engine,omitempty"`
	ID               string         `yaml:"id,omitempty"`
	Account          string         `yaml:"account,omitempty"`
	CapitalWeight    *float64       `yaml:"capital_weight,omitempty"`
	Factor           map[string]any `yaml:"factor,omitempty"`
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
	if p.Engine != "" && p.Engine != EngineTimeSeries {
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
		fields["factor"] = p.Factor
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
}

// YAMLImport is an in-memory candidate. It does not authorize or perform a
// file rewrite: disk migration still needs backups, conflict checks and rereads.
type YAMLImport struct {
	Original      []byte
	YAML          []byte
	SourceVersion int
	Changed       bool
}

var v2PolicyKeys = []string{"engine", "id", "account", "capital_weight", "factor"}

// ImportV1YAML minimally adds the format marker without expanding environment
// expressions, paths, aliases or defaults. Reserved legacy More collisions fail
// explicitly; their values cannot safely acquire a new meaning automatically.
func ImportV1YAML(raw []byte, path string) (*YAMLImport, error) {
	node, version, err := configDocument(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	result := &YAMLImport{Original: bytes.Clone(raw), YAML: bytes.Clone(raw), SourceVersion: version}
	if version == ConfigVersionV2 {
		return result, nil
	}
	if err := legacyReservedKeys(node); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if node != nil && node.Style&yaml.FlowStyle != 0 {
		return nil, fmt.Errorf("%s: v1 flow mapping requires an explicit reviewed conversion", path)
	}
	newline := []byte("\n")
	if bytes.Contains(raw, []byte("\r\n")) {
		newline = []byte("\r\n")
	}
	offset := 0
	if node != nil {
		for line := 1; line < node.Line; line++ {
			index := bytes.IndexByte(raw[offset:], '\n')
			if index < 0 {
				break
			}
			offset += index + 1
		}
	}
	// Keep a UTF-8 BOM at the beginning of the document.
	if offset == 0 && bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		offset = 3
	}
	var candidate []byte
	if versionNode := nodeValue(node, "config_version"); versionNode != nil {
		offset = 0
		for line := 1; line < versionNode.Line; line++ {
			offset += bytes.IndexByte(raw[offset:], '\n') + 1
		}
		offset += versionNode.Column - 1
		if offset >= len(raw) || raw[offset] != '1' {
			return nil, fmt.Errorf("%s: cannot safely replace the v1 marker", path)
		}
		candidate = bytes.Clone(raw)
		candidate[offset] = '2'
	} else {
		marker := append([]byte("config_version: 2"), newline...)
		candidate = make([]byte, 0, len(raw)+len(marker))
		candidate = append(candidate, raw[:offset]...)
		candidate = append(candidate, marker...)
		candidate = append(candidate, raw[offset:]...)
	}
	var before, after map[string]any
	if err := yaml.Unmarshal(raw, &before); err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(candidate, &after); err != nil {
		return nil, err
	}
	delete(after, "config_version")
	delete(before, "config_version")
	if len(before) == 0 && len(after) == 0 {
		before, after = nil, nil
	}
	if !reflect.DeepEqual(before, after) {
		return nil, fmt.Errorf("%s: cannot prove v1 conversion preserves all configuration values", path)
	}
	result.YAML, result.Changed = candidate, true
	return result, nil
}

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

func legacyReservedKeys(root *yaml.Node) error {
	if root == nil {
		return nil
	}
	var values map[string]any
	if err := root.Decode(&values); err != nil {
		return err
	}
	for _, key := range []string{"data", "execution"} {
		if _, exists := values[key]; exists {
			return fmt.Errorf("v1 key %q conflicts with a v2 reserved field; explicit conversion required", key)
		}
	}
	policies, _ := values["run_policy"].([]any)
	for i, item := range policies {
		policy, _ := item.(map[string]any)
		for _, key := range v2PolicyKeys {
			if _, exists := policy[key]; exists {
				return fmt.Errorf("v1 run_policy[%d].%s is a legacy More parameter and conflicts with v2; explicit conversion required", i, key)
			}
		}
	}
	return nil
}

// ParseUnifiedYAML imports a source entirely in memory and produces a v2 DTO.
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
	for i, raw := range raws {
		candidate, err := ImportV1YAML(raw, paths[i])
		if err != nil {
			return nil, errs.New(core.ErrBadConfig, err)
		}
		section, err := extractLLMSection(raw)
		if err != nil {
			return nil, errs.New(core.ErrBadConfig, err)
		}
		utils2.DeepCopyMap(llmMerged, section)
		var layer map[string]any
		if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(candidate.YAML))), &layer); err != nil {
			return nil, errs.New(core.ErrBadConfig, err)
		}
		if err := validateV2Fields(layer); err != nil {
			return nil, errs.NewFull(core.ErrBadConfig, err, "%s", paths[i])
		}
		for _, meta := range metadata {
			recordLayerOrigins(meta.origins, merged, layer, paths[i])
		}
		mergeConfigLayer(merged, layer)
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
			if err := mapstructure.Decode(fields, &extras); err != nil {
				return nil, err
			}
			if extras.Engine != "" {
				policy.Engine = extras.Engine
			}
			policy.ID, policy.Account, policy.CapitalWeight, policy.Factor = extras.ID, extras.Account, extras.CapitalWeight, extras.Factor
			for _, key := range v2PolicyKeys {
				delete(fields, key)
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
	if c == nil || c.Root == nil || c.ConfigVersion != ConfigVersionV2 {
		return fmt.Errorf("a v2 root configuration is required")
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
	if overrides, ok := c.Execution["accounts"].(map[string]any); ok {
		for name := range overrides {
			if c.Root.Accounts[name] == nil {
				return fmt.Errorf("execution.accounts.%s refers to an unknown account", name)
			}
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
		if err := validateAdvanced(fmt.Sprintf("run_policy[%d].factor", i), policy.Factor); err != nil {
			return err
		}
		for _, key := range v2PolicyKeys {
			if _, exists := policy.More[key]; exists {
				return fmt.Errorf("run_policy[%d].More.%s conflicts with a v2 field", i, key)
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
	if len(c.Data) != 0 || len(c.Execution) != 0 {
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

// MarshalYAML emits the same root keys and the single v2 strategy list.
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
	fields["config_version"] = ConfigVersionV2
	if c.RunPolicy != nil {
		fields["run_policy"] = c.RunPolicy
	}
	if c.Data != nil {
		fields["data"] = c.Data
	}
	if c.Execution != nil {
		fields["execution"] = c.Execution
	}
	return fields, nil
}

func validateTimeSeriesPolicies(policies []*RunPolicyConfig) *errs.Error {
	for i, policy := range policies {
		if policy == nil {
			return errs.NewMsg(core.ErrBadConfig, "nil run_policy[%d]", i)
		}
		if engine, ok := policy.More["engine"]; ok && engine == EngineFactor {
			return errs.NewMsg(core.ErrBadConfig, "run_policy[%d]: factor cannot run through the legacy time_series configuration path; use config_version: 2 and unified runtime assembly", i)
		}
	}
	return nil
}
