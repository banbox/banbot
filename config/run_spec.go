package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"gopkg.in/yaml.v3"
)

// FieldOrigin records the file that supplied the final value, including list
// entries and explicit null/empty replacements. Derived and CLI values are
// distinguished from file values so path bases are never guessed.
type FieldOrigin struct {
	Source string `yaml:"source"`
	Kind   string `yaml:"kind"`
}

// RunSpec owns a frozen unified model. All views and exports are copies.
type RunSpec struct {
	value                *UnifiedConfig
	effective            map[string]any
	origins              map[string]FieldOrigin
	dataDir, strategyDir string
	args                 CmdArgs
}

func (s *RunSpec) Config() *UnifiedConfig {
	if s == nil {
		return nil
	}
	return cloneConfigValue(s.value).(*UnifiedConfig)
}
func (s *RunSpec) Origin(field string) (FieldOrigin, bool) {
	if s == nil {
		return FieldOrigin{}, false
	}
	origin, ok := s.origins[field]
	return origin, ok
}
func (s *RunSpec) Origins() map[string]FieldOrigin {
	if s == nil {
		return nil
	}
	return maps.Clone(s.origins)
}
func (s *RunSpec) Engines() []string {
	var engines []string
	if s != nil {
		for _, policy := range s.value.RunPolicy {
			engines = append(engines, policy.Engine)
		}
	}
	slices.Sort(engines)
	return slices.Compact(engines)
}

// RuntimeSnapshot supplies common runtime settings and the TS subset without
// routing factor policies through legacy TS validation. Resource assembly
// still belongs to entry and must validate each engine's capabilities.
func (s *RunSpec) RuntimeSnapshot() (*Snapshot, *errs.Error) {
	if s == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "RunSpec is required")
	}
	cfg := cloneConfigValue(s.value.Root).(*Config)
	for _, policy := range s.value.RunPolicy {
		if policy.Engine == EngineTimeSeries {
			cfg.RunPolicy = append(cfg.RunPolicy, cloneConfigValue(policy.RunPolicyConfig).(*RunPolicyConfig))
		}
	}
	if err := cfg.NormalizeRuntime(); err != nil {
		return nil, err
	}
	location, err := s.args.parseTimeZone()
	if err != nil {
		return nil, err
	}
	return NewSnapshotWithDirs(cfg, s.dataDir, s.strategyDir, location), nil
}

func (s *RunSpec) EffectiveYAML(redact bool) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("RunSpec is required")
	}
	fields := cloneStringMap(s.effective)
	// An effective artifact is independently loadable from its result directory;
	// resolve advanced paths here while migrated user files stay unchanged.
	for field := range s.origins {
		if !IsAdvancedPathField(field) {
			continue
		}
		path, err := s.ResolvePath(field)
		if err != nil {
			return nil, err
		}
		setEffectiveField(fields, field, path)
	}
	if redact {
		if database, ok := fields["database"].(map[string]any); ok {
			for _, key := range []string{"url", "sid_registry_url"} {
				if _, exists := database[key]; exists {
					database[key] = "<redacted>"
				}
			}
		}
		delete(fields, "mail")
		redactConfigFields(fields)
	}
	return yaml.Marshal(fields)
}

// IsAdvancedPathField identifies paths whose base is their declaring config
// file, including normalized [] indices used by private Web snapshots.
func IsAdvancedPathField(field string) bool {
	if field == "data.archive" || field == "execution.store" || field == "execution.history" || field == "execution.sender_lease_dir" {
		return true
	}
	if strings.HasPrefix(field, "execution.accounts.") && (strings.HasSuffix(field, ".store") || strings.HasSuffix(field, ".history") || strings.HasSuffix(field, ".sender_lease_dir")) {
		return true
	}
	return strings.HasPrefix(field, "run_policy[") && (strings.HasSuffix(field, ".factor.archive") || (strings.Contains(field, ".factor.chunks[") && strings.HasSuffix(field, ".path")) || (strings.Contains(field, ".factor.config.Chunks[") && strings.HasSuffix(field, ".Path")) || strings.HasSuffix(field, ".factor.config.Execution.StorePath") || strings.HasSuffix(field, ".factor.config.Execution.HistoryPath") || strings.HasSuffix(field, ".factor.config.Execution.SenderLeaseDir"))
}

func setEffectiveField(root map[string]any, path string, value any) {
	parts := strings.Split(strings.ReplaceAll(strings.ReplaceAll(path, "[", "."), "]", ""), ".")
	var current any = root
	for i, part := range parts {
		last := i == len(parts)-1
		switch item := current.(type) {
		case map[string]any:
			if last {
				item[part] = value
				return
			}
			current = item[part]
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(item) {
				return
			}
			if last {
				item[index] = value
				return
			}
			current = item[index]
		default:
			return
		}
	}
}
func (s *RunSpec) Hash() (string, error) {
	raw, err := s.EffectiveYAML(true)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func redactConfigFields(value any) {
	switch item := value.(type) {
	case map[string]any:
		for key, child := range item {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "token") || lower == "pwd" || lower == "apikey" || lower == "api_key" || lower == "private_key" {
				item[key] = "<redacted>"
			} else {
				redactConfigFields(child)
			}
		}
	case []any:
		for _, child := range item {
			redactConfigFields(child)
		}
	}
}

// ResolvePath resolves a final advanced path against its contributing source.
// It never changes the source YAML or expands a runtime path back onto disk.
func (s *RunSpec) ResolvePath(field string) (string, error) {
	value, ok := effectiveField(s.effective, field)
	if !ok {
		return "", fmt.Errorf("%s is not configured", field)
	}
	path, ok := value.(string)
	if !ok || strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("%s must be a nonempty path", field)
	}
	if path == ":memory:" {
		return path, nil
	}
	if strings.HasPrefix(path, "$") || strings.HasPrefix(path, "@") {
		if s.dataDir == "" {
			return "", fmt.Errorf("%s requires DataDir", field)
		}
		return filepath.Join(s.dataDir, strings.TrimLeft(path, "$@\\/")), nil
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	origin, exists := s.origins[field]
	if !exists || origin.Kind == "default" {
		return "", fmt.Errorf("%s has no source path base", field)
	}
	base := s.dataDir
	if origin.Kind == "file" {
		base = filepath.Dir(origin.Source)
	}
	if base == "" {
		return filepath.Abs(path)
	}
	return filepath.Abs(filepath.Join(base, path))
}

func effectiveField(root map[string]any, path string) (any, bool) {
	parts := strings.Split(strings.ReplaceAll(strings.ReplaceAll(path, "[", "."), "]", ""), ".")
	var value any = root
	for _, part := range parts {
		switch item := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = item[part]
			if !ok {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(item) {
				return nil, false
			}
			value = item[index]
		default:
			return nil, false
		}
	}
	return value, true
}

// LoadRunSpec is the normal entry boundary for both engines. File inputs are
// migrated, while ConfigData is imported in memory without a temporary file.
func LoadRunSpec(args *CmdArgs, showLog bool) (*RunSpec, *errs.Error) {
	if args == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "command arguments are required")
	}
	input := *args
	input.ExplicitFlags = maps.Clone(args.ExplicitFlags)
	input.Configs = slices.Clone(args.Configs)
	input.Pairs, input.TimeFrames = slices.Clone(args.Pairs), slices.Clone(args.TimeFrames)
	if input.RawPairs != "" || input.ExplicitFlags["pairs"] {
		input.Pairs = utils.SplitSolid(input.RawPairs, ",", true)
	}
	if input.RawTimeFrames != "" || input.ExplicitFlags["timeframes"] {
		input.TimeFrames = utils.SplitSolid(input.RawTimeFrames, ",", true)
	}
	dir := ResolveDataDir(input.DataDir)
	if dir == "" && input.Inited {
		dir = ResolveDataDir(DataDir)
	}
	var paths []string
	if !input.NoDefault {
		if dir == "" {
			return nil, errs.NewMsg(core.ErrBadConfig, "-datadir or env BanDataDir is required")
		}
		for _, name := range []string{"config.yml", "config.local.yml"} {
			path := filepath.Join(dir, name)
			if _, err := os.Stat(path); err == nil {
				paths = append(paths, path)
			} else if !os.IsNotExist(err) {
				return nil, errs.New(core.ErrBadConfig, err)
			}
		}
	}
	for _, path := range input.Configs {
		if strings.HasPrefix(path, "$") || strings.HasPrefix(path, "@") {
			if dir == "" {
				return nil, errs.NewMsg(core.ErrBadConfig, "DataDir required for %s", path)
			}
			path = filepath.Join(dir, strings.TrimLeft(path, "$@\\/"))
		}
		paths = append(paths, path)
	}
	var inline [][]byte
	var names []string
	if input.ConfigData != "" {
		inline, names = [][]byte{[]byte(input.ConfigData)}, []string{"ConfigData"}
	}
	origins := make(map[string]FieldOrigin)
	effective := make(map[string]any)
	metadata := &loadedConfigMetadata{origins: origins, effective: effective, preflight: func(value *UnifiedConfig) error {
		return applyUnifiedArguments(cloneConfigValue(value).(*UnifiedConfig), &input)
	}}
	value, err := loadUnifiedSources(paths, inline, names, showLog, nil, metadata)
	if err != nil {
		return nil, err
	}
	if err := applyUnifiedArguments(value, &input); err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	applyCLIOrigins(effective, origins, value.Root, &input)
	for i, policy := range value.RunPolicy {
		path := fmt.Sprintf("run_policy[%d].engine", i)
		if _, ok := origins[path]; !ok {
			origins[path] = FieldOrigin{Source: "time_series", Kind: "default"}
		}
		if policies, ok := effective["run_policy"].([]any); ok {
			policies[i].(map[string]any)["engine"] = policy.Engine
		}
	}
	return &RunSpec{value: cloneConfigValue(value).(*UnifiedConfig), effective: cloneStringMap(effective), origins: origins, dataDir: dir, strategyDir: os.Getenv("BanStratDir"), args: input}, nil
}

func applyUnifiedArguments(value *UnifiedConfig, args *CmdArgs) error {
	value.Root.applyArguments(args)
	requiresRange := value.Root.TimeStart != "" || value.Root.TimeRangeRaw != "" || args.ExplicitFlags["timerange"] || args.ExplicitFlags["timestart"]
	for _, policy := range value.RunPolicy {
		requiresRange = requiresRange || policy.Engine == EngineTimeSeries
	}
	// Archive-only research uses chunk bounds and needs no TS time range.
	if requiresRange || len(value.RunPolicy) == 0 {
		return value.Root.Apply(args)
	}
	return nil
}

type loadedConfigMetadata struct {
	origins   map[string]FieldOrigin
	effective map[string]any
	preflight func(*UnifiedConfig) error
}

func recordLayerOrigins(origins map[string]FieldOrigin, merged, layer map[string]any, source string) {
	deleteField := func(key string) {
		for field := range origins {
			if field == key || strings.HasPrefix(field, key+".") || strings.HasPrefix(field, key+"[") {
				delete(origins, field)
			}
		}
	}
	if _, ok := layer["timerange"]; ok {
		deleteField("time_start")
		deleteField("time_end")
	}
	if _, ok := layer["time_start"]; ok {
		deleteField("timerange")
	}
	if _, ok := layer["time_end"]; ok {
		deleteField("timerange")
	}
	kind := "file"
	if source == "ConfigData" || source == "stdin" {
		kind = "inline"
	}
	origin := FieldOrigin{Source: source, Kind: kind}
	var walk func(string, any, any)
	walk = func(path string, value, previous any) {
		currentMap, isMap := value.(map[string]any)
		previousMap, wasMap := previous.(map[string]any)
		if !isMap || !wasMap {
			deleteField(path)
		}
		origins[path] = origin
		if isMap {
			for key, child := range currentMap {
				walk(path+"."+key, child, previousMap[key])
			}
		}
		if list, ok := value.([]any); ok {
			for i, child := range list {
				walk(fmt.Sprintf("%s[%d]", path, i), child, nil)
			}
		}
	}
	for key, value := range layer {
		previous := merged[key]
		if noExtends[key] {
			deleteField(key)
			previous = nil
		}
		walk(key, value, previous)
	}
}

func applyCLIOrigins(fields map[string]any, origins map[string]FieldOrigin, cfg *Config, args *CmdArgs) {
	set := func(field string, value any) {
		fields[field] = cloneConfigValue(value)
		origins[field] = FieldOrigin{Source: "CLI", Kind: "cli"}
	}
	if args.BTStrictSet || args.ExplicitFlags["bt-strict"] {
		set("bt_strict", cfg.BTStrict)
	}
	if args.StakeAmount > 0 || args.ExplicitFlags["stake-amount"] {
		set("stake_amount", cfg.StakeAmount)
	}
	if args.StakePct > 0 || args.ExplicitFlags["stake-pct"] {
		set("stake_pct", cfg.StakePct)
	}
	if len(args.Pairs) > 0 || args.ExplicitFlags["pairs"] {
		set("pairs", cfg.Pairs)
	}
	if len(args.TimeFrames) > 0 || args.ExplicitFlags["timeframes"] {
		set("run_timeframes", cfg.RunTimeframes)
	}
	if args.TimeRange != "" || args.ExplicitFlags["timerange"] {
		set("timerange", cfg.TimeRangeRaw)
		delete(fields, "time_start")
		delete(fields, "time_end")
		delete(origins, "time_start")
		delete(origins, "time_end")
	}
	if args.TimeStart != "" || args.ExplicitFlags["timestart"] {
		set("time_start", cfg.TimeStart)
		set("time_end", cfg.TimeEnd)
		delete(fields, "timerange")
		delete(origins, "timerange")
	}
}
