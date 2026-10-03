package config

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
)

// AppendImportedPolicies is used only at the legacy import boundary. Normal
// overlays still replace run_policy as a whole. Each imported path retains its
// original file base, while the resulting task consumes one immutable model.
func (s *RunSpec) AppendImportedPolicies(imported *RunSpec) (*RunSpec, error) {
	if s == nil || imported == nil || len(imported.value.RunPolicy) == 0 {
		return nil, fmt.Errorf("configuration and imported policies are required")
	}
	value := s.Config()
	for _, policy := range imported.Config().RunPolicy {
		if policy.Engine != EngineFactor {
			return nil, fmt.Errorf("legacy factor import contains a non-factor policy")
		}
		value.RunPolicy = append(value.RunPolicy, policy)
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	effective := cloneStringMap(s.effective)
	policies, _ := effective["run_policy"].([]any)
	extra, _ := imported.effective["run_policy"].([]any)
	offset := len(policies)
	for _, policy := range extra {
		policies = append(policies, cloneConfigValue(policy))
	}
	effective["run_policy"] = policies
	origins := maps.Clone(s.origins)
	for field, origin := range imported.origins {
		if !strings.HasPrefix(field, "run_policy[") {
			continue
		}
		end := strings.IndexByte(field, ']')
		index, err := strconv.Atoi(field[len("run_policy["):end])
		if err != nil {
			return nil, err
		}
		origins[fmt.Sprintf("run_policy[%d]%s", offset+index, field[end+1:])] = origin
	}
	return &RunSpec{value: value, effective: effective, origins: origins, dataDir: s.dataDir, strategyDir: s.strategyDir, args: s.args}, nil
}
