package config

import (
	"fmt"
	"strings"
)

// Only an explicit engine opts a strategy into identity/budget semantics.
// Versionless v0.5 policies keep their open More map, including same-name keys.
func hasPolicyEngine(fields map[string]any) bool {
	return fields["engine"] == EngineTimeSeries || fields["engine"] == EngineFactor
}

var accountExecutionKeys = strings.Fields("mode store history sender_lease_dir live_provider funding_policy instruments margin_rate max_account_margin max_virtual_gross strategy_gross_limit")
var factorPolicyKeys = strings.Fields("archive chunks snapshot combo portfolio decision research manifest prices funding_source initial_nav max_records definition expressions")

func factorFields(fields map[string]any) map[string]any {
	if fields["engine"] != EngineFactor {
		return nil
	}
	result := make(map[string]any)
	for _, key := range factorPolicyKeys {
		if value, exists := fields[key]; exists {
			result[key] = value
		}
	}
	return result
}

// Normalize each layer before merging and recording origins. Old v0.6 aliases
// are accepted at this boundary; all consumers and exports use shallow paths.
func normalizeConfigLayer(layer map[string]any, marked bool) ([]string, error) {
	delete(layer, "config_version")
	// v0.5's published template used pwd: 123. Preserve that spelling on disk
	// while decoding the password as text, without weakening other field types.
	if server, ok := layer["api_server"].(map[string]any); ok {
		users, _ := server["users"].([]any)
		for _, raw := range users {
			user, _ := raw.(map[string]any)
			normalizePassword(user)
		}
	}
	if accounts, ok := layer["accounts"].(map[string]any); ok {
		for _, raw := range accounts {
			account, _ := raw.(map[string]any)
			server, _ := account["api_server"].(map[string]any)
			normalizePassword(server)
		}
	}
	var aliases []string
	if execution, ok := layer["execution"].(map[string]any); ok {
		if raw, exists := execution["accounts"]; exists {
			overrides, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("execution.accounts must be a mapping")
			}
			accounts, ok := layer["accounts"].(map[string]any)
			if layer["accounts"] != nil && !ok {
				return nil, fmt.Errorf("accounts must be a mapping")
			}
			if accounts == nil {
				accounts = make(map[string]any)
			}
			for name, rawFields := range overrides {
				fields, ok := rawFields.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("execution.accounts.%s must be a mapping", name)
				}
				if err := validateAdvanced("execution.accounts."+name, fields); err != nil {
					return nil, err
				}
				account, _ := accounts[name].(map[string]any)
				if existing, exists := accounts[name]; exists && existing != nil && account == nil {
					return nil, fmt.Errorf("accounts.%s must be a mapping", name)
				}
				if account == nil {
					account = make(map[string]any)
				}
				for key, value := range fields {
					if _, exists := account[key]; exists {
						return nil, fmt.Errorf("accounts.%s.%s is also set in execution.accounts", name, key)
					}
					account[key] = value
				}
				accounts[name] = account
				aliases = append(aliases, name)
			}
			layer["accounts"] = accounts
			delete(execution, "accounts")
		}
	}
	policies, _ := layer["run_policy"].([]any)
	for i, raw := range policies {
		policy, ok := raw.(map[string]any)
		if !ok {
			continue // validateV2Fields reports the malformed list entry.
		}
		if marked {
			if _, exists := policy["engine"]; !exists {
				policy["engine"] = EngineTimeSeries
			}
			if !hasPolicyEngine(policy) {
				return nil, fmt.Errorf("run_policy[%d]: unsupported engine %v", i, policy["engine"])
			}
		}
		if policy["engine"] == EngineFactor {
			if _, exists := policy["config"]; exists {
				return nil, fmt.Errorf("run_policy[%d].config: unknown advanced field", i)
			}
			if raw, exists := policy["factor"]; exists {
				fields, ok := raw.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("run_policy[%d].factor must be a mapping", i)
				}
				if err := validateAdvanced(fmt.Sprintf("run_policy[%d]", i), fields); err != nil {
					return nil, err
				}
				for key, value := range fields {
					if _, exists := policy[key]; exists {
						return nil, fmt.Errorf("run_policy[%d].%s is also set in factor", i, key)
					}
					policy[key] = value
				}
				delete(policy, "factor")
			}
		}
	}
	return aliases, nil
}

func normalizePassword(fields map[string]any) {
	switch fields["pwd"].(type) {
	case int, int64, uint64:
		fields["pwd"] = fmt.Sprint(fields["pwd"])
	}
}
