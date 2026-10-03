package entry

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
)

// The legacy-only execution profile has no shared-account cold history reader.
// Reject an enabled output rather than silently accepting an ineffective key.
func validateLegacyHistory(spec *config.RunSpec) error {
	u := spec.Config()
	for _, policy := range u.RunPolicy {
		path, err := accountHistoryPath(spec, policyAccount(u, policy))
		if err != nil {
			return err
		}
		if path != "" {
			return errors.New("execution.history requires shared simulated replay; pure TS execution does not support cold history")
		}
	}
	return nil
}

func accountHistoryPath(spec *config.RunSpec, account string) (string, error) {
	u := spec.Config()
	field := "execution.history"
	_, exists := u.Execution["history"]
	accounts, _ := u.Execution["accounts"].(map[string]any)
	settings, _ := accounts[account].(map[string]any)
	if _, overridden := settings["history"]; overridden {
		field, exists = "execution.accounts."+account+".history", true
	}
	if !exists {
		return "", nil
	}
	return spec.ResolvePath(field)
}

func validateAccountHistoryPaths(spec *config.RunSpec, configs []runner.Config) error {
	paths := make(map[string]string)
	seenAccounts := make(map[string]bool)
	check := func(account, path string) error {
		if path == "" {
			return nil
		}
		key := filepath.Clean(path)
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if old, exists := paths[key]; exists && old != account {
			return fmt.Errorf("execution.history is shared by accounts %s and %s; configure a separate history path for each account", old, account)
		}
		paths[key] = account
		return nil
	}
	for _, c := range configs {
		seenAccounts[c.AccountID] = true
		if err := check(c.AccountID, c.Execution.HistoryPath); err != nil {
			return err
		}
	}
	u := spec.Config()
	for _, policy := range u.RunPolicy {
		account := policyAccount(u, policy)
		if policy.Engine != config.EngineTimeSeries || seenAccounts[account] {
			continue
		}
		path, err := accountHistoryPath(spec, account)
		if err != nil {
			return err
		}
		if err := check(account, path); err != nil {
			return err
		}
	}
	return nil
}
