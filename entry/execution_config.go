package entry

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
)

func accountExecutionFields(u *config.UnifiedConfig, account string) map[string]any {
	fields := make(map[string]any, len(u.Execution))
	for key, value := range u.Execution {
		fields[key] = value
	}
	for key, value := range u.AccountExecution[account] {
		fields[key] = value
	}
	return fields
}

// Both engine entry points resolve account overrides and path provenance here.
func applyAccountExecutionConfig(spec *config.RunSpec, account string, settingsConfig *runner.ExecutionConfig) error {
	u := spec.Config()
	fields := accountExecutionFields(u, account)
	settings := make(map[string]any)
	for key, value := range fields {
		switch key {
		case "store", "history", "sender_lease_dir", "hub_socket", "live_provider", "funding_policy", "accounts", "mode":
		default:
			settings[key] = value
		}
	}
	if err := decodeFactorFields(settings, settingsConfig); err != nil {
		return err
	}
	if err := (execution.OrderPolicy{Mode: settingsConfig.OrderMode, NearLimitBPS: settingsConfig.NearLimitBPS, ProtectionMode: settingsConfig.ProtectionMode}).Validate(); err != nil {
		return err
	}
	for key, destination := range map[string]*string{"store": &settingsConfig.StorePath, "history": &settingsConfig.HistoryPath, "sender_lease_dir": &settingsConfig.SenderLeaseDir} {
		if _, exists := fields[key]; !exists {
			continue
		}
		field := "execution." + key
		if _, overridden := u.AccountExecution[account][key]; overridden {
			field = "accounts." + account + "." + key
		}
		path, err := spec.ResolvePath(field)
		if err != nil {
			return err
		}
		*destination = path
	}
	return nil
}
