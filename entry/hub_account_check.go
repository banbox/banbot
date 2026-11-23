package entry

import (
	"errors"
	"fmt"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// verifyHubProductionAccount runs the legacy startup account checks for a
// production Hub account before any adapter, ledger or order path exists.
//
// Hard errors: missing trade permission (when the venue reports it), failing
// balance/access calls, and a hedge (dual-side) position mode, because the
// execution model holds one net position per instrument. Withdraw permission
// and missing IP restriction are only logged as warnings. An exchange that
// cannot report account access is treated as the legacy path treats it: the
// unknown capability does not block registration (the adapter still rejects
// hedged venue positions). Settled cash versus declared strategy capital is
// enforced by BootstrapCapital on first registration, so it is not repeated.
// Dry-run accounts have no credentials and are never checked.
func verifyHubProductionAccount(exchange banexg.BanExchange, request execution.HubAccountRegistration, credential *config.AccountConfig, exchangeName string) error {
	if request.Env != core.RunEnvProd {
		return nil
	}
	if exchange == nil {
		return errors.New("hub: account check requires an exchange session")
	}
	account := request.Account.Account
	report := live.CheckAccountWithExchange(exchange, account, nil, []string{request.Account.SettlementDomain}, false)
	secrets := hubSecretStrings(credential, exchangeName)
	errorsOut := make([]string, 0, len(report.Errors)+1)
	for _, msg := range report.Errors {
		errorsOut = append(errorsOut, redactSecrets(msg, secrets))
	}
	if report.PosMode == banexg.PosModeHedge {
		errorsOut = append(errorsOut, "position mode is hedge (dual-side); switch the account to one-way mode")
	}
	for _, warn := range report.Warns {
		log.Warn("hub account check", zap.String("acc", account), zap.String("warn", redactSecrets(warn, secrets)))
	}
	if report.PosMode == "" {
		log.Warn("hub account check", zap.String("acc", account), zap.String("warn", "position mode not reported by venue; relying on venue snapshot checks"))
	}
	if len(errorsOut) > 0 {
		return fmt.Errorf("hub: account check failed for %s: %s", account, strings.Join(errorsOut, "; "))
	}
	return nil
}

func hubSecretStrings(credential *config.AccountConfig, exchangeName string) []string {
	if credential == nil {
		return nil
	}
	secret := credential.GetApiSecretFor(exchangeName, core.RunEnvProd)
	return []string{secret.APIKey, secret.APISecret}
}

func redactSecrets(text string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[redacted]")
		}
	}
	return text
}
