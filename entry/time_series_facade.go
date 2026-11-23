package entry

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
)

// tsHubLocalBridge builds the Hub-facing bridge of an embedded production TS
// robot. The robot journal lives beside the default Hub
// robots' but under a distinct "ts-local-" name: it records commands against
// THIS embedded account's epochs and must never be reloaded by a socket Hub
// robot of the same identity, nor the other way round.
func tsHubLocalBridge(snapshot *config.Snapshot, account execution.AccountKey, bridge biz.SharedOrderBridgeConfig, port *execution.LocalHubPort) (*biz.HubOrderBridgeConfig, error) {
	if snapshot == nil || port == nil || len(bridge.Strategies) == 0 {
		return nil, errors.New("time-series execution: local Hub port and explicit strategy bridge required")
	}
	var strategies []string
	for _, strategy := range bridge.Strategies {
		strategies = append(strategies, string(strategy.ID))
	}
	slices.Sort(strategies)
	identity := fmt.Sprintf("%s/%s/%s/%q", account.VenueSessionIdentity, account.Account, account.SettlementDomain, strategies)
	path := filepath.Join(snapshot.DataDir, "execution", "robots", fmt.Sprintf("ts-local-%x.json", sha256.Sum256([]byte(identity))))
	return &biz.HubOrderBridgeConfig{Client: port, Account: account, Bridge: bridge, WriterID: fmt.Sprintf("ts-local-%x", sha256.Sum256([]byte(path))), StatePath: path, LocalAccount: true}, nil
}
