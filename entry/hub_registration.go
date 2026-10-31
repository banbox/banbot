package entry

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/banbox/banbot/execution"
)

// A second robot may add contributors, but cannot replace the host credentials,
// valuation universe or account-wide safety limits of an already active owner.
func hubAdditionalRegistration(account *execution.SharedAccount, initial HubAccountConfig) func(context.Context, execution.HubAccountRegistration) error {
	return func(ctx context.Context, request execution.HubAccountRegistration) error {
		incoming, err := decodeHubAccountConfig(request)
		if err != nil {
			return err
		}
		for _, unit := range incoming.Instruments {
			if !slices.ContainsFunc(initial.Instruments, func(host execution.BanexgInstrument) bool {
				return host.Symbol == unit.Symbol && sameFactorInstrument(host.Instrument, unit.Instrument)
			}) {
				return errors.New("hub: additional robot requires host instrument expansion")
			}
		}
		baseline := func(c HubAccountConfig) ([]byte, error) {
			c.Capital, c.Instruments, c.Risk.StrategyGrossLimits = nil, nil, nil
			return json.Marshal(c)
		}
		want, err := baseline(initial)
		if err != nil {
			return err
		}
		got, err := baseline(incoming)
		if err != nil || string(want) != string(got) {
			return errors.New("hub: immutable account registration baseline mismatch")
		}
		var additions []execution.HubContributorRegistration
		var ids []string
		for _, unit := range incoming.Instruments {
			ids = append(ids, unit.Instrument.ID)
		}
		for strategy, capital := range incoming.Capital {
			if original, exists := initial.Capital[strategy]; exists {
				if !original.Equal(capital) || !initial.Risk.StrategyGrossLimits[strategy].Equal(incoming.Risk.StrategyGrossLimits[strategy]) {
					return errors.New("hub: immutable bootstrap strategy registration mismatch")
				}
				continue
			}
			additions = append(additions, execution.HubContributorRegistration{Strategy: strategy, PolicyVersion: "hub-v1", Capital: capital, GrossLimit: incoming.Risk.StrategyGrossLimits[strategy], Instruments: ids})
		}
		if len(additions) == 0 {
			return nil
		}
		slices.SortFunc(additions, func(a, b execution.HubContributorRegistration) int {
			if a.Strategy < b.Strategy {
				return -1
			}
			if a.Strategy > b.Strategy {
				return 1
			}
			return 0
		})
		borrow := account.Borrow()
		defer borrow.Release()
		return borrow.RegisterContributors(ctx, additions, time.Now().UnixMilli())
	}
}
