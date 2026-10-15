package runtime

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg"
)

type LegacyExecutionOptions struct {
	VenueSessionIdentity string
	SenderLeaseDir       string
}

func (o LegacyExecutionOptions) Validate() error {
	if strings.TrimSpace(o.VenueSessionIdentity) != o.VenueSessionIdentity || o.VenueSessionIdentity == "" || !filepath.IsAbs(o.SenderLeaseDir) {
		return errors.New("runtime: legacy sender requires stable session identity and absolute shared lease directory")
	}
	return nil
}

type legacySenderBinding struct {
	sender      *execution.LegacySender
	exchange    banexg.BanExchange
	options     LegacyExecutionOptions
	domains     []string
	pending     int
	established bool
}

func (p *Process) bindLegacyExchange(runtime *Runtime, exchange banexg.BanExchange, options LegacyExecutionOptions) (*execution.LegacyExchange, func(bool), error) {
	if runtime.Core.RunEnv != core.RunEnvProd || runtime.Config.View() == nil {
		return nil, nil, errors.New("runtime: legacy sender requires explicit production account configuration")
	}
	domains := map[string]bool{}
	for _, domain := range runtime.Config.View().StakeCurrency {
		domains[domain] = true
	}
	// Loaded normalized markets describe old full-key lease domains. Acquiring
	// those locks also fences an older executable for every declared currency.
	for _, market := range exchange.GetCurMarkets() {
		if market == nil {
			continue
		}
		domain := market.Settle
		if market.Spot {
			domain = market.Quote
		}
		if domain != "" {
			domains[domain] = true
		}
	}
	var settlements []string
	for domain := range domains {
		if domain == "" || strings.TrimSpace(domain) != domain {
			return nil, nil, errors.New("runtime: legacy sender settlement domains must be canonical")
		}
		settlements = append(settlements, domain)
	}
	slices.Sort(settlements)
	if len(settlements) == 0 {
		return nil, nil, errors.New("runtime: legacy sender requires declared stake or market settlement domains")
	}
	p.sharedAccountMu.Lock()
	defer p.sharedAccountMu.Unlock()
	senders := make(map[string]*execution.LegacySender)
	var borrowed []string
	finishLocked := func(success bool) {
		for _, identity := range borrowed {
			binding := p.legacySenders[identity]
			binding.pending--
			binding.established = binding.established || success
			if binding.pending == 0 && !binding.established {
				binding.sender.Close()
				delete(p.legacySenders, identity)
			}
		}
	}
	rollback := func(err error) (*execution.LegacyExchange, func(bool), error) {
		finishLocked(false)
		return nil, nil, err
	}
	for account, config := range runtime.Accounts {
		if config == nil || config.NoTrade {
			continue
		}
		key := execution.AccountKey{VenueSessionIdentity: options.VenueSessionIdentity, Account: account, SettlementDomain: settlements[0]}
		if err := key.Validate(); err != nil {
			return rollback(err)
		}
		identity := execution.SenderIdentity(key)
		for sharedKey := range p.sharedAccounts {
			if execution.SenderIdentity(sharedKey) == identity {
				return rollback(fmt.Errorf("runtime: physical account already owns shared execution"))
			}
		}
		if binding := p.legacySenders[identity]; binding != nil {
			if binding.exchange != exchange || binding.options != options || !slices.Equal(binding.domains, settlements) {
				return rollback(fmt.Errorf("runtime: physical account cannot bind a second SDK session or sender policy"))
			}
			senders[account] = binding.sender
			binding.pending++
			borrowed = append(borrowed, identity)
			continue
		}
		keeper, err := p.accountOwners.Acquire(key)
		if err != nil {
			return rollback(err)
		}
		sender, err := execution.NewLegacySender(keeper, settlements, options.SenderLeaseDir)
		if err != nil {
			keeper.Release()
			return rollback(fmt.Errorf("runtime: physical account sender lease unavailable: %w", err))
		}
		if p.legacySenders == nil {
			p.legacySenders = make(map[string]*legacySenderBinding)
		}
		p.legacySenders[identity] = &legacySenderBinding{sender: sender, exchange: exchange, options: options, domains: settlements, pending: 1}
		borrowed = append(borrowed, identity)
		senders[account] = sender
	}
	wrapped, err := execution.NewLegacyExchange(runtime.Core.Context(), exchange, runtime.defaultAccount, senders)
	if err != nil {
		return rollback(err)
	}
	finish := func(success bool) {
		p.sharedAccountMu.Lock()
		defer p.sharedAccountMu.Unlock()
		finishLocked(success)
	}
	return wrapped, finish, nil
}
