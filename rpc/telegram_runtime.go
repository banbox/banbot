package rpc

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	utils2 "github.com/banbox/banbot/utils"
	"time"
)

func (t *Telegram) orders() OrderManagerInterface {
	if t.session != nil {
		return t.session.Orders
	}
	return orderManager
}

func (t *Telegram) wallets() WalletInfoProvider {
	if t.session != nil {
		return t.session.Wallets
	}
	return walletProvider
}

func (t *Telegram) accountConfigs() map[string]*config.AccountConfig {
	if t.session != nil {
		return t.session.accounts
	}
	return config.Accounts
}

func (t *Telegram) configView() *config.Config {
	if t.session != nil {
		return t.session.config
	}
	return &config.Data
}

func (t *Telegram) langMsg(code, fallback string) string {
	if t.session != nil {
		if message := t.session.messages[code]; message != "" {
			return message
		}
		return fallback
	}
	return config.GetLangMsg(code, fallback)
}

func (t *Telegram) readLangFile(name string) (string, error) {
	if t.session != nil {
		return config.ReadLangFileFrom(t.session.dataDir, t.session.config.ShowLangCode, name)
	}
	return config.ReadLangFile(config.ShowLangCode, name)
}

func (t *Telegram) nowMS() int64 {
	if t.session != nil {
		if t.session.Clock != nil {
			return t.session.Clock.TimeMS()
		}
		return time.Now().UnixMilli()
	}
	return btime.TimeMS()
}

func (t *Telegram) noEnterUntil() map[string]int64 {
	if t.session != nil {
		if t.session.Core != nil {
			return t.session.Core.NoEnterUntilSnapshot()
		}
		return nil
	}
	return core.LegacyNoEnterUntilSnapshot()
}

func (t *Telegram) noEnterUntilFor(account string) (int64, bool) {
	if t.session != nil {
		if t.session.Core != nil {
			return t.session.Core.NoEnterUntilFor(account)
		}
		return 0, false
	}
	return core.LegacyNoEnterUntilFor(account)
}

func (t *Telegram) currentDashboard() *utils2.ClientIO {
	if t.session != nil {
		return t.dashboard
	}
	return currentDashBot()
}
