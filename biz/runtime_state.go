package biz

import "sync"

// TradingState owns the account-scoped managers and wallets for one Runtime.
// The registries remain concrete maps behind the state so lookup and lazy
// writes use one per-runtime lock. The legacy package maps remain available
// only through compatibility facades.
type TradingState struct {
	orderManagers map[string]IOrderMgr
	liveManagers  map[string]*LiveOrderMgr
	wallets       map[string]*BanWallets
	registryMu    sync.RWMutex
	triggerMu     sync.Mutex

	snapshotMu  sync.Mutex
	snapshotCfg *walletSnapshotCfg
}

func NewTradingState() *TradingState {
	return &TradingState{
		orderManagers: make(map[string]IOrderMgr),
		liveManagers:  make(map[string]*LiveOrderMgr),
		wallets:       make(map[string]*BanWallets),
	}
}

func (s *TradingState) ensure() {
	if s == nil {
		return
	}
	s.registryMu.Lock()
	s.ensureLocked()
	s.registryMu.Unlock()
}

func (s *TradingState) ensureLocked() {
	if s.orderManagers == nil {
		s.orderManagers = make(map[string]IOrderMgr)
	}
	if s.liveManagers == nil {
		s.liveManagers = make(map[string]*LiveOrderMgr)
	}
	if s.wallets == nil {
		s.wallets = make(map[string]*BanWallets)
	}
}

func (s *TradingState) OrderManager(account string) IOrderMgr {
	if s == nil {
		return nil
	}
	s.registryMu.RLock()
	manager := s.orderManagers[account]
	s.registryMu.RUnlock()
	return manager
}

func (s *TradingState) LiveManager(account string) *LiveOrderMgr {
	if s == nil {
		return nil
	}
	s.registryMu.RLock()
	manager := s.liveManagers[account]
	s.registryMu.RUnlock()
	return manager
}

func (s *TradingState) Wallet(account string) *BanWallets {
	if s == nil {
		return nil
	}
	s.registryMu.Lock()
	s.ensureLocked()
	wallet := s.wallets[account]
	if wallet == nil {
		wallet = &BanWallets{Items: make(map[string]*ItemWallet), Account: account}
		s.wallets[account] = wallet
	}
	s.registryMu.Unlock()
	return wallet
}

// SetOrderManager binds one account manager while preserving the concrete
// registry used by the hot path. A nil manager removes the account entry.
func (s *TradingState) SetOrderManager(account string, manager IOrderMgr) {
	if s == nil {
		return
	}
	s.registryMu.Lock()
	s.ensureLocked()
	if manager == nil {
		delete(s.orderManagers, account)
	} else {
		s.orderManagers[account] = manager
	}
	s.registryMu.Unlock()
}

// SetLiveManager binds one account's live manager. A nil manager removes the
// account entry.
func (s *TradingState) SetLiveManager(account string, manager *LiveOrderMgr) {
	if s == nil {
		return
	}
	s.registryMu.Lock()
	s.ensureLocked()
	if manager == nil {
		delete(s.liveManagers, account)
	} else {
		s.liveManagers[account] = manager
	}
	s.registryMu.Unlock()
}

// OrderManagersSnapshot returns a typed copy for lifecycle operations that
// need to iterate the registry without holding the state lock while running a
// manager callback.
func (s *TradingState) OrderManagersSnapshot() map[string]IOrderMgr {
	if s == nil {
		return nil
	}
	s.registryMu.RLock()
	result := make(map[string]IOrderMgr, len(s.orderManagers))
	for account, manager := range s.orderManagers {
		result[account] = manager
	}
	s.registryMu.RUnlock()
	return result
}

// LiveManagersSnapshot returns a typed copy of the live manager registry.
func (s *TradingState) LiveManagersSnapshot() map[string]*LiveOrderMgr {
	if s == nil {
		return nil
	}
	s.registryMu.RLock()
	result := make(map[string]*LiveOrderMgr, len(s.liveManagers))
	for account, manager := range s.liveManagers {
		result[account] = manager
	}
	s.registryMu.RUnlock()
	return result
}

// WalletsSnapshot returns a typed copy of the wallet registry.
func (s *TradingState) WalletsSnapshot() map[string]*BanWallets {
	if s == nil {
		return nil
	}
	s.registryMu.RLock()
	result := make(map[string]*BanWallets, len(s.wallets))
	for account, wallet := range s.wallets {
		result[account] = wallet
	}
	s.registryMu.RUnlock()
	return result
}

func (s *TradingState) snapshotConfig() *walletSnapshotCfg {
	if s == nil {
		return nil
	}
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if s.snapshotCfg == nil {
		s.snapshotCfg = newWalletSnapshotCfg()
	}
	return s.snapshotCfg
}

// Reset releases all account-scoped managers and wallets owned by this
// runtime. Callers must have joined their lifecycle callbacks first.
func (s *TradingState) Reset() {
	if s == nil {
		return
	}
	s.registryMu.Lock()
	s.orderManagers = make(map[string]IOrderMgr)
	s.liveManagers = make(map[string]*LiveOrderMgr)
	s.wallets = make(map[string]*BanWallets)
	s.registryMu.Unlock()
	s.snapshotMu.Lock()
	s.snapshotCfg = nil
	s.snapshotMu.Unlock()
}
