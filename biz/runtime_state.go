package biz

import "sync"

// TradingState owns the account-scoped managers and wallets for one Runtime.
// The maps are deliberately concrete: construction happens at the lifecycle
// boundary, while order processing reads a direct map entry on the hot path.
// The legacy package maps remain available only through the compatibility
// constructors and Get* facade functions.
type TradingState struct {
	OrderManagers map[string]IOrderMgr
	LiveManagers  map[string]*LiveOrderMgr
	Wallets       map[string]*BanWallets
	triggerMu     sync.Mutex

	snapshotMu  sync.Mutex
	snapshotCfg *walletSnapshotCfg
}

func NewTradingState() *TradingState {
	return &TradingState{
		OrderManagers: make(map[string]IOrderMgr),
		LiveManagers:  make(map[string]*LiveOrderMgr),
		Wallets:       make(map[string]*BanWallets),
	}
}

func (s *TradingState) ensure() {
	if s == nil {
		return
	}
	if s.OrderManagers == nil {
		s.OrderManagers = make(map[string]IOrderMgr)
	}
	if s.LiveManagers == nil {
		s.LiveManagers = make(map[string]*LiveOrderMgr)
	}
	if s.Wallets == nil {
		s.Wallets = make(map[string]*BanWallets)
	}
}

func (s *TradingState) OrderManager(account string) IOrderMgr {
	if s == nil {
		return nil
	}
	return s.OrderManagers[account]
}

func (s *TradingState) LiveManager(account string) *LiveOrderMgr {
	if s == nil {
		return nil
	}
	return s.LiveManagers[account]
}

func (s *TradingState) Wallet(account string) *BanWallets {
	if s == nil {
		return nil
	}
	s.ensure()
	wallet := s.Wallets[account]
	if wallet == nil {
		wallet = &BanWallets{Items: make(map[string]*ItemWallet), Account: account}
		s.Wallets[account] = wallet
	}
	return wallet
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
	s.OrderManagers = make(map[string]IOrderMgr)
	s.LiveManagers = make(map[string]*LiveOrderMgr)
	s.Wallets = make(map[string]*BanWallets)
	s.snapshotMu.Lock()
	s.snapshotCfg = nil
	s.snapshotMu.Unlock()
}
