package orm

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/banbox/banbot/config"
)

// SIDAllocator coordinates symbol IDs for runtimes that share one storage
// identity. It is deliberately separate from SymbolState: resetting a runtime
// must not make a later runtime reuse an ID.
type SIDAllocator struct {
	identityMu      sync.RWMutex
	namespace       string
	reservationRoot string
	legacyConfig    bool
	max             atomic.Int32
	registryMu      sync.RWMutex
	registry        *SymbolSIDRegistry

	ensureMu            sync.Mutex
	reserveMu           sync.RWMutex
	reservations        map[string]int32
	pendingReservations map[string]int32
	reservationKeys     map[int32]string
	rootMu              sync.RWMutex
	recoveryRoots       map[string]struct{}
}

func NewSIDAllocator() *SIDAllocator {
	allocator := newSIDAllocator(defaultSIDAllocatorNamespace(), config.GetDataDirSafe())
	allocator.legacyConfig = true
	return allocator
}

// NewSIDAllocatorForNamespace creates an allocator whose reservations are
// owned by one storage namespace. Its process lease and pending ledger are
// derived from that namespace, not from the current runtime DataDir.
func NewSIDAllocatorForNamespace(namespace string) *SIDAllocator {
	return newSIDAllocator(namespace, config.GetDataDirSafe())
}

// NewSIDAllocatorForStorage binds an allocator to the Runtime's immutable
// storage snapshot instead of package-global configuration. dataDir remains
// the recovery-marker root; the SID coordination root is identity-derived.
func NewSIDAllocatorForStorage(namespace, dataDir string) *SIDAllocator {
	return newSIDAllocator(namespace, dataDir)
}

// NewSIDAllocatorForStorageWithRegistry binds the allocator to an explicit
// shared registry. The registry is a low-frequency symbol metadata dependency;
// no registry lookup occurs on data or strategy hot paths.
func NewSIDAllocatorForStorageWithRegistry(namespace, dataDir string, registry *SymbolSIDRegistry) *SIDAllocator {
	allocator := newSIDAllocator(namespace, dataDir)
	allocator.registry = registry
	return allocator
}

func newSIDAllocator(namespace, dataDir string) *SIDAllocator {
	namespace = strings.TrimSpace(namespace)
	return &SIDAllocator{
		namespace:           namespace,
		reservationRoot:     sidReservationRoot(dataDir, namespace),
		reservations:        make(map[string]int32),
		pendingReservations: make(map[string]int32),
		reservationKeys:     make(map[int32]string),
		recoveryRoots:       make(map[string]struct{}),
	}
}

func (a *SIDAllocator) Namespace() string {
	if a == nil {
		return ""
	}
	a.identityMu.RLock()
	namespace := a.namespace
	a.identityMu.RUnlock()
	return namespace
}

// BindSIDRegistry attaches the process-owned registry to this allocator. A
// second, different registry is rejected because mixing authorities can split
// one storage identity into two SID spaces.
func (a *SIDAllocator) BindSIDRegistry(registry *SymbolSIDRegistry) error {
	if a == nil || registry == nil {
		return nil
	}
	a.registryMu.Lock()
	defer a.registryMu.Unlock()
	if a.registry != nil && a.registry != registry && a.registry.URL() != registry.URL() {
		return fmt.Errorf("SID allocator %q is already bound to registry %q", a.Namespace(), a.registry.URL())
	}
	if a.registry == nil {
		a.reserveMu.RLock()
		hasReservations := len(a.reservations) > 0 || len(a.pendingReservations) > 0
		a.reserveMu.RUnlock()
		if hasReservations {
			return fmt.Errorf("SID allocator %q cannot switch to a registry after local SID reservations exist", a.Namespace())
		}
	}
	a.registry = registry
	return nil
}

func (a *SIDAllocator) SIDRegistry() *SymbolSIDRegistry {
	if a == nil {
		return nil
	}
	a.registryMu.RLock()
	registry := a.registry
	a.registryMu.RUnlock()
	return registry
}

// configuredSIDRegistry is used only by legacy facades. Explicit Runtime
// allocators are bound during construction and never consult package config.
func (a *SIDAllocator) configuredSIDRegistry() (*SymbolSIDRegistry, error) {
	if registry := a.SIDRegistry(); registry != nil {
		return registry, nil
	}
	if a == nil || !a.legacyConfig {
		return nil, nil
	}
	if config.Database == nil || strings.TrimSpace(config.Database.SIDRegistryURL) == "" {
		return nil, nil
	}
	registry, err := NewSymbolSIDRegistry(config.Database.SIDRegistryURL, config.Database.AutoCreate)
	if err != nil {
		return nil, err
	}
	if err := a.BindSIDRegistry(registry); err != nil {
		registry.Close()
		return nil, err
	}
	return a.SIDRegistry(), nil
}

func (a *SIDAllocator) bindStorage(dataDir string) {
	if a == nil {
		return
	}
	base := strings.TrimSpace(dataDir)
	if base == "" {
		base = config.GetDataDirSafe()
	}
	a.identityMu.Lock()
	if a.namespace == "" {
		a.namespace = CanonicalStorageIdentityForType("", databaseURL(), base, databaseType())
		if a.namespace == "" && base != "" {
			a.namespace = "data-dir:" + absoluteStoragePath(base)
		}
	}
	if a.reservationRoot == "" {
		a.reservationRoot = sidReservationRoot(base, a.namespace)
	}
	a.identityMu.Unlock()
}

func (a *SIDAllocator) sharedReservationRoot() string {
	if a == nil {
		return ""
	}
	a.identityMu.RLock()
	root := a.reservationRoot
	a.identityMu.RUnlock()
	return root
}

func defaultSIDAllocatorNamespace() string {
	dataDir := config.GetDataDirSafe()
	namespace := CanonicalStorageIdentityForType("", databaseURL(), dataDir, databaseType())
	if namespace == "" && strings.TrimSpace(dataDir) != "" {
		namespace = "data-dir:" + absoluteStoragePath(dataDir)
	}
	return namespace
}

func sidReservationRoot(_ string, namespace string) string {
	return sharedStorageCoordinationRoot(namespace)
}

// registerRecoveryRoot records a recovery directory known to this process and
// storage namespace. It is metadata for low-frequency symbol reconciliation;
// SID allocation itself remains protected by ensureMu and reserveMu.
func (a *SIDAllocator) registerRecoveryRoot(root string) {
	if a == nil || strings.TrimSpace(root) == "" {
		return
	}
	a.rootMu.Lock()
	if a.recoveryRoots == nil {
		a.recoveryRoots = make(map[string]struct{})
	}
	a.recoveryRoots[root] = struct{}{}
	a.rootMu.Unlock()
}

func (a *SIDAllocator) recoveryRootSnapshot() []string {
	if a == nil {
		return nil
	}
	a.rootMu.RLock()
	roots := make([]string, 0, len(a.recoveryRoots))
	for root := range a.recoveryRoots {
		roots = append(roots, root)
	}
	a.rootMu.RUnlock()
	return roots
}

// SymbolState owns the in-memory symbol indexes and subscription sets for one
// runtime. Cached symbols are immutable snapshots: writers publish a
// replacement pointer for metadata changes, while readers can use the cached
// pointer without taking ownership of mutable caller memory. The package-level
// API delegates to defaultSymbolState for legacy callers; new runtimes use
// their own instance directly.
type SymbolState struct {
	mu          sync.RWMutex
	lifecycleMu sync.RWMutex

	identitySet      bool
	identityExchange string
	identityMarket   string
	recoveryMu       sync.RWMutex
	recoveryRoot     string

	keySymbols map[string]*ExSymbol
	idSymbols  map[int32]*ExSymbol
	markets    map[string]int
	maxSID     int32
	allocator  *SIDAllocator
	generation uint64

	tryListMu  sync.Mutex
	tryListIDs map[int32]bool

	subMu     sync.RWMutex
	pairs     map[string]*ExSymbol
	hourPairs map[string]*ExSymbol
}

func NewSymbolState() *SymbolState {
	return NewSymbolStateWithAllocator(nil)
}

// NewSymbolStateWithAllocator creates a symbol state with an optional shared
// SID allocator. A nil allocator creates an allocator local to this state.
// Runtime composition roots should pass their Process-owned allocator when
// several runtimes can write to the same symbol database.
func NewSymbolStateWithAllocator(allocator *SIDAllocator) *SymbolState {
	return NewSymbolStateWithAllocatorAndIdentity(allocator, "", "")
}

// NewSymbolStateWithIdentity creates a symbol state bound to one immutable
// exchange and market. An empty exchange or market keeps the state unbound,
// preserving the legacy global fallback used by NewSymbolState.
func NewSymbolStateWithIdentity(exchange, market string) *SymbolState {
	return NewSymbolStateWithAllocatorAndIdentity(nil, exchange, market)
}

// NewSymbolStateWithAllocatorAndIdentity combines a process-owned SID
// allocator with an immutable exchange/market identity.
func NewSymbolStateWithAllocatorAndIdentity(allocator *SIDAllocator, exchange, market string) *SymbolState {
	if allocator == nil {
		allocator = NewSIDAllocator()
	}
	return &SymbolState{
		identitySet:      exchange != "" && market != "",
		identityExchange: exchange,
		identityMarket:   market,
		keySymbols:       make(map[string]*ExSymbol),
		idSymbols:        make(map[int32]*ExSymbol),
		markets:          make(map[string]int),
		tryListIDs:       make(map[int32]bool),
		pairs:            make(map[string]*ExSymbol),
		hourPairs:        make(map[string]*ExSymbol),
		allocator:        allocator,
	}
}

func (s *SymbolState) acceptsIdentity(exchange, market string) bool {
	return s == nil || !s.identitySet || exchange == s.identityExchange && market == s.identityMarket
}

func loadDefaultSymbolState() *SymbolState {
	defaultSymbolStateMu.RLock()
	state := defaultSymbolState
	defaultSymbolStateMu.RUnlock()
	return state
}

func swapDefaultSymbolState(next *SymbolState) *SymbolState {
	defaultSymbolStateMu.Lock()
	previous := defaultSymbolState
	defaultSymbolState = next
	defaultSymbolStateMu.Unlock()
	return previous
}

// SetExSymbols replaces this state's in-memory symbol indexes. It does not
// read or write persistent storage.
func (s *SymbolState) SetExSymbols(items []*ExSymbol) error {
	if s == nil {
		return fmt.Errorf("nil SymbolState")
	}
	allocator := s.sidAllocator()
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	keys := make(map[string]*ExSymbol, len(items))
	ids := make(map[int32]*ExSymbol, len(items))
	markets := make(map[string]int)
	var maxSID int32
	for _, item := range items {
		if item == nil || item.ID <= 0 || item.Exchange == "" || item.Market == "" || item.Symbol == "" {
			return fmt.Errorf("invalid frozen exchange symbol")
		}
		if !s.acceptsIdentity(item.Exchange, item.Market) {
			return fmt.Errorf("symbol %s:%s:%s does not belong to state identity %s:%s",
				item.Exchange, item.Market, item.Symbol, s.identityExchange, s.identityMarket)
		}
		key := exSymbolKey(item.Exchange, item.Market, item.Symbol, item.ExgReal)
		if ids[item.ID] != nil {
			return fmt.Errorf("duplicate frozen exchange symbol: sid=%d", item.ID)
		}
		copyItem := *item
		itemCopy := &copyItem
		ids[copyItem.ID] = itemCopy
		if current, exists := keys[key]; !exists {
			keys[key] = itemCopy
			markets[fmt.Sprintf("%s:%s", copyItem.Exchange, copyItem.Market)]++
		} else if current.ExgReal != "" && copyItem.ExgReal == "" {
			// Keep the same canonical preference as CacheExSymbol: an empty
			// ExgReal is the generic lookup row, while ID lookup retains both.
			keys[key] = itemCopy
		}
		maxSID = max(maxSID, copyItem.ID)
	}
	reservations := make([]sidCachedReservation, 0, len(items))
	for _, item := range items {
		key := exSymbolKey(item.Exchange, item.Market, item.Symbol)
		canonical := keys[key]
		reservations = append(reservations, sidCachedReservation{
			key:         key,
			canonicalID: canonical.ID,
			physicalID:  item.ID,
		})
	}
	s.mu.Lock()
	if err := allocator.reserveCachedSIDBatch(reservations); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("set exchange symbol catalog: %w", err)
	}
	s.keySymbols = keys
	s.idSymbols = ids
	s.markets = markets
	s.maxSID = maxSID
	s.generation++
	s.mu.Unlock()
	s.tryListMu.Lock()
	s.tryListIDs = make(map[int32]bool)
	s.tryListMu.Unlock()
	s.ResetSubSymbol()
	allocator.observeSID(maxSID)
	return nil
}

func (s *SymbolState) CacheExSymbol(exs *ExSymbol) {
	_ = s.CacheExSymbolChecked(exs)
}

func (s *SymbolState) CacheExSymbolChecked(exs *ExSymbol) error {
	if s == nil {
		return nil
	}
	allocator := s.sidAllocator()
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()
	return s.cacheExSymbolChecked(exs)
}

func (s *SymbolState) cacheExSymbolChecked(exs *ExSymbol) error {
	if s == nil || exs == nil {
		return nil
	}
	if !s.acceptsIdentity(exs.Exchange, exs.Market) {
		return fmt.Errorf("symbol %s:%s:%s does not belong to state identity %s:%s",
			exs.Exchange, exs.Market, exs.Symbol, s.identityExchange, s.identityMarket)
	}
	item := cloneExSymbol(exs)
	key := exSymbolKey(item.Exchange, item.Market, item.Symbol)
	allocator := s.sidAllocator()
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.idSymbols[item.ID]; current != nil && exSymbolKey(current.Exchange, current.Market, current.Symbol) != key {
		return fmt.Errorf("sid %d is already cached for logical symbol %s, cannot cache %s", item.ID,
			exSymbolKey(current.Exchange, current.Market, current.Symbol), key)
	}
	canonical := s.keySymbols[key]
	canonicalID := item.ID
	var replacedID int32
	if canonical != nil {
		canonicalID = canonical.ID
		if canonical.ExgReal != "" && item.ExgReal == "" {
			canonicalID = item.ID
			if canonical.ID != item.ID {
				replacedID = canonical.ID
			}
		}
	}
	if item.ID > 0 {
		if canonicalID <= 0 {
			canonicalID = item.ID
		}
		if err := allocator.reserveCachedSID(key, canonicalID, item.ID, replacedID); err != nil {
			return err
		}
	}
	s.cacheExSymbolLocked(item)
	return nil
}

func (s *SymbolState) cacheExSymbolLocked(exs *ExSymbol) {
	if exs == nil {
		return
	}
	if s.keySymbols == nil {
		s.keySymbols = make(map[string]*ExSymbol)
	}
	if s.idSymbols == nil {
		s.idSymbols = make(map[int32]*ExSymbol)
	}
	if s.markets == nil {
		s.markets = make(map[string]int)
	}
	if exs.ID > s.maxSID {
		s.maxSID = exs.ID
	}
	if s.allocator != nil {
		s.allocator.observeSID(exs.ID)
	}
	s.idSymbols[exs.ID] = exs
	key := exSymbolKey(exs.Exchange, exs.Market, exs.Symbol, exs.ExgReal)
	if cur, ok := s.keySymbols[key]; ok {
		if cur.ExgReal == "" || exs.ExgReal != "" {
			return
		}
		// Replacing the canonical pointer does not add another logical
		// exchange/market/symbol identity.
		s.keySymbols[key] = exs
		return
	}
	s.keySymbols[key] = exs
	market := fmt.Sprintf("%s:%s", exs.Exchange, exs.Market)
	s.markets[market]++
}

func (s *SymbolState) GetExSymbols(exgName, market string) map[int32]*ExSymbol {
	res := make(map[int32]*ExSymbol)
	if s == nil {
		return res
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, exs := range s.keySymbols {
		if exgName != "" && exs.Exchange != exgName {
			continue
		}
		if market != "" && exs.Market != market {
			continue
		}
		res[exs.ID] = exs
	}
	return res
}

// GetExSymbolsByID returns every cached SID, including non-canonical rows that
// share an exchange, market, and symbol with another row.
func (s *SymbolState) GetExSymbolsByID(exgName, market string) map[int32]*ExSymbol {
	res := make(map[int32]*ExSymbol)
	if s == nil {
		return res
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, exs := range s.idSymbols {
		if exgName != "" && exs.Exchange != exgName {
			continue
		}
		if market != "" && exs.Market != market {
			continue
		}
		res[id] = exs
	}
	return res
}

func (s *SymbolState) GetExSymbolMap(exgName, market string) map[string]*ExSymbol {
	res := make(map[string]*ExSymbol)
	if s == nil {
		return res
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, exs := range s.keySymbols {
		if exgName != "" && exs.Exchange != exgName {
			continue
		}
		if market != "" && exs.Market != market {
			continue
		}
		if cur, ok := res[exs.Symbol]; !ok || cur.ExgReal != "" && exs.ExgReal == "" {
			res[exs.Symbol] = exs
		}
	}
	return res
}

func (s *SymbolState) GetSymbolByID(id int32) *ExSymbol {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	item := s.idSymbols[id]
	s.mu.RUnlock()
	return item
}

func (s *SymbolState) GetExSymbol2(exgName, market, symbol string, _ ...string) *ExSymbol {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	item := s.keySymbols[exSymbolKey(exgName, market, symbol)]
	s.mu.RUnlock()
	return item
}

func (s *SymbolState) SymbolCount() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	count := len(s.keySymbols)
	s.mu.RUnlock()
	return count
}

func (s *SymbolState) MarketCount(exchange, market string) int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	count := s.markets[fmt.Sprintf("%s:%s", exchange, market)]
	s.mu.RUnlock()
	return count
}

func (s *SymbolState) MaxSID() int32 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	id := s.maxSID
	s.mu.RUnlock()
	return id
}

func (s *SymbolState) ObserveSID(id int32) {
	if s == nil {
		return
	}
	s.sidAllocator().observeSID(id)
	s.mu.Lock()
	if id > s.maxSID {
		s.maxSID = id
	}
	s.mu.Unlock()
}

func (s *SymbolState) NextSID() int32 {
	if s == nil {
		return 0
	}
	allocator := s.sidAllocator()
	unlock := allocator.lockEnsure()
	defer unlock()
	return nextSymbolSID(allocator, s)
}

func (s *SymbolState) SetMaxSID(id int32) {
	if s == nil {
		return
	}
	s.sidAllocator().observeSID(id)
	s.mu.Lock()
	s.maxSID = id
	s.mu.Unlock()
}

func (s *SymbolState) sidAllocator() *SIDAllocator {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.allocator == nil {
		s.allocator = NewSIDAllocator()
	}
	allocator := s.allocator
	s.mu.Unlock()
	return allocator
}

func (s *SymbolState) updateAggRules(id int32, aggRules string, base *ExSymbol) *ExSymbol {
	return s.updateSymbol(id, base, func(exs *ExSymbol) {
		exs.AggRules = aggRules
	})
}

func (s *SymbolState) updateListMS(id int32, listMS, delistMS int64, base *ExSymbol) *ExSymbol {
	return s.updateSymbol(id, base, func(exs *ExSymbol) {
		exs.ListMs = listMS
		exs.DelistMs = delistMS
	})
}

func (s *SymbolState) updateSymbol(id int32, base *ExSymbol, update func(*ExSymbol)) *ExSymbol {
	if s == nil {
		return nil
	}
	allocator := s.sidAllocator()
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()
	s.mu.RLock()
	item := s.idSymbols[id]
	s.mu.RUnlock()
	if item == nil {
		if base == nil {
			return nil
		}
		next := cloneExSymbol(base)
		next.ID = id
		update(next)
		if err := s.cacheExSymbolChecked(next); err != nil {
			return nil
		}
		return s.GetSymbolByID(id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := exSymbolKey(item.Exchange, item.Market, item.Symbol)
	canonical := s.keySymbols[key]
	if canonical != nil && canonical.ID > 0 {
		if err := allocator.reserveCachedSID(key, canonical.ID, item.ID, 0); err != nil {
			return nil
		}
	}
	next := *item
	update(&next)
	s.replaceSymbolLocked(id, &next)
	return &next
}

func (s *SymbolState) replaceSymbolLocked(id int32, next *ExSymbol) {
	old := s.idSymbols[id]
	s.idSymbols[id] = next
	for key, item := range s.keySymbols {
		if item == old || item != nil && item.ID == id {
			s.keySymbols[key] = next
		}
	}
	s.subMu.Lock()
	for symbol, item := range s.hourPairs {
		if item != nil && item.ID == id {
			s.hourPairs[symbol] = next
		}
	}
	s.subMu.Unlock()
}

func (s *SymbolState) Reset() {
	if s == nil {
		return
	}
	allocator := s.sidAllocator()
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	s.keySymbols = make(map[string]*ExSymbol)
	s.idSymbols = make(map[int32]*ExSymbol)
	s.markets = make(map[string]int)
	s.maxSID = 0
	s.generation++
	s.mu.Unlock()
	s.tryListMu.Lock()
	s.tryListIDs = make(map[int32]bool)
	s.tryListMu.Unlock()
	s.ResetSubSymbol()
}

func (s *SymbolState) AddHourSymbol(exs *ExSymbol) {
	if s == nil || exs == nil {
		return
	}
	if !s.acceptsIdentity(exs.Exchange, exs.Market) {
		return
	}
	// Prefer the canonical immutable snapshot already owned by this state. If
	// the caller has not cached the symbol yet, keep an isolated copy here.
	s.mu.RLock()
	canonical := s.idSymbols[exs.ID]
	if canonical == nil {
		canonical = s.keySymbols[exSymbolKey(exs.Exchange, exs.Market, exs.Symbol, exs.ExgReal)]
	}
	s.mu.RUnlock()
	if canonical == nil {
		canonical = cloneExSymbol(exs)
	}
	s.subMu.Lock()
	if s.hourPairs == nil {
		s.hourPairs = make(map[string]*ExSymbol)
	}
	s.hourPairs[canonical.Symbol] = canonical
	s.subMu.Unlock()
}

func (s *SymbolState) Sub1mSymbol(pair string) {
	if s == nil {
		return
	}
	s.subMu.Lock()
	s.pairs[pair] = nil
	s.subMu.Unlock()
}

func (s *SymbolState) ResetSubSymbol() {
	if s == nil {
		return
	}
	s.subMu.Lock()
	s.hourPairs = make(map[string]*ExSymbol)
	s.pairs = make(map[string]*ExSymbol)
	s.subMu.Unlock()
}

func (s *SymbolState) GetHourOnlySymbols() map[int32]*ExSymbol {
	res := make(map[int32]*ExSymbol)
	if s == nil {
		return res
	}
	s.subMu.RLock()
	defer s.subMu.RUnlock()
	for _, exs := range s.hourPairs {
		if exs == nil {
			continue
		}
		if _, ok := s.pairs[exs.Symbol]; !ok {
			res[exs.ID] = exs
		}
	}
	return res
}

func cloneExSymbol(exs *ExSymbol) *ExSymbol {
	if exs == nil {
		return nil
	}
	copyItem := *exs
	return &copyItem
}

type sidReservation struct {
	key string
	id  int32
}

type sidCachedReservation struct {
	key         string
	canonicalID int32
	physicalID  int32
	replacedID  int32
}

func sameExSymbolSnapshot(a, b *ExSymbol) bool {
	return a != nil && b != nil && a.ID == b.ID && a.Exchange == b.Exchange && a.ExgReal == b.ExgReal &&
		a.Market == b.Market && a.Symbol == b.Symbol && a.Combined == b.Combined &&
		a.ListMs == b.ListMs && a.DelistMs == b.DelistMs && a.AggRules == b.AggRules
}

func (s *SymbolState) validateReservedSymbol(item *ExSymbol) error {
	if s == nil || item == nil {
		return nil
	}
	key := exSymbolKey(item.Exchange, item.Market, item.Symbol)
	s.mu.RLock()
	canonical := s.keySymbols[key]
	byID := s.idSymbols[item.ID]
	s.mu.RUnlock()
	if canonical != nil && canonical.ID != item.ID {
		return fmt.Errorf("logical symbol %s is already cached as sid %d, cannot reserve sid %d", key, canonical.ID, item.ID)
	}
	if byID != nil && exSymbolKey(byID.Exchange, byID.Market, byID.Symbol) != key {
		return fmt.Errorf("sid %d is already cached for logical symbol %s, cannot reserve %s", item.ID,
			exSymbolKey(byID.Exchange, byID.Market, byID.Symbol), key)
	}
	if canonical != nil && !sameExSymbolSnapshot(canonical, item) {
		return fmt.Errorf("reservation for logical symbol %s sid %d conflicts with canonical metadata", key, item.ID)
	}
	if byID != nil && !sameExSymbolSnapshot(byID, item) {
		return fmt.Errorf("reservation for logical symbol %s sid %d conflicts with cached metadata", key, item.ID)
	}
	return nil
}

func (s *SymbolState) reserveCanonicalSIDs(allocator *SIDAllocator) error {
	if s == nil || allocator == nil {
		return nil
	}
	s.mu.RLock()
	reservations := make([]sidReservation, 0, len(s.keySymbols))
	for _, item := range s.keySymbols {
		if item == nil || item.ID <= 0 {
			continue
		}
		reservations = append(reservations, sidReservation{
			key: exSymbolKey(item.Exchange, item.Market, item.Symbol),
			id:  item.ID,
		})
	}
	s.mu.RUnlock()
	if err := allocator.reserveSIDBatch(reservations); err != nil {
		return fmt.Errorf("reserve cached exchange symbols: %w", err)
	}
	return nil
}

func (a *SIDAllocator) lockEnsure() func() {
	if a == nil {
		return func() {}
	}
	a.ensureMu.Lock()
	return a.ensureMu.Unlock
}

// Reservations close QuestDB's WAL visibility window. Pending reservations
// remain unconfirmed until the database row is visible. Cross-host SID
// allocation is provided by SIDRegistry; the local pending ledger is only a
// compatibility recovery fence for single-writer deployments.
func (a *SIDAllocator) reservedSID(key string) int32 {
	if a == nil {
		return 0
	}
	a.reserveMu.RLock()
	id := a.reservations[key]
	a.reserveMu.RUnlock()
	return id
}

func (a *SIDAllocator) pendingSID(key string) int32 {
	if a == nil {
		return 0
	}
	a.reserveMu.RLock()
	id := a.pendingReservations[key]
	a.reserveMu.RUnlock()
	return id
}

func (a *SIDAllocator) reservationSID(key string) int32 {
	if a == nil {
		return 0
	}
	a.reserveMu.RLock()
	id := a.reservations[key]
	if id <= 0 {
		id = a.pendingReservations[key]
	}
	a.reserveMu.RUnlock()
	return id
}

func (a *SIDAllocator) reserveSID(key string, id int32) int32 {
	if a == nil || key == "" || id <= 0 {
		return 0
	}
	if err := a.reserveSIDBatch([]sidReservation{{key: key, id: id}}); err != nil {
		return 0
	}
	return id
}

func (a *SIDAllocator) reserveSIDBatch(items []sidReservation) error {
	return a.reserveSIDBatchState(items, true)
}

func (a *SIDAllocator) reservePendingSIDBatch(items []sidReservation) error {
	return a.reserveSIDBatchState(items, false)
}

func (a *SIDAllocator) markSIDConfirmed(key string, id int32) error {
	return a.reserveSIDBatch([]sidReservation{{key: key, id: id}})
}

func (a *SIDAllocator) reserveSIDBatchState(items []sidReservation, confirmed bool) error {
	if a == nil || len(items) == 0 {
		return nil
	}
	a.reserveMu.Lock()
	defer a.reserveMu.Unlock()
	reservations := cloneSIDReservations(a.reservations)
	pendingReservations := cloneSIDReservations(a.pendingReservations)
	reservationKeys := cloneSIDReservationKeys(a.reservationKeys)
	for key, id := range reservations {
		if id <= 0 {
			continue
		}
		if owner := reservationKeys[id]; owner != "" && owner != key {
			return fmt.Errorf("sid %d is reserved for both logical symbols %s and %s", id, owner, key)
		}
		reservationKeys[id] = key
	}
	for key, id := range pendingReservations {
		if id <= 0 {
			continue
		}
		if owner := reservationKeys[id]; owner != "" && owner != key {
			return fmt.Errorf("sid %d is reserved for both logical symbols %s and %s", id, owner, key)
		}
		reservationKeys[id] = key
		if confirmedID := reservations[key]; confirmedID != 0 {
			if confirmedID != id {
				return fmt.Errorf("logical symbol %s is confirmed as sid %d and pending as sid %d", key, confirmedID, id)
			}
			delete(pendingReservations, key)
		}
	}
	pendingKeys := make(map[string]int32, len(items))
	pendingIDs := make(map[int32]string, len(items))
	for _, item := range items {
		if item.key == "" || item.id <= 0 {
			return fmt.Errorf("invalid SID reservation: key=%q sid=%d", item.key, item.id)
		}
		if current := pendingKeys[item.key]; current != 0 && current != item.id {
			return fmt.Errorf("logical symbol %s is reserved for both sid %d and sid %d", item.key, current, item.id)
		}
		if owner := pendingIDs[item.id]; owner != "" && owner != item.key {
			return fmt.Errorf("sid %d is reserved for both logical symbols %s and %s", item.id, owner, item.key)
		}
		if current := reservations[item.key]; current != 0 && current != item.id {
			return fmt.Errorf("logical symbol %s is already reserved as sid %d, cannot reserve sid %d", item.key, current, item.id)
		}
		if current := pendingReservations[item.key]; current != 0 && current != item.id {
			return fmt.Errorf("logical symbol %s is pending as sid %d, cannot reserve sid %d", item.key, current, item.id)
		}
		if owner := reservationKeys[item.id]; owner != "" && owner != item.key {
			return fmt.Errorf("sid %d is already reserved for logical symbol %s, cannot reserve %s", item.id, owner, item.key)
		}
		pendingKeys[item.key] = item.id
		pendingIDs[item.id] = item.key
	}
	for key, id := range pendingKeys {
		if confirmed {
			delete(pendingReservations, key)
			reservations[key] = id
		} else if reservations[key] == 0 {
			pendingReservations[key] = id
		}
		reservationKeys[id] = key
	}
	a.reservations = reservations
	a.pendingReservations = pendingReservations
	a.reservationKeys = reservationKeys
	for _, item := range items {
		a.observeSID(item.id)
	}
	return nil
}

func cloneSIDReservations(src map[string]int32) map[string]int32 {
	if len(src) == 0 {
		return make(map[string]int32)
	}
	dst := make(map[string]int32, len(src))
	for key, id := range src {
		dst[key] = id
	}
	return dst
}

func cloneSIDReservationKeys(src map[int32]string) map[int32]string {
	if len(src) == 0 {
		return make(map[int32]string)
	}
	dst := make(map[int32]string, len(src))
	for id, key := range src {
		dst[id] = key
	}
	return dst
}

// reserveCachedSID publishes a confirmed canonical identity and records every
// cached physical SID under that identity. A generic row may replace a
// non-empty ExgReal row as the canonical lookup entry; the old physical SID
// remains fenced so a later cache cannot reuse it for another identity.
func (a *SIDAllocator) reserveCachedSID(key string, canonicalID, physicalID, replacedID int32) error {
	return a.reserveCachedSIDBatch([]sidCachedReservation{{
		key:         key,
		canonicalID: canonicalID,
		physicalID:  physicalID,
		replacedID:  replacedID,
	}})
}

func (a *SIDAllocator) reserveCachedSIDBatch(items []sidCachedReservation) error {
	if a == nil || len(items) == 0 {
		return nil
	}
	a.reserveMu.Lock()
	defer a.reserveMu.Unlock()
	reservations := cloneSIDReservations(a.reservations)
	pendingReservations := cloneSIDReservations(a.pendingReservations)
	reservationKeys := cloneSIDReservationKeys(a.reservationKeys)
	for _, item := range items {
		if err := reserveCachedSIDOnMaps(reservations, pendingReservations, reservationKeys,
			item.key, item.canonicalID, item.physicalID, item.replacedID); err != nil {
			return err
		}
	}
	a.reservations = reservations
	a.pendingReservations = pendingReservations
	a.reservationKeys = reservationKeys
	for _, item := range items {
		a.observeSID(item.canonicalID)
		a.observeSID(item.physicalID)
	}
	return nil
}

func reserveCachedSIDOnMaps(reservations, pendingReservations map[string]int32, reservationKeys map[int32]string,
	key string, canonicalID, physicalID, replacedID int32) error {
	if key == "" || canonicalID <= 0 || physicalID <= 0 {
		return nil
	}
	for reservedKey, id := range reservations {
		if id <= 0 {
			continue
		}
		if owner := reservationKeys[id]; owner != "" && owner != reservedKey {
			return fmt.Errorf("sid %d is reserved for both logical symbols %s and %s", id, owner, reservedKey)
		}
		reservationKeys[id] = reservedKey
	}
	for pendingKey, id := range pendingReservations {
		if id <= 0 {
			continue
		}
		if owner := reservationKeys[id]; owner != "" && owner != pendingKey {
			return fmt.Errorf("sid %d is reserved for both logical symbols %s and %s", id, owner, pendingKey)
		}
		reservationKeys[id] = pendingKey
		if confirmedID := reservations[pendingKey]; confirmedID != 0 {
			if confirmedID != id {
				return fmt.Errorf("logical symbol %s is confirmed as sid %d and pending as sid %d", pendingKey, confirmedID, id)
			}
			delete(pendingReservations, pendingKey)
		}
	}
	currentID := reservations[key]
	if currentID == 0 {
		currentID = pendingReservations[key]
	}
	if replacedID > 0 {
		if currentID != 0 && currentID != replacedID {
			return fmt.Errorf("logical symbol %s is reserved as sid %d, cannot replace sid %d", key, currentID, replacedID)
		}
	} else if currentID != 0 && currentID != canonicalID {
		return fmt.Errorf("logical symbol %s is already reserved as sid %d, cannot reserve sid %d", key, currentID, canonicalID)
	}
	for _, id := range []int32{canonicalID, physicalID, replacedID} {
		if id <= 0 {
			continue
		}
		if owner := reservationKeys[id]; owner != "" && owner != key {
			return fmt.Errorf("sid %d is already reserved for logical symbol %s, cannot reserve %s", id, owner, key)
		}
	}
	delete(pendingReservations, key)
	reservations[key] = canonicalID
	reservationKeys[canonicalID] = key
	reservationKeys[physicalID] = key
	return nil
}

func (a *SIDAllocator) observeSID(id int32) {
	if a == nil || id <= 0 {
		return
	}
	for {
		current := a.max.Load()
		if id <= current || a.max.CompareAndSwap(current, id) {
			return
		}
	}
}

func (s *SymbolState) catalogGeneration() uint64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	generation := s.generation
	s.mu.RUnlock()
	return generation
}

func (s *SymbolState) hasCatalogGeneration(generation uint64) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	matches := s.generation == generation
	s.mu.RUnlock()
	return matches
}

func (s *SymbolState) lockCatalogGeneration(generation uint64) (func(), bool) {
	if s == nil {
		return func() {}, false
	}
	s.lifecycleMu.RLock()
	if !s.hasCatalogGeneration(generation) {
		s.lifecycleMu.RUnlock()
		return func() {}, false
	}
	return s.lifecycleMu.RUnlock, true
}

func prepareSymbolSIDAllocation(allocator *SIDAllocator, state *SymbolState, dbMax int32) {
	if allocator == nil || state == nil {
		return
	}
	state.ObserveSID(dbMax)
	stateMax := state.MaxSID()
	allocator.observeSID(stateMax)
	if allocatorMax := allocator.max.Load(); stateMax < allocatorMax {
		state.SetMaxSID(allocatorMax)
	}
}

func nextSymbolSID(allocator *SIDAllocator, state *SymbolState) int32 {
	if allocator == nil || state == nil {
		return 0
	}
	// Keep the allocator counter authoritative even for internal callers that
	// already hold, or intentionally do not hold, the ensure lock. The atomic
	// increment prevents two states sharing an allocator from returning the same
	// SID while their local catalogs are still empty.
	state.mu.Lock()
	allocator.observeSID(state.maxSID)
	id := allocator.max.Add(1)
	state.maxSID = max(state.maxSID, id)
	state.mu.Unlock()
	return id
}
