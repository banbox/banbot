package orm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

var (
	defaultSymbolState   = NewSymbolState()
	defaultSymbolStateMu sync.RWMutex
	aggRulesMu           sync.RWMutex
	aggRules             = map[string]AggRuleFunc{
		"min":   aggMin,
		"max":   aggMax,
		"last":  aggLast,
		"first": aggFirst,
		"sum":   aggSum,
		"avg":   aggAvg,
		"mid":   aggMid,
	}
)

type AggRuleFunc func(rows []*DataRecord, field SeriesField) (any, error)

func cacheExSymbol(exs *ExSymbol) {
	loadDefaultSymbolState().CacheExSymbol(exs)
}

func exSymbolKey(exchange, market, symbol string, _ ...string) string {
	return fmt.Sprintf("%s:%s:%s", exchange, market, symbol)
}

func findExSymbol(exchange, market, symbol string) *ExSymbol {
	return loadDefaultSymbolState().GetExSymbol2(exchange, market, symbol)
}

func (q *Queries) LoadExgSymbols(exgName string) *errs.Error {
	state, err := q.requireSymbolState()
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	return q.loadExgSymbols(state, exgName)
}

func (q *SymbolQueries) LoadExgSymbols(exgName string) *errs.Error {
	if q == nil {
		return errs.NewMsg(core.ErrBadConfig, "symbol query is required")
	}
	state := q.symbolState()
	if state == nil {
		return errs.NewMsg(core.ErrBadConfig, "explicit storage requires an explicit symbol state")
	}
	return q.Queries.loadExgSymbols(state, exgName)
}

func (q *Queries) loadExgSymbols(state *SymbolState, exgName string) *errs.Error {
	state = symbolStateOrDefault(state)
	unlockEnsure := state.sidAllocator().lockEnsure()
	defer unlockEnsure()
	return q.loadExgSymbolsLocked(state, exgName)
}

// loadExgSymbolsLocked is used by catalog operations that already own the
// allocator ensure lock. Keeping the database read and cache publication in a
// separate call shape avoids recursive locking in EnsureSymbols.
func (q *Queries) loadExgSymbolsLocked(state *SymbolState, exgName string) *errs.Error {
	ctx := context.Background()
	exsList, err := q.ListSymbols(ctx, exgName)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if err := cacheExgSymbolsForState(state, exsList); err != nil {
		return errs.New(core.ErrBadConfig, fmt.Errorf("cache exchange symbol: %w", err))
	}
	return nil
}

// cacheExgSymbolsForState publishes only symbols owned by state. ListSymbols
// is exchange-scoped, while explicit SymbolState instances are market-scoped.
// All rows still advance the shared SID allocator so a filtered market cannot
// cause a later symbol allocation to reuse a physical database SID.
func cacheExgSymbolsForState(state *SymbolState, exsList []*ExSymbol) error {
	state = symbolStateOrDefault(state)
	for _, exs := range exsList {
		if exs == nil {
			continue
		}
		state.ObserveSID(exs.ID)
		if !state.acceptsIdentity(exs.Exchange, exs.Market) {
			continue
		}
		if err := state.cacheExSymbolChecked(exs); err != nil {
			return err
		}
	}
	return nil
}

func GetExSymbols(exgName, market string) map[int32]*ExSymbol {
	return loadDefaultSymbolState().GetExSymbols(exgName, market)
}

func GetExSymbolMap(exgName, market string) map[string]*ExSymbol {
	return loadDefaultSymbolState().GetExSymbolMap(exgName, market)
}

func GetSymbolByID(id int32) *ExSymbol {
	return loadDefaultSymbolState().GetSymbolByID(id)
}

func GetExSymbolCur(symbol string) (*ExSymbol, *errs.Error) {
	if exg.Default == nil {
		state := loadDefaultSymbolState()
		item := state.GetExSymbol2(core.ExgName, core.Market, symbol)
		if item == nil {
			return nil, errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache", symbol, state.SymbolCount())
		}
		return item, nil
	}
	return GetExSymbol(exg.Default, symbol)
}

func GetExSymbol(exchange banexg.BanExchange, symbol string) (*ExSymbol, *errs.Error) {
	if exchange == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	market, err := exchange.GetMarket(symbol)
	// It is not immediately exited here, it may be delisted, and it is returned empty, but there is historical data, you can try to get it from the cache below
	// 这里不立即退出，可能退市了这里返回空，但有历史数据，可尝试从下面缓存获取
	exgInfo := exchange.Info()
	var marketType = exgInfo.MarketType
	if market != nil {
		marketType = market.Type
	}
	state := loadDefaultSymbolState()
	item := state.GetExSymbol2(exgInfo.ID, marketType, symbol)
	if item == nil {
		if err == nil {
			err = errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache", symbol, state.SymbolCount())
		}
		return nil, err
	}
	return item, nil
}

// GetExSymbolCur resolves a symbol from this state without consulting the
// package-level legacy cache. It is intentionally a low-frequency lookup used
// while constructing feeders; event processing keeps the resolved pointer.
func (s *SymbolState) GetExSymbolCur(symbol string) (*ExSymbol, *errs.Error) {
	if s == nil {
		return GetExSymbolCur(symbol)
	}
	if s.identitySet {
		item := s.GetExSymbol2(s.identityExchange, s.identityMarket, symbol)
		if item == nil {
			return nil, errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache", symbol, s.SymbolCount())
		}
		return item, nil
	}
	if exg.Default == nil {
		item := s.GetExSymbol2(core.ExgName, core.Market, symbol)
		if item == nil {
			return nil, errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache", symbol, s.SymbolCount())
		}
		return item, nil
	}
	return s.GetExSymbol(exg.Default, symbol)
}

// GetExSymbol resolves a symbol from this state using the exchange's market
// classification. Database loading remains an explicit composition concern;
// callers should populate the state before entering a running data path.
func (s *SymbolState) GetExSymbol(exchange banexg.BanExchange, symbol string) (*ExSymbol, *errs.Error) {
	if s == nil {
		return GetExSymbol(exchange, symbol)
	}
	if exchange == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	market, err := exchange.GetMarket(symbol)
	exgInfo := exchange.Info()
	marketType := exgInfo.MarketType
	if market != nil {
		marketType = market.Type
	}
	if !s.acceptsIdentity(exgInfo.ID, marketType) {
		return nil, errs.NewMsg(core.ErrBadConfig, "exchange %s market %s does not match symbol state identity %s:%s",
			exgInfo.ID, marketType, s.identityExchange, s.identityMarket)
	}
	item := s.GetExSymbol2(exgInfo.ID, marketType, symbol)
	if item == nil {
		if err == nil {
			err = errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache", symbol, s.SymbolCount())
		}
		return nil, err
	}
	return item, nil
}

func GetExSymbol2(exgName, market, symbol string, exgReal ...string) *ExSymbol {
	return loadDefaultSymbolState().GetExSymbol2(exgName, market, symbol, exgReal...)
}

func EnsureExSymbol(exchange, market, symbol string, exgReal ...string) (*ExSymbol, error) {
	if exchange == "" {
		return nil, fmt.Errorf("exchange is required")
	}
	if market == "" {
		return nil, fmt.Errorf("market is required")
	}
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if item := GetExSymbol2(exchange, market, symbol, exgReal...); item != nil {
		return item, nil
	}
	exs := &ExSymbol{
		Exchange: exchange,
		Market:   market,
		Symbol:   symbol,
	}
	if len(exgReal) > 0 {
		exs.ExgReal = exgReal[0]
	}
	if err := EnsureSymbols([]*ExSymbol{exs}, exchange); err != nil {
		return nil, err
	}
	if item := GetExSymbol2(exchange, market, symbol, exgReal...); item != nil {
		return item, nil
	}
	return nil, fmt.Errorf("ensure exsymbol failed for %s:%s:%s", exchange, market, symbol)
}

func makeExSymbolFromAdd(id int32, item AddSymbolsParams) *ExSymbol {
	return &ExSymbol{
		ID:       id,
		Exchange: item.Exchange,
		ExgReal:  item.ExgReal,
		Market:   item.Market,
		Symbol:   item.Symbol,
		Combined: item.Combined,
		ListMs:   item.ListMs,
		DelistMs: item.DelistMs,
		AggRules: item.AggRules,
	}
}

func EnsureExgSymbols(exchange banexg.BanExchange) *errs.Error {
	_, err := LoadMarkets(exchange, false)
	if err != nil {
		return err
	}
	exInfo := exchange.Info()
	exgId := exInfo.ID
	marMap := registrationMarkets(exInfo, exchange.GetCurMarkets(), hasConfiguredMarketSnapshot())
	exsList := make([]*ExSymbol, 0, len(marMap))
	for symbol, market := range marMap {
		delistMS := market.Expiry
		if !market.Active && delistMS == 0 {
			delistMS = btime.UTCStamp()
		}
		exsList = append(exsList, &ExSymbol{
			Exchange: exgId,
			Market:   market.Type,
			Symbol:   symbol,
			Combined: market.Combined,
			ListMs:   market.Created,
			DelistMs: delistMS,
		})
	}
	err = EnsureSymbols(exsList, exgId)
	if err != nil {
		return err
	}
	if len(exInfo.Markets) == 0 {
		// China Futures needs to call LoadMarkets again after EnsureSymbols to pass in symbols for the loading to be successful
		// 中国期货需要在EnsureSymbols后再次调用LoadMarkets传入symbols才能加载成功
		_, err = LoadMarkets(exchange, false)
	} else {
		// Mark the coins that are not returned by the exchange as delisted
		var editList []*ExSymbol
		for _, exs := range loadDefaultSymbolState().GetExSymbolsByID(exInfo.ID, exInfo.MarketType) {
			if exs.Exchange != exInfo.ID || exs.Market != exInfo.MarketType || exs.DelistMs > 0 {
				continue
			}
			if _, ok := exInfo.Markets[exs.Symbol]; !ok {
				item := *exs
				item.DelistMs = btime.UTCStamp()
				editList = append(editList, &item)
			}
		}
		if len(editList) > 0 {
			pq, conn2, err2 := Conn(nil)
			if err2 != nil {
				return err2
			}
			defer conn2.Release()
			for _, exs := range editList {
				err_ := pq.SetListMS(context.Background(), SetListMSParams{
					ID:       exs.ID,
					ListMs:   exs.ListMs,
					DelistMs: exs.DelistMs,
				})
				if err_ != nil {
					return NewDbErr(core.ErrDbExecFail, err_)
				}
			}
		}
	}
	return err
}

func registrationMarkets(exInfo *banexg.ExgInfo, current banexg.MarketMap, includeSnapshot bool) banexg.MarketMap {
	if !includeSnapshot {
		return current
	}
	for symbol, market := range exInfo.Markets {
		if market.Type == exInfo.MarketType {
			current[symbol] = market
		}
	}
	return current
}

func EnsureCurSymbols(symbols []string) *errs.Error {
	return ensureCurSymbols(loadDefaultSymbolState(), exg.Default, config.Exchange.Name, core.Market, symbols)
}

// EnsureCurSymbolsWithSymbolState registers the current exchange markets in
// an explicit symbol state. Market/exchange sessions remain an external
// capability; the resulting catalog and ID updates stay in state.
func EnsureCurSymbolsWithSymbolState(state *SymbolState, exchange banexg.BanExchange, symbols []string) *errs.Error {
	if state == nil {
		return EnsureCurSymbols(symbols)
	}
	if exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	exInfo := exchange.Info()
	if !state.acceptsIdentity(exInfo.ID, exInfo.MarketType) {
		return errs.NewMsg(core.ErrBadConfig, "exchange %s market %s does not match symbol state identity %s:%s",
			exInfo.ID, exInfo.MarketType, state.identityExchange, state.identityMarket)
	}
	return ensureCurSymbolsWithRuntimeConfig(state, exchange, exInfo.ID, exInfo.MarketType, symbols,
		&config.Data, config.GetDataDir(), nil, false)
}

// EnsureCurSymbolsWithRuntimeConfig registers current markets using the
// caller's immutable Config and Core state. Explicit pair refresh must use this
// entrypoint when market snapshots or contract reloads are configured per run.
func EnsureCurSymbolsWithRuntimeConfig(state *SymbolState, exchange banexg.BanExchange, symbols []string,
	cfg *config.Config, dataDir string, runtimeCore *core.State,
) *errs.Error {
	if state == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime symbol state is required")
	}
	if exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	exInfo := exchange.Info()
	if exInfo == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange info is required")
	}
	if !state.acceptsIdentity(exInfo.ID, exInfo.MarketType) {
		return errs.NewMsg(core.ErrBadConfig, "exchange %s market %s does not match symbol state identity %s:%s",
			exInfo.ID, exInfo.MarketType, state.identityExchange, state.identityMarket)
	}
	return ensureCurSymbolsWithRuntimeConfig(state, exchange, exInfo.ID, exInfo.MarketType, symbols,
		cfg, dataDir, runtimeCore, true)
}

func ensureCurSymbols(state *SymbolState, exchange banexg.BanExchange, exchangeName, marketType string, symbols []string) *errs.Error {
	return ensureCurSymbolsWithRuntimeConfig(state, exchange, exchangeName, marketType, symbols,
		&config.Data, config.GetDataDir(), nil, false)
}

func ensureCurSymbolsWithRuntimeConfig(state *SymbolState, exchange banexg.BanExchange, exchangeName, marketType string,
	symbols []string, cfg *config.Config, dataDir string, runtimeCore *core.State, explicit bool,
) *errs.Error {
	if state == nil {
		state = loadDefaultSymbolState()
	}
	if exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	exsList := make([]*ExSymbol, 0, len(symbols))
	marMap, err := loadMarketsWithRuntimeConfig(state, exchange, false, cfg, dataDir, runtimeCore, explicit)
	if err != nil {
		return err
	}
	for _, symbol := range symbols {
		mar, ok := marMap[symbol]
		if !ok {
			return errs.NewMsg(core.ErrInvalidSymbol, "symbol %s not found", symbol)
		}
		exsList = append(exsList, &ExSymbol{
			Exchange: exchangeName,
			Market:   marketType,
			Symbol:   symbol,
			Combined: mar.Combined,
			ListMs:   mar.Created,
			DelistMs: mar.Expiry,
		})
	}
	return state.EnsureSymbols(exsList, exchangeName)
}

func EnsureSymbols(symbols []*ExSymbol, exchanges ...string) *errs.Error {
	return loadDefaultSymbolState().EnsureSymbols(symbols, exchanges...)
}

// EnsureSymbols resolves and persists symbols using this runtime's symbol
// indexes. The database handle is bound to the same state for every cache
// update made by this operation.
func (s *SymbolState) EnsureSymbols(symbols []*ExSymbol, exchanges ...string) *errs.Error {
	if s == nil {
		return errs.NewMsg(core.ErrBadConfig, "symbol state is required")
	}
	allocator := s.sidAllocator()
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()

	var err *errs.Error
	var exgNames = make(map[string]bool)
	for _, exs := range symbols {
		if exs == nil {
			return errs.NewMsg(core.ErrBadConfig, "symbol is required")
		}
		if !s.acceptsIdentity(exs.Exchange, exs.Market) {
			return errs.NewMsg(core.ErrBadConfig, "symbol %s:%s:%s does not match symbol state identity %s:%s",
				exs.Exchange, exs.Market, exs.Symbol, s.identityExchange, s.identityMarket)
		}
		exgNames[exs.Exchange] = true
	}
	for _, name := range exchanges {
		exgNames[name] = true
	}
	pq, conn2, err2 := s.Conn(nil)
	if err2 != nil {
		return err2
	}
	defer conn2.Release()
	spq := newEnsureSymbolQueries(pq.WithSeriesSymbolState(s), s)
	if s.SymbolCount() == 0 {
		// Not yet loaded, load the information of all the underlying assets of the specified exchange
		// 尚未加载，加载指定交易所所有标的信息
		for exgId := range exgNames {
			err = spq.Queries.loadExgSymbolsLocked(s, exgId)
			if err != nil {
				return err
			}
		}
	}
	if err := reserveStateSymbols(allocator, s, exgNames); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	// Check symbols that need to be inserted
	// 检查需要插入的标的
	adds := map[string]*ExSymbol{}
	for _, exs := range symbols {
		key := exSymbolKey(exs.Exchange, exs.Market, exs.Symbol)
		item, resolveErr := resolveEnsuredSymbolLocked(allocator, s, exs)
		if resolveErr != nil {
			return errs.New(core.ErrBadConfig, resolveErr)
		}
		if item == nil {
			adds[key] = exs
		}
	}
	if len(adds) == 0 {
		return nil
	}
	// Reload and add under the allocator-owned ensure reservation. Runtimes
	// created by one Process share this boundary; independent legacy states do
	// not, and cross-process uniqueness still depends on storage constraints.
	for exgId := range exgNames {
		err = spq.Queries.loadExgSymbolsLocked(s, exgId)
		if err != nil {
			return err
		}
	}
	if err := reserveStateSymbols(allocator, s, exgNames); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	argList := make([]AddSymbolsParams, 0, len(adds))
	for _, item := range adds {
		resolved, resolveErr := resolveEnsuredSymbolLocked(allocator, s, item)
		if resolveErr != nil {
			return errs.New(core.ErrBadConfig, resolveErr)
		}
		if resolved != nil {
			continue
		}
		argList = append(argList, AddSymbolsParams{Exchange: item.Exchange, ExgReal: item.ExgReal,
			Market: item.Market, Symbol: item.Symbol, Combined: item.Combined, ListMs: item.ListMs, DelistMs: item.DelistMs,
			AggRules: item.AggRules})
	}
	_, err_ := spq.Queries.addSymbolsLocked(context.Background(), s, spq.symbols == nil, argList)
	if err_ != nil {
		errMsg := err_.Error()
		if strings.Contains(errMsg, "SQLSTATE 22001") {
			log.Error("save fail, data too lang", zap.Error(err_), zap.Any("data", argList))
		}
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	// QuestDB may not expose the inserted WAL rows to the reload below before
	// timeout. Keep the allocator reservation until a later visibility pass.
	if err := reserveStateSymbols(allocator, s, exgNames); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	for exgId := range exgNames {
		err = spq.Queries.loadExgSymbolsLocked(s, exgId)
		if err != nil {
			return err
		}
	}
	if err := reserveStateSymbols(allocator, s, exgNames); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	// 刷新Sid
	for _, exs := range symbols {
		if _, err := resolveEnsuredSymbolLocked(allocator, s, exs); err != nil {
			return errs.New(core.ErrBadConfig, err)
		}
	}
	return nil
}

func newEnsureSymbolQueries(q *Queries, state *SymbolState) *SymbolQueries {
	if state == loadDefaultSymbolState() {
		state = nil
	}
	return NewSymbolQueries(q, state)
}

func reserveStateSymbols(allocator *SIDAllocator, state *SymbolState, exchanges map[string]bool) error {
	if allocator == nil || state == nil {
		return nil
	}
	reservations := make([]sidReservation, 0)
	for _, item := range state.GetExSymbols("", "") {
		if item == nil || item.ID <= 0 || len(exchanges) != 0 && !exchanges[item.Exchange] {
			continue
		}
		reservations = append(reservations, sidReservation{
			key: exSymbolKey(item.Exchange, item.Market, item.Symbol),
			id:  item.ID,
		})
	}
	if err := allocator.reserveSIDBatch(reservations); err != nil {
		return fmt.Errorf("reserve cached exchange symbol SIDs: %w", err)
	}
	return nil
}

func resolveEnsuredSymbol(allocator *SIDAllocator, state *SymbolState, target *ExSymbol) (*ExSymbol, error) {
	if allocator == nil && state != nil {
		allocator = state.sidAllocator()
	}
	unlockEnsure := allocator.lockEnsure()
	defer unlockEnsure()
	return resolveEnsuredSymbolLocked(allocator, state, target)
}

// resolveEnsuredSymbolLocked resolves a symbol while the caller owns the
// allocator's ensure lock. Keeping this form separate avoids recursive lock
// acquisition in EnsureSymbols while preserving a safe wrapper for callers
// that do not already hold the lock.
func resolveEnsuredSymbolLocked(allocator *SIDAllocator, state *SymbolState, target *ExSymbol) (*ExSymbol, error) {
	if target == nil {
		return nil, nil
	}
	if state == nil {
		return nil, fmt.Errorf("resolve exchange symbol: symbol state is nil")
	}
	key := exSymbolKey(target.Exchange, target.Market, target.Symbol)
	item := state.GetExSymbol2(target.Exchange, target.Market, target.Symbol)
	if id := allocator.reservedSID(key); id > 0 {
		if item != nil && item.ID != id {
			return nil, fmt.Errorf("logical symbol %s is cached as sid %d, reserved as sid %d", key, item.ID, id)
		}
		if item == nil {
			reserved := *target
			reserved.ID = id
			if err := state.cacheExSymbolChecked(&reserved); err != nil {
				return nil, err
			}
			item = state.GetExSymbol2(target.Exchange, target.Market, target.Symbol)
		}
	}
	if item != nil {
		*target = *cloneExSymbol(item)
	}
	return item, nil
}

func LoadAllExSymbols() *errs.Error {
	ctx := context.Background()
	pq, conn2, err2 := Conn(ctx)
	if err2 != nil {
		return err2
	}
	defer conn2.Release()
	exgList, err_ := pq.ListExchanges(ctx)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	for _, exgId := range exgList {
		err := pq.LoadExgSymbols(exgId)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
GetAllExSymbols
Gets all the objects that have been loaded into the cache
获取已加载到缓存的所有标的
*/
func GetAllExSymbols() map[int32]*ExSymbol {
	return loadDefaultSymbolState().GetExSymbolsByID("", "")
}

func (s *ExSymbol) GetValidStart(startMS int64) int64 {
	return max(s.ListMs, startMS)
}

func (s *ExSymbol) ToShort() string {
	slashArr := strings.Split(s.Symbol, "/")
	if len(slashArr) == 1 {
		// 非数字货币，直接返回
		return s.Symbol
	}
	comArr := strings.Split(slashArr[1], ":")
	if len(comArr) == 1 {
		// 现货，直接返回
		return s.Symbol
	}
	base, quote, settle := slashArr[0], comArr[0], comArr[1]
	if !strings.HasPrefix(settle, quote) {
		// 是币本位合约，直接返回
		return s.Symbol
	}
	if quote == settle {
		return fmt.Sprintf("%s/%s.P", base, quote)
	} else {
		suffix := settle[len(quote)+1:]
		return fmt.Sprintf("%s/%s.%s", base, quote, suffix)
	}
}

func (s *ExSymbol) AggRule(col string) string {
	rules := s.AggRuleMap()
	if rule, ok := rules[col]; ok {
		return normalizeAggRule(rule)
	}
	return "last"
}

func (s *ExSymbol) AggRuleMap() map[string]string {
	if s == nil || strings.TrimSpace(s.AggRules) == "" {
		return nil
	}
	var rules map[string]string
	if err := json.Unmarshal([]byte(s.AggRules), &rules); err != nil {
		return nil
	}
	for col, rule := range rules {
		rules[col] = normalizeAggRule(rule)
	}
	return rules
}

func (s *ExSymbol) SetAggRules(rules map[string]string) error {
	if len(rules) == 0 {
		s.AggRules = ""
		return nil
	}
	clean := make(map[string]string, len(rules))
	for col, rule := range rules {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		rule = strings.ToLower(strings.TrimSpace(rule))
		if rule == "" {
			rule = "last"
		}
		clean[col] = rule
	}
	data, err := json.Marshal(clean)
	if err != nil {
		return err
	}
	s.AggRules = string(data)
	return nil
}

func normalizeAggRule(rule string) string {
	rule = strings.ToLower(strings.TrimSpace(rule))
	aggRulesMu.RLock()
	_, ok := aggRules[rule]
	aggRulesMu.RUnlock()
	if ok {
		return rule
	}
	return "last"
}

func RegisterAggRule(name string, fn AggRuleFunc) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || fn == nil {
		return false
	}
	aggRulesMu.Lock()
	aggRules[name] = fn
	aggRulesMu.Unlock()
	return true
}

func GetAggRuleFunc(name string) (AggRuleFunc, bool) {
	raw := strings.ToLower(strings.TrimSpace(name))
	name = normalizeAggRule(raw)
	if name != raw {
		return nil, false
	}
	aggRulesMu.RLock()
	fn, ok := aggRules[name]
	aggRulesMu.RUnlock()
	return fn, ok
}

func aggFirst(rows []*DataRecord, field SeriesField) (any, error) {
	return aggEdge(rows, field, true)
}

func aggLast(rows []*DataRecord, field SeriesField) (any, error) {
	return aggEdge(rows, field, false)
}

func aggEdge(rows []*DataRecord, field SeriesField, first bool) (any, error) {
	for i := 0; i < len(rows); i++ {
		idx := i
		if !first {
			idx = len(rows) - 1 - i
		}
		if rows[idx] == nil || rows[idx].Values == nil {
			continue
		}
		if val, ok := rows[idx].Values[field.Name]; ok {
			return val, nil
		}
	}
	return nil, fmt.Errorf("series field %q has no values", field.Name)
}

func aggMin(rows []*DataRecord, field SeriesField) (any, error) {
	val, ok, hasNull, err := aggFloatSeed(rows, field)
	if err != nil {
		return nil, err
	}
	if !ok {
		if hasNull {
			return nil, nil
		}
		return nil, fmt.Errorf("series field %q has no values", field.Name)
	}
	for _, row := range rows {
		cur, _, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if ok && cur < val {
			val = cur
		}
	}
	return aggNumericResult(field, val), nil
}

func aggMax(rows []*DataRecord, field SeriesField) (any, error) {
	val, ok, hasNull, err := aggFloatSeed(rows, field)
	if err != nil {
		return nil, err
	}
	if !ok {
		if hasNull {
			return nil, nil
		}
		return nil, fmt.Errorf("series field %q has no values", field.Name)
	}
	for _, row := range rows {
		cur, _, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if ok && cur > val {
			val = cur
		}
	}
	return aggNumericResult(field, val), nil
}

func aggSum(rows []*DataRecord, field SeriesField) (any, error) {
	total := 0.0
	seen := false
	hasNull := false
	for _, row := range rows {
		val, present, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if present && !ok {
			hasNull = true
			continue
		}
		if ok {
			total += val
			seen = true
		}
	}
	if !seen {
		if hasNull {
			return nil, nil
		}
		return nil, fmt.Errorf("series field %q has no values", field.Name)
	}
	return aggNumericResult(field, total), nil
}

func aggAvg(rows []*DataRecord, field SeriesField) (any, error) {
	total := 0.0
	count := 0
	hasNull := false
	for _, row := range rows {
		val, present, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if present && !ok {
			hasNull = true
			continue
		}
		if ok {
			total += val
			count++
		}
	}
	if count == 0 {
		if hasNull {
			return nil, nil
		}
		return nil, fmt.Errorf("series field %q has no values", field.Name)
	}
	return aggNumericResult(field, total/float64(count)), nil
}

func aggMid(rows []*DataRecord, field SeriesField) (any, error) {
	minVal, ok, hasNull, err := aggFloatSeed(rows, field)
	if err != nil {
		return nil, err
	}
	if !ok {
		if hasNull {
			return nil, nil
		}
		return nil, fmt.Errorf("series field %q has no values", field.Name)
	}
	maxVal := minVal
	for _, row := range rows {
		val, _, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}
	return aggNumericResult(field, (minVal+maxVal)/2), nil
}

func aggFloatSeed(rows []*DataRecord, field SeriesField) (float64, bool, bool, error) {
	hasNull := false
	for _, row := range rows {
		val, present, ok, err := aggFloatValue(row, field)
		if err != nil {
			return 0, false, hasNull, err
		}
		if present && !ok {
			hasNull = true
			continue
		}
		if ok {
			return val, true, hasNull, nil
		}
	}
	return 0, false, hasNull, nil
}

func aggFloatValue(row *DataRecord, field SeriesField) (float64, bool, bool, error) {
	if row == nil || row.Values == nil {
		return 0, false, false, nil
	}
	val, ok := row.Values[field.Name]
	if !ok {
		return 0, false, false, nil
	}
	if val == nil {
		return 0, true, false, nil
	}
	num, err := utils.ToFloat64(val)
	if err != nil {
		return 0, true, false, err
	}
	return num, true, true, nil
}

func aggNumericResult(field SeriesField, val float64) any {
	if field.Type == "int" {
		return int64(val)
	}
	return val
}

func InitListDates() *errs.Error {
	return InitListDatesWithExchange(nil, exg.Default)
}

// InitListDatesWithState initializes listing metadata in the supplied symbol
// state. A nil state preserves the legacy package-level behavior.
func InitListDatesWithState(state *SymbolState) *errs.Error {
	return InitListDatesWithExchange(state, exg.Default)
}

// InitListDatesWithExchange initializes listing metadata using an explicit
// exchange, avoiding process-global exchange lookup for runtime-owned states.
func InitListDatesWithExchange(state *SymbolState, exchange banexg.BanExchange) *errs.Error {
	if exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	if state == nil {
		state = loadDefaultSymbolState()
	}
	pq, conn2, err2 := state.Conn(nil)
	if err2 != nil {
		return err2
	}
	defer conn2.Release()
	spq := NewSymbolQueries(pq, state)
	exInfo := exchange.Info()
	exsList := state.GetExSymbols(exInfo.ID, exInfo.MarketType)
	marketMap := exchange.GetCurMarkets()
	for _, exs := range exsList {
		if exs.ListMs > 0 && exs.DelistMs > 0 {
			continue
		}
		mar, ok := marketMap[exs.Symbol]
		if !ok {
			continue
		}
		listMS, delistMS := exs.ListMs, exs.DelistMs
		changed := false
		if delistMS == 0 && mar.Expiry > 0 {
			delistMS = mar.Expiry
			changed = true
		}
		if listMS == 0 && mar.Created > 0 {
			// 只有合约有Created字段，现货需从k线计算
			listMS = mar.Created
			changed = true
		}
		if changed {
			err_ := spq.SetListMS(context.Background(), SetListMSParams{
				ID:       exs.ID,
				ListMs:   listMS,
				DelistMs: delistMS,
			})
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
		}
	}
	return nil
}

func EnsureListDates(sess *Queries, exchange banexg.BanExchange, exsMap map[int32]*ExSymbol, exsList []*ExSymbol) *errs.Error {
	return EnsureListDatesWithStateAndOptions(sess, nil, exchange, exsMap, exsList, LegacyKlineRuntimeOptions())
}

// EnsureListDatesWithState keeps listing-date discovery on an explicit symbol
// state while retaining the old helper as a legacy facade.
func EnsureListDatesWithState(sess *Queries, state *SymbolState, exchange banexg.BanExchange,
	exsMap map[int32]*ExSymbol, exsList []*ExSymbol) *errs.Error {
	return EnsureListDatesWithStateAndOptions(sess, state, exchange, exsMap, exsList, LegacyKlineRuntimeOptions())
}

// EnsureListDatesWithStateAndOptions performs low-frequency listing-date
// discovery with an explicit download/network policy. The supplied query and
// symbol state remain the only mutable owners touched by this operation.
func EnsureListDatesWithStateAndOptions(sess *Queries, state *SymbolState, exchange banexg.BanExchange,
	exsMap map[int32]*ExSymbol, exsList []*ExSymbol, options KlineRuntimeOptions) *errs.Error {
	if err := validateKlineRuntimeOptions(options); err != nil {
		return err
	}
	canDownload := options.allowDownload()
	if exchange == nil {
		if !canDownload {
			return klineDownloadDisabledError("EnsureListDates")
		}
		return errs.NewMsg(core.ErrBadConfig, "EnsureListDates: exchange is required")
	}
	if !canDownload {
		for _, exs := range exsMap {
			if exs == nil || exs.ListMs == 0 {
				return klineDownloadDisabledError("EnsureListDates")
			}
		}
		for _, exs := range exsList {
			if exs == nil || exs.ListMs == 0 {
				return klineDownloadDisabledError("EnsureListDates")
			}
		}
		return nil
	}
	exInfo := exchange.Info()
	if exInfo.MarketType != banexg.MarketSpot {
		return nil
	}
	var stateErr error
	state, stateErr = resolveQuerySymbolState(sess, state)
	if stateErr != nil {
		return errs.New(core.ErrBadConfig, stateErr)
	}
	generation := state.catalogGeneration()
	state.tryListMu.Lock()
	if state.tryListIDs == nil {
		state.tryListIDs = make(map[int32]bool)
	}
	candidates := make([]*ExSymbol, 0, (len(exsMap)+len(exsList))/4)
	addCandidate := func(v *ExSymbol) {
		if v == nil || v.ListMs != 0 {
			return
		}
		if state.tryListIDs[v.ID] {
			return
		}
		state.tryListIDs[v.ID] = true
		candidates = append(candidates, cloneExSymbol(v))
	}
	for _, v := range exsMap {
		addCandidate(v)
	}
	for _, v := range exsList {
		addCandidate(v)
	}
	state.tryListMu.Unlock()
	if len(candidates) == 0 {
		return nil
	}
	hasFetch := !options.NetDisable && exchange.HasApi(banexg.ApiFetchOHLCV, exInfo.MarketType)
	var prgBar *utils.PrgBar
	cacheNum := len(candidates)
	if cacheNum > 10 && hasFetch {
		costSecs := float64(cacheNum) / 6
		log.Info("calculating listDates for new symbols", zap.Int("num", cacheNum),
			zap.Int("secs", int(costSecs)))
		prgBar = utils.NewPrgBar(cacheNum, "InitListDates")
		defer prgBar.Close()
	}
	var err *errs.Error
	for i, exs := range candidates {
		if !state.hasCatalogGeneration(generation) {
			return nil
		}
		if prgBar != nil {
			prgBar.Add(1)
		}
		startMS := core.MSMinStamp
		var listMS int64
		if hasFetch {
			var klines []*banexg.Kline
			klines, err = exchange.FetchOHLCV(exs.Symbol, "1m", startMS, 1, nil)
			if len(klines) > 0 {
				listMS = klines[0].Time
			}
		} else {
			var rows []*DataSeries
			rows, err = sess.QuerySeries(exs, "1m", startMS, 0, 1, false)
			if len(rows) > 0 {
				listMS = rows[0].TimeMS
			}
		}
		if err != nil {
			return err
		}
		if listMS > 0 {
			unlockCatalog, current := state.lockCatalogGeneration(generation)
			if !current {
				return nil
			}
			err_ := sess.setListMS(context.Background(), state, SetListMSParams{
				ID:       exs.ID,
				ListMs:   listMS,
				DelistMs: exs.DelistMs,
			}, exs)
			unlockCatalog()
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
			if latest := state.GetSymbolByID(exs.ID); latest != nil {
				exs = latest
				candidates[i] = latest
			}
		}
	}
	// Metadata updates use copy-on-write snapshots. Refresh caller-owned
	// containers so filters and setup code observe the new snapshot without
	// mutating an object that may already be read by another goroutine.
	for id := range exsMap {
		if item := state.GetSymbolByID(id); item != nil {
			exsMap[id] = item
		}
	}
	for i, item := range exsList {
		if item != nil {
			if latest := state.GetSymbolByID(item.ID); latest != nil {
				exsList[i] = latest
			}
		}
	}
	return nil
}

func ParseShort(exgName, short string) (*ExSymbol, *errs.Error) {
	return loadDefaultSymbolState().ParseShort(exgName, short)
}

// ParseShort resolves a display symbol using only this symbol catalog.
func (state *SymbolState) ParseShort(exgName, short string) (*ExSymbol, *errs.Error) {
	if state == nil {
		return nil, errs.NewMsg(core.ErrInvalidSymbol, "symbol state is required")
	}
	slashArr := strings.Split(short, "/")
	var symbol string
	var market = banexg.MarketSpot
	if len(slashArr) > 1 {
		// 对数字货币 BTC/USDT:USDT BTC/USDT.P BTC/USDT.2309
		dotArr := strings.Split(slashArr[1], ".")
		quote := dotArr[0]
		var suffix = ""
		if len(dotArr) > 1 {
			suffix = quote
			market = banexg.MarketLinear
			if !strings.EqualFold(dotArr[1], "p") {
				suffix += "-" + dotArr[1]
			}
		} else {
			comArr := strings.Split(quote, ":")
			if len(comArr) > 1 {
				quote, suffix = comArr[0], comArr[1]
				if strings.HasPrefix(suffix, quote) {
					market = banexg.MarketLinear
				} else {
					market = banexg.MarketInverse
				}
			}
		}
		if market == banexg.MarketSpot {
			symbol = fmt.Sprintf("%s/%s", slashArr[0], quote)
		} else {
			symbol = fmt.Sprintf("%s/%s:%s", slashArr[0], quote, suffix)
		}
	} else {
		symbol = short
	}
	item := state.GetExSymbol2(exgName, market, symbol)
	if item == nil {
		exgMarket := fmt.Sprintf("%s:%s", exgName, market)
		pairNum := state.MarketCount(exgName, market)
		if pairNum == 0 {
			pq2, conn2, err2 := state.Conn(nil)
			if err2 != nil {
				return nil, err2
			}
			err := pq2.WithSymbolState(state).LoadExgSymbols(exgName)
			conn2.Release()
			if err != nil {
				return nil, err
			}
			item = state.GetExSymbol2(exgName, market, symbol)
			if item != nil {
				return item, nil
			}
			pairNum = state.MarketCount(exgName, market)
		}
		err := errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache for %s", symbol, pairNum, exgMarket)
		return nil, err
	}
	return item, nil
}

func AddHourSymbol(exs *ExSymbol) {
	loadDefaultSymbolState().AddHourSymbol(exs)
}

func Sub1mSymbol(pair string) {
	loadDefaultSymbolState().Sub1mSymbol(pair)
}

func ResetSubSymbol() {
	loadDefaultSymbolState().ResetSubSymbol()
}

func GetHourOnlySymbols() map[int32]*ExSymbol {
	return loadDefaultSymbolState().GetHourOnlySymbols()
}
