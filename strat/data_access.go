package strat

import (
	"fmt"
	"strings"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
)

var opaqueExchangeCache sync.Map // map[string]bool

// NewDataSub derives a subscription identity from an independent series.
// A KLineSeriesInfo can have an extension label as its Name; for those columns,
// declare DataSub{Source: "kline", TimeFrame: info.TimeFrame} explicitly instead.
func NewDataSub(info *orm.SeriesInfo) *DataSub {
	if info == nil {
		return nil
	}
	return &DataSub{Source: orm.NormalizeSeriesSource(info.Name), TimeFrame: info.TimeFrame}
}

// NewRefKlineSub builds a read-only K-line subscription for an auxiliary timeframe
// or a cross-symbol reference pair (such as "BTC/USDT:USDT" or "BTC").
// An empty symbol or "_cur_" targets the current StratJob symbol; non-empty
// symbols are normalized to the job's exchange/market and resolved or ensured
// in SymbolState without creating a trading StratJob.
func NewRefKlineSub(s *StratJob, symbol, timeframe string, warmupNum ...int) *DataSub {
	if s == nil {
		warm := 0
		if len(warmupNum) > 0 && warmupNum[0] > 0 {
			warm = warmupNum[0]
		}
		var exs *orm.ExSymbol
		sym := strings.TrimSpace(symbol)
		if sym != "" && sym != "_cur_" {
			exs, _ = orm.GetExSymbolCur(sym)
			if exs == nil {
				exs = &orm.ExSymbol{Exchange: core.ExgName, Market: core.Market, Symbol: sym}
			}
		}
		return &DataSub{
			Source:    orm.SeriesSourceKline,
			ExSymbol:  exs,
			TimeFrame: timeframe,
			WarmupNum: warm,
		}
	}
	return s.RefKlineSub(symbol, timeframe, warmupNum...)
}

// RefKlineSub builds a read-only K-line subscription for an auxiliary timeframe
// (symbol == "" or "_cur_") or another symbol in the same exchange and market
// (e.g. "BTC" or "BTC/USDT:USDT"). When warmupNum is omitted, WarmupNum defaults
// to max(0, s.Strat.WarmupNum) and can be customized on the returned *DataSub.
func (s *StratJob) RefKlineSub(symbol, timeframe string, warmupNum ...int) *DataSub {
	warm := 0
	if len(warmupNum) > 0 {
		warm = max(0, warmupNum[0])
	} else if s != nil && s.Strat != nil && s.Strat.WarmupNum > 0 {
		warm = s.Strat.WarmupNum
	}
	if s == nil {
		return NewRefKlineSub(nil, symbol, timeframe, warm)
	}
	sym := strings.TrimSpace(symbol)
	if sym == "" || sym == "_cur_" {
		return &DataSub{
			Source:    orm.SeriesSourceKline,
			ExSymbol:  s.resolveRefExSymbol("", true),
			TimeFrame: timeframe,
			WarmupNum: warm,
		}
	}
	exs := s.resolveRefExSymbol(sym, true)
	return &DataSub{
		Source:    orm.SeriesSourceKline,
		ExSymbol:  exs,
		TimeFrame: timeframe,
		WarmupNum: warm,
	}
}

// RefEnv returns the synchronized *ta.BarEnv of a subscribed auxiliary timeframe
// (symbol == "" or "_cur_") or cross-symbol reference K-line stream from s.DataHub.
// It returns nil until at least one K-line bar of that stream has arrived; once
// non-nil, Open, High, Low, Close, Volume, Quote, BuyVolume, and TradeNum are
// all guaranteed non-nil.
func (s *StratJob) RefEnv(symbol, timeframe string) *ta.BarEnv {
	if s == nil || timeframe == "" {
		return nil
	}
	sym := strings.TrimSpace(symbol)
	if (sym == "" || sym == "_cur_") && s.Symbol != nil && timeframe == s.TimeFrame {
		if s.DataHub != nil {
			if df := s.DataHub.Get(timeframe, orm.SeriesSourceKline, s.Symbol.ID); df != nil {
				if env := df.BarEnv(); env != nil {
					return env
				}
			}
		}
		if s.Env != nil && s.Env.Close != nil {
			return s.Env
		}
		return nil
	}
	exs := s.resolveRefExSymbol(sym, false)
	if exs != nil && exs.ID > 0 {
		if s.DataHub == nil {
			return nil
		}
		return s.DataHub.Get(timeframe, orm.SeriesSourceKline, exs.ID).BarEnv()
	}
	if s.DataHub != nil && s.effectiveSymbols() == nil && sym != "" && sym != "_cur_" {
		_, _, normSym, altSym := s.normalizeRefSymbolCandidates(sym)
		return s.DataHub.findKlineBySymbol(timeframe, normSym, altSym, sym).BarEnv()
	}
	return nil
}

// Data returns the DataHub view identified by sub. A nil ExSymbol uses the
// current job's already-bound symbol. When the job has a runtime catalog,
// explicit symbols are checked against it, without consulting a global catalog.
func (s *StratJob) Data(sub *DataSub) *DataFields {
	if s == nil || sub == nil {
		return nil
	}
	exs := sub.ExSymbol
	if exs == nil {
		if s.Symbol == nil {
			return nil
		}
		return s.DataHub.Get(sub.TimeFrame, sub.Source, s.Symbol.ID)
	}
	if symbols := s.effectiveSymbols(); symbols != nil {
		canonical := symbols.GetSymbolByID(exs.ID)
		if canonical == nil || (canonical != exs && (canonical.Exchange != exs.Exchange || canonical.Market != exs.Market || canonical.Symbol != exs.Symbol)) {
			return nil
		}
		exs = canonical
	}
	return s.DataHub.Get(sub.TimeFrame, sub.Source, exs.ID)
}

func (s *StratJob) effectiveSymbols() *orm.SymbolState {
	if s == nil {
		return nil
	}
	if s.symbols != nil {
		return s.symbols
	}
	if s.strategyState != nil && s.strategyState != legacyState {
		return s.strategyState.Symbols
	}
	return nil
}

func (s *StratJob) effectiveExchange() banexg.BanExchange {
	if s == nil {
		return nil
	}
	if s.runtimeExchange != nil {
		return s.runtimeExchange
	}
	if s.strategyState != nil && s.strategyState != legacyState {
		if s.strategyState.Exchange != nil {
			return s.strategyState.Exchange
		}
		if hooks := s.strategyState.PairUpdateHooks(); hooks.Exchange != nil {
			return hooks.Exchange
		}
		return nil
	}
	if hooks := SnapshotPairUpdateHooks(); hooks.Exchange != nil {
		return hooks.Exchange
	}
	if s.symbols == nil && s.runtimeCore == nil {
		return exg.Default
	}
	return nil
}

func (s *StratJob) effectiveConfig() *config.Config {
	if s == nil {
		return nil
	}
	if s.strategyState != nil && s.strategyState != legacyState {
		return s.strategyState.Config
	}
	if s.symbols == nil && s.runtimeCore == nil {
		return &config.Data
	}
	return nil
}

func (s *StratJob) effectiveCore() *core.State {
	if s == nil {
		return nil
	}
	if s.runtimeCore != nil {
		return s.runtimeCore
	}
	if s.strategyState != nil && s.strategyState != legacyState {
		return s.strategyState.Core
	}
	return nil
}

func (s *StratJob) refMarketContext() (exgName, market, quote, settle string) {
	symbols := s.effectiveSymbols()
	if s.Symbol != nil {
		sym := s.Symbol
		if symbols != nil && sym.ID > 0 {
			if canonical := symbols.GetSymbolByID(sym.ID); canonical != nil {
				sym = canonical
			}
		}
		exgName = sym.Exchange
		market = sym.Market
		if sym.Symbol != "" {
			base0, q0, st0, _ := core.SplitSymbol(sym.Symbol)
			quote = q0
			if st0 != "" && !(market == banexg.MarketInverse && st0 == base0) {
				settle = st0
			}
		}
	}
	if cfg := s.effectiveConfig(); cfg != nil {
		if exgName == "" && cfg.Exchange != nil {
			exgName = cfg.Exchange.Name
		}
		if market == "" {
			market = cfg.MarketType
		}
		if quote == "" && len(cfg.StakeCurrency) > 0 {
			quote = cfg.StakeCurrency[0]
		}
	}
	if coreState := s.effectiveCore(); coreState != nil {
		if exgName == "" {
			exgName = coreState.ExgName
		}
		if market == "" {
			market = coreState.Market
		}
	}
	if exgObj := s.effectiveExchange(); exgObj != nil {
		if info := exgObj.Info(); info != nil {
			if exgName == "" {
				exgName = info.ID
			}
			if market == "" {
				market = info.MarketType
			}
		}
	}
	if (exgName == "" || market == "") && symbols != nil {
		for _, item := range symbols.GetExSymbols("", "") {
			if item != nil {
				if exgName == "" {
					exgName = item.Exchange
				}
				if market == "" {
					market = item.Market
				}
				if quote == "" && item.Symbol != "" {
					_, quote, _, _ = core.SplitSymbol(item.Symbol)
				}
				break
			}
		}
	}
	isExplicit := (s.strategyState != nil && s.strategyState != legacyState) || s.symbols != nil || s.runtimeCore != nil
	if !isExplicit {
		if exgName == "" {
			exgName = core.ExgName
		}
		if market == "" {
			market = core.Market
		}
	}
	return exgName, market, quote, settle
}

func (s *StratJob) usesOpaqueSymbols(exgName string) bool {
	if s != nil {
		if s.Symbol != nil && strings.Contains(s.Symbol.Symbol, "/") {
			return false
		}
		if exgObj := s.effectiveExchange(); exgObj != nil {
			_, ok := exgObj.(banexg.PriceSymbolCapability)
			return ok
		}
	}
	exgName = strings.TrimSpace(exgName)
	if exgName == "" {
		return false
	}
	if cached, ok := opaqueExchangeCache.Load(exgName); ok {
		return cached.(bool)
	}
	opaque := config.ExchangeUsesOpaqueSymbols(exgName)
	opaqueExchangeCache.Store(exgName, opaque)
	return opaque
}

func (s *StratJob) normalizeRefSymbolCandidates(symbol string) (exgName, market, primary, alt string) {
	exgName, market, quote, settle := s.refMarketContext()
	symbol = strings.TrimSpace(symbol)
	if symbol == "" || symbol == "_cur_" {
		if s.Symbol != nil {
			primary = s.Symbol.Symbol
		}
		return exgName, market, primary, ""
	}
	primary = symbol
	if !strings.Contains(symbol, "/") && !s.usesOpaqueSymbols(exgName) && quote != "" {
		switch market {
		case banexg.MarketSpot:
			primary = fmt.Sprintf("%s/%s", symbol, quote)
		case banexg.MarketLinear:
			st := settle
			if st == "" {
				st = quote
			}
			primary = fmt.Sprintf("%s/%s:%s", symbol, quote, st)
		case banexg.MarketInverse:
			primary = fmt.Sprintf("%s/%s:%s", symbol, quote, symbol)
		}
	} else if market == banexg.MarketLinear && strings.Contains(symbol, "/") && !strings.Contains(symbol, ":") {
		_, q, _, _ := core.SplitSymbol(symbol)
		st := settle
		if st == "" {
			st = q
		}
		if st != "" {
			alt = fmt.Sprintf("%s:%s", symbol, st)
		}
	}
	return exgName, market, primary, alt
}

func lookupSymbolInState(symbols *orm.SymbolState, exgName, market, cand string) *orm.ExSymbol {
	if symbols == nil || cand == "" {
		return nil
	}
	if exgName != "" && market != "" {
		if item := symbols.GetExSymbol2(exgName, market, cand); item != nil {
			return item
		}
	}
	if item, err := symbols.GetExSymbolCur(cand); err == nil && item != nil {
		return item
	}
	return nil
}

func (s *StratJob) resolveRefExSymbol(symbol string, ensure bool) *orm.ExSymbol {
	if s == nil {
		return nil
	}
	symbols := s.effectiveSymbols()
	symbol = strings.TrimSpace(symbol)
	if symbol == "" || symbol == "_cur_" {
		if s.Symbol == nil {
			return nil
		}
		if symbols != nil {
			if canonical := canonicalDataSubSymbol(symbols, s.Symbol); canonical != nil {
				return canonical
			}
		}
		return s.Symbol
	}

	exgName, market, primary, alt := s.normalizeRefSymbolCandidates(symbol)
	candidates := []string{primary}
	if alt != "" && alt != primary {
		candidates = append(candidates, alt)
	}

	if symbols != nil {
		for _, cand := range candidates {
			if item := lookupSymbolInState(symbols, exgName, market, cand); item != nil {
				return item
			}
		}
		if ensure && s.inspectEffect == nil {
			exgObj := s.effectiveExchange()
			cfg := s.effectiveConfig()
			coreState := s.effectiveCore()
			for _, cand := range candidates {
				if cand == "" {
					continue
				}
				if symbols.Storage() != nil {
					if exgObj != nil {
						if cfg != nil {
							_ = orm.EnsureCurSymbolsWithRuntimeConfigRetired(symbols, exgObj, []string{cand}, cfg, "", coreState, true)
						} else {
							_ = orm.EnsureCurSymbolsWithSymbolState(symbols, exgObj, []string{cand})
						}
					}
					if lookupSymbolInState(symbols, exgName, market, cand) == nil && exgName != "" && market != "" {
						_ = symbols.EnsureSymbols([]*orm.ExSymbol{{Exchange: exgName, Market: market, Symbol: cand}}, exgName)
					}
				} else if exgObj != nil {
					if mar, err := exgObj.GetMarket(cand); err == nil && mar != nil {
						eName, mType := exgName, market
						if info := exgObj.Info(); info != nil && eName == "" {
							eName = info.ID
						}
						if mType == "" {
							mType = mar.Type
						}
						symStr := mar.Symbol
						if symStr == "" {
							symStr = cand
						}
						sid := symbols.NextSID()
						_ = symbols.CacheExSymbolChecked(&orm.ExSymbol{
							ID:       sid,
							Exchange: eName,
							Market:   mType,
							Symbol:   symStr,
							Combined: mar.Combined,
							ListMs:   mar.Created,
							DelistMs: mar.Expiry,
						})
					}
				}
				if item := lookupSymbolInState(symbols, exgName, market, cand); item != nil {
					return item
				}
			}
		}
		if ensure {
			target := primary
			if alt != "" && market == banexg.MarketLinear {
				target = alt
			}
			return &orm.ExSymbol{Exchange: exgName, Market: market, Symbol: target}
		}
		return nil
	}

	isExplicit := (s.strategyState != nil && s.strategyState != legacyState) || s.runtimeCore != nil
	if !isExplicit {
		for _, cand := range candidates {
			if exgName != "" && market != "" {
				if item := orm.GetExSymbol2(exgName, market, cand); item != nil {
					return item
				}
			}
			if item, err := orm.GetExSymbolCur(cand); err == nil && item != nil {
				return item
			}
		}
		if ensure && s.inspectEffect == nil {
			for _, cand := range candidates {
				if exg.Default != nil {
					_ = orm.EnsureCurSymbols([]string{cand})
				} else if exgName != "" && market != "" {
					_, _ = orm.EnsureExSymbol(exgName, market, cand)
				}
				if exgName != "" && market != "" {
					if item := orm.GetExSymbol2(exgName, market, cand); item != nil {
						return item
					}
				}
				if item, err := orm.GetExSymbolCur(cand); err == nil && item != nil {
					return item
				}
			}
		}
	}
	if ensure {
		target := primary
		if alt != "" && market == banexg.MarketLinear {
			target = alt
		}
		return &orm.ExSymbol{Exchange: exgName, Market: market, Symbol: target}
	}
	return nil
}
