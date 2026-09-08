package core

import (
	"sync"
	"sync/atomic"
)

type symbolPartsCacheValue struct {
	pair  string
	parts [4]string
	once  sync.Once
	valid atomic.Bool
}

// SymbolParserStrategy supplies exchange-owned symbol semantics. It is called
// once per pair and parser generation, on cache misses.
type SymbolParserStrategy func(pair string) [4]string

// SymbolParserStrategyWithError is used at composition boundaries where an
// adapter can reject a symbol. A rejected result is never retained in the
// successful hot cache, so a later metadata refresh can retry it.
type SymbolParserStrategyWithError func(pair string) ([4]string, error)

type symbolParserCache struct {
	parse     func(string) ([4]string, bool)
	isDefault bool
	values    sync.Map
	hot       atomic.Pointer[symbolPartsCacheValue]
	warm      atomic.Pointer[symbolPartsCacheValue]
}

// SymbolParser keeps symbol parsing state local to one Runtime.
type SymbolParser struct {
	cache atomic.Pointer[symbolParserCache]
}

func NewSymbolParser(_ string) *SymbolParser {
	return NewSymbolParserWithStrategy("", nil)
}

// NewSymbolParserWithStrategy keeps the exchange-name argument for source
// compatibility. Parsing semantics come only from parse; the name is never
// consulted by the common parser.
func NewSymbolParserWithStrategy(_ string, parse SymbolParserStrategy) *SymbolParser {
	parser := &SymbolParser{}
	parser.cache.Store(newSymbolParserCache(parse))
	return parser
}

// NewSymbolParserWithErrorStrategy installs an error-aware parser. Errors are
// represented internally as a cache miss; Split keeps its allocation-free,
// four-string API for existing callers while allowing the next call to retry.
func NewSymbolParserWithErrorStrategy(_ string, parse SymbolParserStrategyWithError) *SymbolParser {
	parser := &SymbolParser{}
	parser.cache.Store(newSymbolParserErrorCache(parse))
	return parser
}

func (p *SymbolParser) SetExchangeName(_ string) {
	p.SetStrategy("", nil)
}

func (p *SymbolParser) SetStrategy(_ string, parse SymbolParserStrategy) {
	if p == nil {
		return
	}
	for {
		cache := p.cache.Load()
		if cache != nil && cache.isDefault && parse == nil {
			return
		}
		next := newSymbolParserCache(parse)
		if p.cache.CompareAndSwap(cache, next) {
			return
		}
	}
}

func (p *SymbolParser) Split(pair string) (string, string, string, string) {
	if p == nil {
		return SplitSymbol(pair)
	}
	cache := p.cache.Load()
	if cache == nil {
		initial := newSymbolParserCache(nil)
		if p.cache.CompareAndSwap(nil, initial) {
			cache = initial
		} else {
			cache = p.cache.Load()
		}
	}
	value := cache.hot.Load()
	if value == nil || value.pair != pair {
		value = cache.warm.Load()
		if value != nil && value.pair == pair {
			return value.parts[0], value.parts[1], value.parts[2], value.parts[3]
		}
		if cached, ok := cache.values.Load(pair); ok {
			value = cached.(*symbolPartsCacheValue)
		} else {
			created := &symbolPartsCacheValue{pair: pair}
			cached, _ := cache.values.LoadOrStore(pair, created)
			value = cached.(*symbolPartsCacheValue)
		}
		value.once.Do(func() {
			parts, ok := cache.parse(pair)
			if ok {
				value.parts = parts
				value.valid.Store(true)
			}
		})
		if !value.valid.Load() {
			cache.values.CompareAndDelete(pair, value)
			return "", "", "", ""
		}
		cache.warm.Store(cache.hot.Load())
		cache.hot.Store(value)
	}
	return value.parts[0], value.parts[1], value.parts[2], value.parts[3]
}

func newSymbolParserCache(parse SymbolParserStrategy) *symbolParserCache {
	isDefault := parse == nil
	if parse == nil {
		parse = splitSymbolParts
	}
	return &symbolParserCache{
		parse:     func(pair string) ([4]string, bool) { return parse(pair), true },
		isDefault: isDefault,
	}
}

func newSymbolParserErrorCache(parse SymbolParserStrategyWithError) *symbolParserCache {
	isDefault := parse == nil
	if parse == nil {
		parse = func(pair string) ([4]string, error) { return splitSymbolParts(pair), nil }
	}
	return &symbolParserCache{
		parse: func(pair string) ([4]string, bool) {
			parts, err := parse(pair)
			return parts, err == nil
		},
		isDefault: isDefault,
	}
}
