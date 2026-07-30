package orm

import "fmt"

// InstallFrozenExSymbols replaces the process-local symbol cache for an
// isolated runtime inspection. It never reads or writes persistent storage.
func InstallFrozenExSymbols(items []*ExSymbol) (func(), error) {
	symbolLock.Lock()
	defer symbolLock.Unlock()

	oldKeys := keySymbolMap
	oldIDs := idSymbolMap
	oldMarkets := marketMap
	oldMaxSID := maxSid

	keySymbolMap = make(map[string]*ExSymbol, len(items))
	idSymbolMap = make(map[int32]*ExSymbol, len(items))
	marketMap = make(map[string]int)
	maxSid = 0
	seenKeys := make(map[string]bool, len(items))
	for _, item := range items {
		if item == nil || item.ID <= 0 || item.Exchange == "" || item.Market == "" || item.Symbol == "" {
			restoreSymbolCache(oldKeys, oldIDs, oldMarkets, oldMaxSID)
			return nil, fmt.Errorf("invalid frozen exchange symbol")
		}
		key := exSymbolKey(item.Exchange, item.Market, item.Symbol, item.ExgReal)
		if idSymbolMap[item.ID] != nil || seenKeys[key] {
			restoreSymbolCache(oldKeys, oldIDs, oldMarkets, oldMaxSID)
			return nil, fmt.Errorf("duplicate frozen exchange symbol: sid=%d key=%s", item.ID, key)
		}
		seenKeys[key] = true
		copyItem := *item
		cacheExSymbol(&copyItem)
	}

	return func() {
		symbolLock.Lock()
		defer symbolLock.Unlock()
		restoreSymbolCache(oldKeys, oldIDs, oldMarkets, oldMaxSID)
	}, nil
}

func restoreSymbolCache(keys map[string]*ExSymbol, ids map[int32]*ExSymbol, markets map[string]int, sid int32) {
	keySymbolMap = keys
	idSymbolMap = ids
	marketMap = markets
	maxSid = sid
}
