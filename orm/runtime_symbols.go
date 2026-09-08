package orm

// InstallFrozenExSymbols replaces the process-local symbol cache for an
// isolated runtime inspection. It never reads or writes persistent storage.
func InstallFrozenExSymbols(items []*ExSymbol) (func(), error) {
	next := NewSymbolState()
	if err := next.SetExSymbols(items); err != nil {
		return nil, err
	}
	previous := swapDefaultSymbolState(next)
	return func() {
		swapDefaultSymbolState(previous)
	}, nil
}
