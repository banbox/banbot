package orm

import (
	"testing"

	"github.com/banbox/banbot/config"
)

func TestParseVerifyArgsUsesProvidedSymbolState(t *testing.T) {
	state := NewSymbolStateWithIdentity("runtime", "spot")
	if err := state.CacheExSymbolChecked(&ExSymbol{ID: 17, Exchange: "runtime", Market: "spot", Symbol: "BOUND/USDT"}); err != nil {
		t.Fatal(err)
	}
	args, err := ParseVerifyArgsWithSymbolState(&config.CmdArgs{Pairs: []string{"BOUND/USDT"}}, state, "runtime", "spot")
	if err != nil {
		t.Fatal(err)
	}
	if len(args.Sids) != 1 || args.Sids[0] != 17 {
		t.Fatalf("resolved sids = %v, want [17]", args.Sids)
	}
}

func TestVerifyDataRangesRequiresExplicitDependencies(t *testing.T) {
	if _, err := VerifyDataRangesWithQueries(nil, NewSymbolState(), &VerifyArgs{}); err == nil {
		t.Fatal("nil queries were accepted")
	}
	if _, err := VerifyDataRangesWithQueries(New(nil), nil, &VerifyArgs{}); err == nil {
		t.Fatal("nil symbol state was accepted")
	}
}
