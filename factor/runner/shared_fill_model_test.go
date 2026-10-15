package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/execution"
)

func TestSharedPaperFillModelRejectsBeforeInitialization(t *testing.T) {
	for _, models := range [][2]execution.PaperFillModel{
		{execution.PaperMidpoint, execution.PaperBidAsk},
		{execution.PaperBidAsk, execution.PaperMidEntryMarketExit},
		{execution.PaperMidpoint, "unknown"},
		{"unknown", "unknown"},
	} {
		t.Run(string(models[0])+"/"+string(models[1]), func(t *testing.T) {
			c := paperConfig(t, archiveConfig(t, false))
			root := filepath.Join(t.TempDir(), "uninitialized")
			c.Execution.StorePath = filepath.Join(root, "execution.db")
			c.Execution.SenderLeaseDir = filepath.Join(root, "lease")
			c.Execution.PaperFillModel = models[0]
			other := c
			other.StrategyID += "-other"
			other.Execution.PaperFillModel = models[1]
			// A separate valid account must not initialize before preflight fails.
			valid := c
			valid.AccountID += "-valid"
			valid.Execution.PaperFillModel = execution.PaperMidpoint
			calls := 0
			factory := func(execution.AccountKey, execution.SharedExecutionOptions) (*execution.SharedAccountBorrow, error) {
				calls++
				return nil, errors.New("unexpected initialization")
			}
			sinks, close, err := NewPaperSinksWithAccounts(context.Background(), []Config{valid, c, other}, factory)
			if err == nil || !strings.Contains(err.Error(), "fill model") {
				t.Fatalf("expected fill model preflight error, got %v", err)
			}
			if calls != 0 || sinks != nil || close != nil {
				t.Fatalf("initialized invalid configuration: calls=%d sinks=%v cleanup=%v", calls, sinks, close != nil)
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("preflight left resources: %v", err)
			}
		})
	}
}

func TestSharedPaperFillModelCompatibleStrategiesShareAdapter(t *testing.T) {
	for _, models := range [][2]execution.PaperFillModel{
		{execution.PaperBidAsk, execution.PaperBidAsk},
		{execution.PaperMidEntryMarketExit, execution.PaperMidEntryMarketExit},
		{"", execution.PaperMidpoint},
		{execution.PaperMidpoint, ""},
	} {
		t.Run(string(models[0])+"/"+string(models[1]), func(t *testing.T) {
			c := paperConfig(t, archiveConfig(t, false))
			c.Execution.PaperFillModel = models[0]
			other := c
			other.StrategyID += "-other"
			other.Execution.PaperFillModel = models[1]
			sinks, close, err := NewPaperSinks(context.Background(), []Config{c, other})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := close(); err != nil {
					t.Error(err)
				}
			}()
			first, second := sinks[0].(*AccountSink), sinks[1].(*AccountSink)
			if first.Paper != second.Paper || first.Account.Service() != second.Account.Service() {
				t.Fatal("compatible strategies did not share execution owner and adapter")
			}
		})
	}
}
