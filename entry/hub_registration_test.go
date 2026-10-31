package entry

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

func TestHubAdditionalRobotRegistrationPreservesAccountBaseline(t *testing.T) {
	ctx := context.Background()
	request, initial := hubConfigFixture()
	registry := &execution.AccountRegistry{}
	owner, err := registry.Acquire(request.Account)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	paper, err := execution.NewPaperAdapter(initial.PaperCash, initial.PaperFee, initial.PaperSlippage)
	if err != nil {
		t.Fatal(err)
	}
	service, err := execution.NewSharedAccount(owner, execution.SharedExecutionOptions{Memory: true, Adapter: paper, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	borrow := service.Borrow()
	defer borrow.Release()
	if err := borrow.RegisterRiskPolicy("hub-v1", initial.Risk); err != nil {
		t.Fatal(err)
	}
	if err := borrow.BootstrapCapital(ctx, initial.Capital, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterAccountQuotes([]execution.Instrument{initial.Instruments[0].Instrument}, func(_ context.Context, _ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000}, nil
	}, func() int64 { return time.Now().UnixMilli() }); err != nil {
		t.Fatal(err)
	}
	if err := borrow.Reconcile("ready", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	created := 0
	hub, err := execution.NewHub(func(context.Context, execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		created++
		return execution.HubAccountBinding{Account: service, Shutdown: func() error { return nil }, RegisterAdditional: hubAdditionalRegistration(service, initial)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	directory, err := os.MkdirTemp("", "banhub-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	socket := filepath.Join(directory, "hub.sock")
	server, err := execution.ListenHubUnix(socket, hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	first, second := execution.NewHubClient(socket), execution.NewHubClient(socket)
	defer first.Close()
	defer second.Close()
	// Both robots use the ordinary full registration RPC.
	request.Configuration, _ = json.Marshal(initial)
	if err := first.Register(ctx, request); err != nil {
		t.Fatal(err)
	}
	additional := initial
	additional.Capital = map[execution.StrategyID]decimal.Decimal{"second": decimal.NewFromInt(200)}
	additional.Risk.StrategyGrossLimits = map[execution.StrategyID]decimal.Decimal{"second": decimal.NewFromInt(500)}
	request.Configuration, _ = json.Marshal(additional)
	if err := second.Register(ctx, request); err != nil {
		t.Fatal(err)
	}
	accepted, err := borrow.Snapshot(ctx)
	if err != nil || !accepted.UnassignedCash.Equal(decimal.NewFromInt(700)) || !accepted.SyntheticStrategyCash["second"].Equal(decimal.NewFromInt(200)) || created != 1 {
		t.Fatal("additional robot did not share original owner", accepted, err, created)
	}
	if err := second.Register(ctx, request); err != nil {
		t.Fatal(err)
	}
	after, _ := borrow.Snapshot(ctx)
	if !reflect.DeepEqual(accepted, after) {
		t.Fatal("registration retry allocated twice")
	}
	for name, mutate := range map[string]func(*HubAccountConfig){
		"account-risk": func(c *HubAccountConfig) { c.Risk.MaxVirtualGross = decimal.NewFromInt(2000) },
		"paper-cash":   func(c *HubAccountConfig) { c.PaperCash = decimal.NewFromInt(2000) },
		"capital": func(c *HubAccountConfig) {
			c.Capital = map[execution.StrategyID]decimal.Decimal{"second": decimal.NewFromInt(201)}
		},
		"universe": func(c *HubAccountConfig) {
			c.Instruments = slicesCloneHubUnits(c.Instruments)
			c.Instruments[0].Symbol = "ETH/USDT:USDT"
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := additional
			mutate(&changed)
			request.Configuration, _ = json.Marshal(changed)
			if err := second.Register(ctx, request); err == nil {
				t.Fatal("immutable registration changed")
			}
		})
	}
}

func slicesCloneHubUnits(units []execution.BanexgInstrument) []execution.BanexgInstrument {
	return append([]execution.BanexgInstrument(nil), units...)
}
