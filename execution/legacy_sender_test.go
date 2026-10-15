package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type legacyWriteExchange struct {
	banexg.BanExchange
	calls    []string
	accounts []string
}

func (e *legacyWriteExchange) record(method string, p map[string]any) {
	e.calls = append(e.calls, method)
	e.accounts = append(e.accounts, p[banexg.ParamAccount].(string))
}
func (e *legacyWriteExchange) CreateOrder(_, _, _ string, _, _ float64, p map[string]any) (*banexg.Order, *errs.Error) {
	e.record("create", p)
	return &banexg.Order{ID: "ack"}, nil
}
func (e *legacyWriteExchange) CancelOrder(_, _ string, p map[string]any) (*banexg.Order, *errs.Error) {
	e.record("cancel", p)
	return nil, nil
}
func (e *legacyWriteExchange) EditOrder(_, _, _ string, _, _ float64, p map[string]any) (*banexg.Order, *errs.Error) {
	e.record("edit", p)
	return nil, nil
}
func (e *legacyWriteExchange) SetLeverage(_ float64, _ string, p map[string]any) (map[string]any, *errs.Error) {
	e.record("leverage", p)
	return nil, nil
}
func (e *legacyWriteExchange) Call(_ string, p map[string]any) (*banexg.HttpRes, *errs.Error) {
	e.record("call", p)
	return nil, nil
}

func legacySenderForTest(t *testing.T, key AccountKey, dir string) (*LegacySender, *AccountRegistry) {
	t.Helper()
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewLegacySender(owner, []string{key.SettlementDomain}, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { registry.Close(); sender.Close() })
	return sender, registry
}

func TestLegacySenderPhysicalAndUpgradeLeases(t *testing.T) {
	key := testIntent(Buy).Account
	dir := t.TempDir()
	sender, registry := legacySenderForTest(t, key, dir)
	other := key
	other.SettlementDomain = "USD"
	if _, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "other.db"), other, dir); err == nil {
		t.Fatal("settlement partition bypassed physical sender lease")
	}
	registry.Close()
	sender.Close()
	body, _ := json.Marshal(key)
	hash := sha256.Sum256(body)
	release, err := acquireStoreLease(hex.EncodeToString(hash[:]), dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "old.db"), key, dir); err == nil {
		t.Fatal("new sender bypassed old executable full-key lease")
	}
	release()
	store, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "reopen.db"), other, dir)
	if err != nil {
		t.Fatal("failed acquisition leaked physical lease", err)
	}
	store.Close()
}

func TestLegacyExchangeGatesEveryWriteAndResolvesAccounts(t *testing.T) {
	key := testIntent(Buy).Account
	sender, registry := legacySenderForTest(t, key, t.TempDir())
	raw := &legacyWriteExchange{}
	e, err := NewLegacyExchange(context.Background(), raw, key.Account, map[string]*LegacySender{key.Account: sender})
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"marker": "unchanged"}
	writes := []func(map[string]any) *errs.Error{
		func(p map[string]any) *errs.Error {
			_, err := e.CreateOrder("BTC", "market", "buy", 1, 0, p)
			return err
		},
		func(p map[string]any) *errs.Error { _, err := e.CancelOrder("id", "BTC", p); return err },
		func(p map[string]any) *errs.Error { _, err := e.EditOrder("BTC", "id", "buy", 1, 1, p); return err },
		func(p map[string]any) *errs.Error { _, err := e.SetLeverage(2, "BTC", p); return err },
		func(p map[string]any) *errs.Error { _, err := e.Call("privateMutation", p); return err },
	}
	for _, write := range writes {
		if err := write(params); err != nil {
			t.Fatal(err)
		}
		if err := write(map[string]any{banexg.ParamAccount: key.Account}); err != nil {
			t.Fatal(err)
		}
	}
	if len(raw.calls) != 10 || len(params) != 1 {
		t.Fatal("write coverage or caller params changed", raw.calls, params)
	}
	for _, account := range raw.accounts {
		if account != key.Account {
			t.Fatal("default account not resolved", account)
		}
	}
	for _, account := range []any{"unknown", "", 12} {
		if err := writes[0](map[string]any{banexg.ParamAccount: account}); err == nil {
			t.Fatal("unowned account wrote", account)
		}
	}
	registry.Stop()
	for _, write := range writes {
		if err := write(nil); err == nil {
			t.Fatal("stopped owner wrote")
		}
	}
	if len(raw.calls) != 10 {
		t.Fatal("rejected mutation reached SDK")
	}
}

func TestLegacySenderJoinsSynchronousAcknowledgment(t *testing.T) {
	key := testIntent(Buy).Account
	sender, _ := legacySenderForTest(t, key, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- sender.Invoke(ctx, func() error { close(started); <-release; return nil }) }()
	<-started
	cancel()
	closed := make(chan struct{})
	go func() { sender.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("lease released while SDK call still running")
	case <-time.After(20 * time.Millisecond):
	}
	if err := sender.Invoke(context.Background(), func() error { return errors.New("unexpected") }); !errors.Is(err, ErrOwnerStopped) {
		t.Fatal("close did not seal admission", err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal("successful venue ACK lost to late cancellation", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("sender did not join")
	}
}
