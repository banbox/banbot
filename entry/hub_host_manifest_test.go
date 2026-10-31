package entry

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

func hostManifestRequest(request execution.HubAccountRegistration, c HubAccountConfig) execution.HubAccountRegistration {
	request.Configuration, _ = json.Marshal(c)
	return request
}

func TestHubHostManifestCredentialFreeImmutableAndPrivate(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	request, c := hubConfigFixture()
	request.Env = core.RunEnvProd
	request.Account.VenueSessionIdentity = "example/linear/prod"
	c.PaperCash = decimal.Zero
	c.Credential = &config.AccountConfig{Exchanges: map[string]*config.ExgApiSecrets{"example": {Prod: &config.ApiSecretConfig{APIKey: "private-key", APISecret: "private-secret"}}}}
	request = hostManifestRequest(request, c)
	initial, err := loadOrCreateHubHostConfig(ctx, root, request, c)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Credential == nil || initial.Credential.GetApiSecretFor("example", core.RunEnvProd).APISecret != "private-secret" {
		t.Fatal("ephemeral credentials not restored")
	}
	path := filepath.Join(hubAccountDirectory(root, request.Account), hubHostManifestName)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"Credential", "private-key", "private-secret", "APISecret"} {
		if strings.Contains(string(body), secret) {
			t.Fatal("credential persisted", secret)
		}
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("manifest is not private", info.Mode())
	}
	for _, mode := range []string{"baseline", "universe", "bootstrap-capital"} {
		t.Run(mode, func(t *testing.T) {
			changed := c
			switch mode {
			case "baseline":
				changed.Risk.MaxVirtualGross = decimal.NewFromInt(999)
			case "universe":
				changed.Instruments = slicesCloneHubUnits(c.Instruments)
				changed.Instruments[0].Symbol = "ETH/USDT:USDT"
			case "bootstrap-capital":
				changed.Capital = map[execution.StrategyID]decimal.Decimal{"s": decimal.NewFromInt(101)}
			}
			if _, err := loadOrCreateHubHostConfig(ctx, root, hostManifestRequest(request, changed), changed); err == nil {
				t.Fatal("manifest baseline changed")
			}
			after, _ := os.ReadFile(path)
			if string(body) != string(after) {
				t.Fatal("failed registration replaced initial declaration")
			}
		})
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateHubHostConfig(ctx, root, request, c); err == nil {
		t.Fatal("publicly readable declaration accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(body, []byte(` {}`)...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateHubHostConfig(ctx, root, request, c); err == nil {
		t.Fatal("corrupt declaration accepted")
	}
}

func TestHubHostManifestRejectsLegacyLedgerBeforeVenueCreation(t *testing.T) {
	request, c := hubConfigFixture()
	request = hostManifestRequest(request, c)
	root := t.TempDir()
	directory := hubAccountDirectory(root, request.Account)
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "ledger.db"), []byte("existing account"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := newHubAccountFactory(root, "", true)(context.Background(), request); err == nil || !strings.Contains(err.Error(), "no initial host declaration") {
		t.Fatal("existing owner inferred or venue constructed before declaration validation", err)
	}
	if _, err := os.Stat(filepath.Join(directory, hubHostManifestName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed recovery minted initial ownership", err)
	}
}

func TestHubHostManifestDryRunBaselineAndSymlinkFailClosed(t *testing.T) {
	ctx := context.Background()
	request, c := hubConfigFixture()
	root := t.TempDir()
	request = hostManifestRequest(request, c)
	if _, err := loadOrCreateHubHostConfig(ctx, root, request, c); err != nil {
		t.Fatal(err)
	}
	changed := c
	changed.PaperCash = decimal.NewFromInt(2000)
	if _, err := loadOrCreateHubHostConfig(ctx, root, hostManifestRequest(request, changed), changed); err == nil {
		t.Fatal("paper baseline changed")
	}
	path := filepath.Join(hubAccountDirectory(root, request.Account), hubHostManifestName)
	renamed := path + ".saved"
	if err := os.Rename(path, renamed); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(renamed, path); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateHubHostConfig(ctx, root, request, c); err == nil {
		t.Fatal("symlink declaration accepted")
	}
}

// Use the actual declaration/account/callback helpers while replacing only the
// network market stream with deterministic quotes.
func mockDeclaredHubFactory(t *testing.T, root string, prices map[string]decimal.Decimal) execution.HubAccountFactory {
	t.Helper()
	return func(ctx context.Context, request execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		incoming, err := decodeHubAccountConfig(request)
		if err != nil {
			return execution.HubAccountBinding{}, err
		}
		initial, err := loadOrCreateHubHostConfig(ctx, root, request, incoming)
		if err != nil {
			return execution.HubAccountBinding{}, err
		}
		var registry execution.AccountRegistry
		owner, err := registry.Acquire(request.Account)
		if err != nil {
			return execution.HubAccountBinding{}, err
		}
		paper, err := execution.NewDurablePaperAdapter(initial.PaperCash, initial.PaperFee, initial.PaperSlippage, execution.PaperBidAsk)
		if err != nil {
			return execution.HubAccountBinding{}, err
		}
		service, err := execution.NewSharedAccount(owner, execution.SharedExecutionOptions{StorePath: filepath.Join(hubAccountDirectory(root, request.Account), "ledger.db"), SenderLeaseDir: filepath.Join(root, "leases"), Adapter: paper, AuthoritativeSnapshot: true})
		if err != nil {
			registry.Close()
			return execution.HubAccountBinding{}, err
		}
		shutdown := func() error { registry.Close(); return service.Close() }
		borrow := service.Borrow()
		defer borrow.Release()
		failed := func(err error) (execution.HubAccountBinding, error) {
			shutdown()
			return execution.HubAccountBinding{}, err
		}
		if err := borrow.RegisterRiskPolicy("hub-v1", initial.Risk); err != nil {
			return failed(err)
		}
		if err := borrow.BootstrapCapital(ctx, initial.Capital, 1); err != nil {
			return failed(err)
		}
		if err := borrow.RecoverPersisted(ctx); err != nil {
			return failed(err)
		}
		if err := borrow.Reconcile("startup", 2); err != nil {
			return failed(err)
		}
		var instruments []execution.Instrument
		for _, unit := range initial.Instruments {
			instruments = append(instruments, unit.Instrument)
		}
		if err := borrow.RegisterAccountQuotes(instruments, func(_ context.Context, id string, now int64) (execution.VisibleQuote, error) {
			price := prices[id]
			return execution.VisibleQuote{Bid: price, Ask: price, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: now}, nil
		}, nil); err != nil {
			return failed(err)
		}
		strategies := map[execution.StrategyID]bool{}
		for id := range initial.Capital {
			strategies[id] = true
		}
		if err := borrow.ValidateAccountBindings(strategies); err != nil {
			return failed(err)
		}
		additional := hubAdditionalRegistration(service, initial)
		if err := additional(ctx, request); err != nil {
			return failed(err)
		}
		return execution.HubAccountBinding{Account: service, Shutdown: shutdown, RegisterAdditional: additional}, nil
	}
}

func TestHubHostManifestRestartSecondRobotRetainsOfflineInitialProtection(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	request, initial := hubConfigFixture()
	eth := initial.Instruments[0]
	eth.Symbol = "ETH/USDT:USDT"
	eth.Instrument.ID = "ETH"
	initial.Instruments = append(initial.Instruments, eth)
	prices := map[string]decimal.Decimal{"BTC": decimal.NewFromInt(100), "ETH": decimal.NewFromInt(100)}
	factory := mockDeclaredHubFactory(t, root, prices)
	first, err := factory(ctx, hostManifestRequest(request, initial))
	if err != nil {
		t.Fatal(err)
	}
	borrow := first.Account.Borrow()
	command := execution.PendingCommand{ID: "initial-entry", Strategy: "s", Lot: "offline-initial", Action: execution.PendingCreate, Create: &execution.PendingCreateSpec{Instrument: "ETH", Side: execution.Buy, QuantitySteps: 10, Protections: []execution.PositionProtection{{Kind: execution.StopLossProtection, Level: decimal.NewFromInt(90)}}}}
	if _, err := borrow.ApplyPendingCommand(ctx, command, 10); err != nil {
		t.Fatal(err)
	}
	before, err := borrow.Snapshot(ctx)
	if err != nil || len(before.Lots) != 1 {
		t.Fatal("initial strategy not filled", before, err)
	}
	second := initial
	second.Instruments = initial.Instruments[:1]
	second.Capital = map[execution.StrategyID]decimal.Decimal{"second": decimal.NewFromInt(200)}
	second.Risk.StrategyGrossLimits = map[execution.StrategyID]decimal.Decimal{"second": decimal.NewFromInt(500)}
	secondRequest := hostManifestRequest(request, second)
	if err := first.RegisterAdditional(ctx, secondRequest); err != nil {
		t.Fatal(err)
	}
	accepted, err := borrow.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	borrow.Release()
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	feeds := newHubFeedHost()
	restarted, err := feeds.factory(factory, root)(ctx, secondRequest)
	if err != nil {
		t.Fatal("B-first restart failed to restore offline A", err)
	}
	defer restarted.Shutdown()
	recovered := restarted.Account.Borrow()
	defer recovered.Release()
	snapshot, err := recovered.Snapshot(ctx)
	if err != nil || !snapshot.UnassignedCash.Equal(accepted.UnassignedCash) || !snapshot.SyntheticStrategyCash["s"].Equal(accepted.SyntheticStrategyCash["s"]) || !snapshot.SyntheticStrategyCash["second"].Equal(accepted.SyntheticStrategyCash["second"]) {
		t.Fatal("B-first restart reallocated initial or contributor capital", snapshot, err)
	}
	if len(feeds.registrations[request.Account].Instruments) != 2 {
		t.Fatal("B-first restart lost offline A public-feed universe")
	}
	book, err := recovered.PendingOrders(ctx)
	if err != nil || len(book) != 1 || book[0].Order.Strategy != "s" || len(book[0].Specs) != 1 {
		t.Fatal("offline initial protection not recovered", book, err)
	}
	prices["ETH"] = decimal.NewFromInt(89)
	if err := recovered.AdvancePendingExecution(ctx, 20); err != nil {
		t.Fatal("offline initial stop did not advance", err)
	}
	final, err := recovered.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lot := range final.Lots {
		if lot.Strategy == "s" && lot.SignedSteps != 0 {
			t.Fatal("offline A stop lost after B-first restart", final)
		}
	}
	if err := restarted.RegisterAdditional(ctx, hostManifestRequest(request, initial)); err != nil {
		t.Fatal("initial A could not reconnect after B-first restart", err)
	}
}

func TestHubHostManifestConcurrentInitialDeclarationsNeverReplaceWinner(t *testing.T) {
	root := t.TempDir()
	request, first := hubConfigFixture()
	second := first
	second.PaperCash = decimal.NewFromInt(2000)
	type result struct {
		config HubAccountConfig
		err    error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, candidate := range []HubAccountConfig{first, second} {
		go func(c HubAccountConfig) {
			<-start
			restored, err := loadOrCreateHubHostConfig(context.Background(), root, hostManifestRequest(request, c), c)
			results <- result{restored, err}
		}(candidate)
	}
	close(start)
	var winner HubAccountConfig
	successes := 0
	for range 2 {
		r := <-results
		if r.err == nil {
			successes++
			winner = r.config
		}
	}
	if successes != 1 {
		t.Fatal("competing initial baselines did not select exactly one immutable owner", successes)
	}
	restored, err := readHubHostDeclaration(root, request)
	if err != nil || !restored.PaperCash.Equal(winner.PaperCash) {
		t.Fatal("initial declaration winner was overwritten", restored, err)
	}
}

func TestHubHostManifestNewContributorFirstRequestAfterRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	request, initial := hubConfigFixture()
	factory := mockDeclaredHubFactory(t, root, map[string]decimal.Decimal{"BTC": decimal.NewFromInt(100)})
	first, err := factory(ctx, hostManifestRequest(request, initial))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	newcomer := initial
	newcomer.Capital = map[execution.StrategyID]decimal.Decimal{"new-robot": decimal.NewFromInt(200)}
	newcomer.Risk.StrategyGrossLimits = map[execution.StrategyID]decimal.Decimal{"new-robot": decimal.NewFromInt(500)}
	incoming := hostManifestRequest(request, newcomer)
	restored, err := factory(ctx, incoming)
	if err != nil {
		t.Fatal("new contributor first reconnect must not become bootstrap owner", err)
	}
	defer restored.Shutdown()
	borrow := restored.Account.Borrow()
	defer borrow.Release()
	snapshot, err := borrow.Snapshot(ctx)
	if err != nil || !snapshot.UnassignedCash.Equal(decimal.NewFromInt(700)) || !snapshot.SyntheticStrategyCash["s"].Equal(decimal.NewFromInt(100)) || !snapshot.SyntheticStrategyCash["new-robot"].Equal(decimal.NewFromInt(200)) {
		t.Fatal("first reconnect lost bootstrap allocation or skipped contributor admission", snapshot, err)
	}
	if err := restored.RegisterAdditional(ctx, incoming); err != nil {
		t.Fatal(err)
	}
	retry, err := borrow.Snapshot(ctx)
	if err != nil || !retry.UnassignedCash.Equal(snapshot.UnassignedCash) || !retry.SyntheticStrategyCash["new-robot"].Equal(snapshot.SyntheticStrategyCash["new-robot"]) {
		t.Fatal("new contributor first-request retry reallocated cash", retry, err)
	}
}
