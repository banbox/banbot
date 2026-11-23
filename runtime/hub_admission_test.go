package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// hubAdmissionRig is one real Hub robot ("ts", the fixture's manager rebuilt with
// an account config) plus a second robot "other" on the same account, driven
// through the same Hub wire. The second robot is only an account-level peer:
// the first robot's own journal never sees its orders.
type hubAdmissionRig struct {
	f        *hubTSFixture
	manager  *biz.HubOrderMgr
	refusals func() int32
	symbol   *orm.ExSymbol
	nextID   int64
	epoch    uint64
	sequence uint64
}

func newHubAdmissionRig(t *testing.T, cfg *config.Config) *hubAdmissionRig {
	t.Helper()
	f := newHubTSFixture(t, nil)
	ctx := context.Background()
	// Release capital for the second robot (unassigned cash), then declare it.
	if err := f.owner.rt.SharedExecution().CashEvent(execution.CashEvent{ID: "release", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Strategy: "ts", Amount: decimal.NewFromInt(-300)}, {Amount: decimal.NewFromInt(300)}}, AtMS: 101}); err != nil {
		t.Fatal(err)
	}
	if err := f.client.RegisterContributor(ctx, f.owner.key, execution.HubContributorRegistration{Strategy: "other", PolicyVersion: "shared-risk-v1", Capital: decimal.NewFromInt(300), GrossLimit: decimal.NewFromInt(300), Instruments: []string{"BTC"}}); err != nil {
		t.Fatal(err)
	}
	lease, err := f.client.ClaimStrategyWriter(ctx, f.owner.key, execution.StrategyWriterClaim{Strategy: "other", WriterID: "peer-robot", ExpectedEpoch: 0})
	if err != nil {
		t.Fatal(err)
	}
	refusals := countOwnerRefusals(f)
	if e := f.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	deps := f.rt.BizDeps()
	deps.Config = config.NewSnapshot(cfg)
	manager, err := biz.NewHubOrderMgr(deps, f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.CleanUp() })
	return &hubAdmissionRig{epoch: lease.Epoch, f: f, manager: manager, refusals: refusals.Load, symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}}
}

// peerEnter opens one resting limit entry of the second robot.
func (r *hubAdmissionRig) peerEnter(t *testing.T, lot, tf string, createdMS int64) error {
	t.Helper()
	r.nextID++
	r.sequence++
	_, err := r.f.client.ApplyPendingCommand(context.Background(), r.f.owner.key, execution.PendingCommand{ID: "peer-" + lot, Strategy: "other", StrategyEpoch: r.epoch, StrategySequence: r.sequence, Lot: execution.VirtualLotID(lot), Action: execution.PendingCreate,
		Create:   &execution.PendingCreateSpec{Instrument: "BTC", Side: execution.Buy, QuantitySteps: 10, Conditions: execution.IntentConditions{Limit: decimal.NewFromInt(50)}},
		Metadata: &execution.TimeSeriesMetadata{OrderID: 900 + r.nextID, StrategyName: "other", Symbol: "BTC", TimeFrame: tf, CreatedMS: createdMS}})
	return err
}

func (r *hubAdmissionRig) peerCancel(t *testing.T, lot string) {
	t.Helper()
	r.sequence++
	if _, err := r.f.client.ApplyPendingCommand(context.Background(), r.f.owner.key, execution.PendingCommand{ID: "peer-cancel-" + lot, Strategy: "other", StrategyEpoch: r.epoch, StrategySequence: r.sequence, Lot: execution.VirtualLotID(lot), Action: execution.PendingCancelEntry, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
}

func (r *hubAdmissionRig) enter(t *testing.T, tf, id string) bool {
	t.Helper()
	od, err := r.manager.EnterOrder(r.symbol, tf, &strat.EnterReq{CommandID: id, Tag: id, Amount: 1})
	if err != nil {
		t.Fatalf("entry %s: %v", id, err)
	}
	if e := r.manager.LastError(); e != nil {
		t.Fatalf("entry %s left a sticky error: %v", id, e)
	}
	return od != nil
}

func TestHubAdmissionAccountMaxOpenOrdersAcrossRobots(t *testing.T) {
	r := newHubAdmissionRig(t, &config.Config{MaxOpenOrders: 2})
	ctx := context.Background()
	status, err := r.f.client.AdmissionStatus(ctx, r.f.owner.key)
	if err != nil || status.Policy.MaxOpenOrders != 2 || status.Registrants["test-robot"].MaxOpenOrders != 2 {
		t.Fatalf("robot did not register its account limits: %+v %v", status, err)
	}
	// Wall-clock signal time: the Hub owner refuses stale entries.
	if err := r.peerEnter(t, "p1", "ws", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if !r.enter(t, "ws", "r1") {
		t.Fatal("first robot entry refused with one slot free")
	}
	// Together the two robots hold the account limit; the robot's own journal
	// counts only 1 < 2, so this is refused by the owner and dropped cleanly.
	before := r.refusals()
	fills := r.f.owner.adapter.Metrics().Fills
	if r.enter(t, "ws", "r2") {
		t.Fatal("account max_open_orders exceeded across robots")
	}
	if r.refusals() != before+1 || r.f.owner.adapter.Metrics().Fills != fills {
		t.Fatalf("expected exactly one owner refusal and no venue fill: refusals %d->%d", before, r.refusals())
	}
	// The robot keeps running: after the peer frees its slot, entries are admitted.
	r.peerCancel(t, "p1")
	if !r.enter(t, "ws", "r3") {
		t.Fatal("robot did not recover after the slot was freed")
	}
	if r.enter(t, "ws", "r4") {
		t.Fatal("robot exceeded its own account limit")
	}
	// A restart with the dropped command in the journal must not resend it.
	refusals := r.refusals()
	if e := r.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	deps := r.f.rt.BizDeps()
	deps.Config = config.NewSnapshot(&config.Config{MaxOpenOrders: 2})
	restarted, err := biz.NewHubOrderMgr(deps, r.f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	if r.refusals() != refusals {
		t.Fatalf("restart resent a dropped entry: %d -> %d", refusals, r.refusals())
	}
}

func TestHubAdmissionMaxSimulOpenAcrossRobots(t *testing.T) {
	r := newHubAdmissionRig(t, &config.Config{MaxSimulOpen: 2})
	// Stay clear of a bar boundary so both robots share one 1m round.
	for time.Now().UnixMilli()%60000 > 55000 {
		time.Sleep(200 * time.Millisecond)
	}
	now := r.f.rt.BizDeps().Clock.TimeMS()
	if err := r.peerEnter(t, "p1", "1m", now); err != nil {
		t.Fatal(err)
	}
	if !r.enter(t, "1m", "r1") {
		t.Fatal("round slot free but robot refused")
	}
	before := r.refusals()
	if r.enter(t, "1m", "r2") {
		t.Fatal("max_simul_open exceeded across robots in one round")
	}
	if r.refusals() != before+1 {
		t.Fatalf("expected the owner to refuse the third entry of the round: %d -> %d", before, r.refusals())
	}
}

// With one robot on the account its own check refuses first, so the owner
// refusal path never fires.
func TestHubAdmissionSingleRobotNeverReachesOwnerLimit(t *testing.T) {
	r := newHubAdmissionRig(t, &config.Config{MaxOpenOrders: 2})
	if !r.enter(t, "ws", "r1") || !r.enter(t, "ws", "r2") {
		t.Fatal("entries within the limit refused")
	}
	if r.enter(t, "ws", "r3") {
		t.Fatal("robot-side limit not enforced")
	}
	if got := r.refusals(); got != 0 {
		t.Fatalf("owner admission fired in a single-robot scenario: %d refusals", got)
	}
	simul := newHubAdmissionRig(t, &config.Config{MaxSimulOpen: 1})
	if !simul.enter(t, "1m", "s1") || simul.enter(t, "1m", "s2") {
		t.Fatal("per-bar simultaneous limit changed for a single robot")
	}
	if got := simul.refusals(); got != 0 {
		t.Fatalf("owner admission fired for the per-bar limit of a single robot: %d refusals", got)
	}
}
