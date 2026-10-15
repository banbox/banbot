package data

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/orm"
)

type planManagedSource struct {
	*stubSeriesSource
	prepare func([]*orm.Subscription, DataSink) error
	handle  *planHandle
}
type planHandle struct {
	stopped atomic.Bool
	joined  atomic.Bool
	errCh   chan error
}

func (h *planHandle) Errors() <-chan error { return h.errCh }

func (h *planHandle) Stop()       { h.stopped.Store(true) }
func (h *planHandle) Join() error { h.joined.Store(true); return nil }
func (s *planManagedSource) SubscribeManaged(_ context.Context, subs []*orm.Subscription, sink DataSink) (LiveSourceSubscription, error) {
	if s.handle == nil {
		s.handle = &planHandle{}
	}
	var err error
	if s.prepare != nil {
		err = s.prepare(subs, sink)
	}
	return s.handle, err
}

type planSink func(*orm.Subscription, []*orm.DataRecord) error

func (s planSink) Emit(sub *orm.Subscription, rows []*orm.DataRecord) error { return s(sub, rows) }

type planWarmupSink struct {
	planSink
	warm func(*orm.Subscription, []*orm.DataRecord) error
}

func (s planWarmupSink) Warmup(sub *orm.Subscription, rows []*orm.DataRecord) error {
	return s.warm(sub, rows)
}

type planEventSource struct {
	*planManagedSource
	start int64
}

type planReadyWaitSource struct {
	*stubSeriesSource
	handle *planReadyWaitHandle
}
type planReadyWaitHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (h *planReadyWaitHandle) Stop()       { h.cancel() }
func (h *planReadyWaitHandle) Join() error { <-h.done; return nil }
func (s *planReadyWaitSource) SubscribeManaged(ctx context.Context, _ []*orm.Subscription, sink DataSink) (LiveSourceSubscription, error) {
	_, cancel := context.WithCancel(ctx)
	s.handle = &planReadyWaitHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.handle.done)
		_ = sink.(interface{ AwaitLiveReady(context.Context) error }).AwaitLiveReady(context.Background())
	}()
	return s.handle, nil
}

func TestOptionalWarmupFailureUnblocksSourceReadinessBeforeJoin(t *testing.T) {
	c := NewDataSourceCatalog()
	src := &planReadyWaitSource{stubSeriesSource: newStubRegistrySource("waiting")}
	src.fetchErr = errors.New("history unavailable")
	if err := c.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	req := planRequest("waiting", 1)
	req.Required = false
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{req}, SubscriptionPlanOptions{AnchorMS: 86400000, EndMS: 86400000, RequireManagedLive: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type installed struct {
		installation *SubscriptionInstallation
		err          error
	}
	done := make(chan installed, 1)
	go func() {
		installation, err := c.InstallLivePlan(ctx, plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { return nil }))
		done <- installed{installation, err}
	}()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.installation.Degradations()) != 1 {
			t.Fatal("optional warmup failure not preserved")
		}
		result.installation.Stop()
		if err := result.installation.Join(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("optional producer readiness deadlocked startup join")
	}
}

func (s *planEventSource) WarmupStart(context.Context, *orm.Subscription, int64) (int64, error) {
	return s.start, nil
}

func TestLiveSubscriptionHistoryReadinessPrecedesQueuedAdmission(t *testing.T) {
	const day int64 = 86400000
	for _, event := range []bool{false, true} {
		t.Run(fmt.Sprint(event), func(t *testing.T) {
			c := NewDataSourceCatalog()
			src := &planManagedSource{stubSeriesSource: newStubRegistrySource("history")}
			req := planRequest("history", 2)
			anchor := 2*day + 3
			src.rows = []*orm.DataRecord{{Sid: 7, TimeMS: 0, EndMS: day, Closed: true, Values: map[string]any{"value": 1.0, "custom": int64(9007199254740993), "null": nil}}, {Sid: 7, TimeMS: day, EndMS: 2 * day, Closed: true, Values: map[string]any{"value": 2.0}}}
			var source DataSource = src
			if event {
				anchor = 104
				req.Subscription.TimeFrame = "event"
				src.info = orm.NewSeriesInfo("history", "event", []orm.SeriesField{{Name: "value", Type: "float"}})
				src.rows[0].TimeMS, src.rows[0].EndMS = 7, 7
				src.rows[1].TimeMS, src.rows[1].EndMS = 103, 103
				source = &planEventSource{src, 7}
			}
			prepared, warmed, live := false, false, false
			src.prepare = func(subs []*orm.Subscription, sink DataSink) error {
				prepared = true
				return sink.Emit(subs[0], []*orm.DataRecord{{Sid: 7, TimeMS: anchor, EndMS: anchor, Values: map[string]any{"value": 3.0}}})
			}
			if err := c.RegisterDataSource(source); err != nil {
				t.Fatal(err)
			}
			plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{req}, SubscriptionPlanOptions{AnchorMS: anchor, EndMS: anchor, RequireManagedLive: true})
			if err != nil {
				t.Fatal(err)
			}
			sink := planWarmupSink{planSink: func(*orm.Subscription, []*orm.DataRecord) error {
				if !warmed {
					t.Fatal("live admitted before historical readiness")
				}
				live = true
				return nil
			}, warm: func(_ *orm.Subscription, rows []*orm.DataRecord) error {
				if !prepared || live || len(rows) != 2 || rows[0].Values["custom"] != int64(9007199254740993) {
					t.Fatal("startup order or concrete value lost")
				}
				if value, ok := rows[0].Values["null"]; !ok || value != nil {
					t.Fatal("NULL lost")
				}
				warmed = true
				return nil
			}}
			install, err := NewLiveSourceProvider(c).InstallSubscriptionPlan(context.Background(), plan, sink)
			if err != nil {
				t.Fatal(err)
			}
			if !live {
				t.Fatal("prepared live data never admitted")
			}
			install.Stop()
			if err := install.Join(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLiveSubscriptionIncompleteHistoryFailsClosedAndJoins(t *testing.T) {
	const day int64 = 86400000
	for _, bad := range []string{"missing", "foreign", "unclosed", "future", "gap", "duplicate"} {
		t.Run(bad, func(t *testing.T) {
			c := NewDataSourceCatalog()
			src := &planManagedSource{stubSeriesSource: newStubRegistrySource("history")}
			src.rows = []*orm.DataRecord{{Sid: 7, TimeMS: 0, EndMS: day, Closed: true}, {Sid: 7, TimeMS: day, EndMS: 2 * day, Closed: true}}
			switch bad {
			case "missing":
				src.rows = src.rows[:1]
			case "foreign":
				src.rows[0].Sid = 8
			case "unclosed":
				src.rows[0].Closed = false
			case "future":
				src.rows[1].EndMS = 2*day + 10
			case "gap":
				src.rows[0].EndMS--
			case "duplicate":
				src.rows[1].TimeMS = 0
			}
			src.prepare = func(subs []*orm.Subscription, sink DataSink) error {
				return sink.Emit(subs[0], []*orm.DataRecord{{Sid: 7}})
			}
			if err := c.RegisterDataSource(src); err != nil {
				t.Fatal(err)
			}
			plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("history", 2)}, SubscriptionPlanOptions{AnchorMS: 2 * day, EndMS: 2 * day, RequireManagedLive: true})
			if err != nil {
				t.Fatal(err)
			}
			called := false
			sink := planWarmupSink{planSink: func(*orm.Subscription, []*orm.DataRecord) error { called = true; return nil }, warm: func(*orm.Subscription, []*orm.DataRecord) error { called = true; return nil }}
			_, err = NewLiveSourceProvider(c).InstallSubscriptionPlan(context.Background(), plan, sink)
			if err == nil || called || !src.handle.stopped.Load() || !src.handle.joined.Load() {
				t.Fatalf("incomplete history admitted: %v", err)
			}
		})
	}
}

func TestOptionalSourceFailuresPreserveRequiredConsumers(t *testing.T) {
	for _, phase := range []string{"startup", "async", "warmup", "unmanaged", "unregistered", "lookback", "version", "prepare-version"} {
		t.Run(phase, func(t *testing.T) {
			c := NewDataSourceCatalog()
			required := &planManagedSource{stubSeriesSource: newStubRegistrySource("required")}
			optional := &planManagedSource{stubSeriesSource: newStubRegistrySource("optional"), handle: &planHandle{errCh: make(chan error, 1)}}
			failure := errors.New("optional source offline")
			req := planRequest("optional", 0)
			req.Consumer = "optional-factor"
			req.Required = false
			optional.prepare = func(subs []*orm.Subscription, sink DataSink) error {
				if phase == "prepare-version" {
					optional.version = "changed during SubscribeManaged"
					return sink.Emit(subs[0], []*orm.DataRecord{{Sid: 7}})
				}
				if phase == "startup" {
					if err := sink.Emit(subs[0], []*orm.DataRecord{{Sid: 7}}); err != nil {
						return err
					}
					return failure
				}
				return nil
			}
			if phase == "warmup" {
				req.Subscription.WarmupNum = 1
				optional.fetchErr = failure
			}
			if phase == "lookback" {
				req.Subscription.TimeFrame = "event"
				req.Subscription.WarmupNum = 1
				optional.info = orm.NewSeriesInfo("optional", "event", []orm.SeriesField{{Name: "value", Type: "float"}})
			}
			if err := c.RegisterDataSource(required); err != nil {
				t.Fatal(err)
			}
			if phase != "unregistered" {
				var source DataSource = optional
				if phase == "unmanaged" {
					source = optional.stubSeriesSource
				}
				if err := c.RegisterDataSource(source); err != nil {
					t.Fatal(err)
				}
			}
			plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("required", 0), req}, SubscriptionPlanOptions{AnchorMS: 86400000, EndMS: 86400000, RequireManagedLive: true})
			if err != nil {
				t.Fatal(err)
			}
			if phase == "version" {
				optional.version = "changed"
			}
			var healthy atomic.Int32
			install, err := c.InstallLivePlan(context.Background(), plan, planSink(func(sub *orm.Subscription, _ []*orm.DataRecord) error {
				if sub.Source != "required" {
					t.Fatal("degraded startup row published")
				}
				healthy.Add(1)
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			if phase == "async" {
				optional.handle.errCh <- failure
				deadline := time.Now().Add(time.Second)
				for len(install.Degradations()) == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			}
			degraded := install.Degradations()
			if len(degraded) != 1 || degraded[0].Source != "optional" || len(degraded[0].Consumers) != 1 || degraded[0].Consumers[0].Name != "optional-factor" || degraded[0].Err == nil {
				t.Fatalf("consumer degradation missing: %+v", degraded)
			}
			healthySub := planRequest("required", 0).Subscription
			if err := install.Emit(&healthySub, nil); err != nil {
				t.Fatal(err)
			}
			if healthy.Load() != 1 || required.handle.stopped.Load() {
				t.Fatal("optional failure stopped healthy consumers")
			}
			select {
			case err := <-install.Errors():
				t.Fatalf("optional failure reported fatal: %v", err)
			default:
			}
			install.Stop()
			if err := install.Join(); err != nil {
				t.Fatal(err)
			}
			if phase == "startup" || phase == "async" || phase == "warmup" || phase == "prepare-version" {
				if !optional.handle.stopped.Load() || !optional.handle.joined.Load() {
					t.Fatal("optional source lifetime leaked")
				}
			}
		})
	}
}

func TestSubscriptionPreparedActivationRejectsCanceledContext(t *testing.T) {
	c := NewDataSourceCatalog()
	source := &planManagedSource{stubSeriesSource: newStubRegistrySource("signal")}
	source.prepare = func(subs []*orm.Subscription, sink DataSink) error {
		return sink.Emit(subs[0], []*orm.DataRecord{{Sid: 7}})
	}
	if err := c.RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("signal", 0)}, SubscriptionPlanOptions{RequireManagedLive: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var published atomic.Int32
	installation, err := c.PrepareLivePlan(ctx, plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { published.Add(1); return nil }))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := installation.Activate(); !errors.Is(err, context.Canceled) || published.Load() != 0 {
		t.Fatalf("canceled generation activated: %v published=%d", err, published.Load())
	}
	installation.Stop()
	if err := installation.Join(); err != nil || !source.handle.stopped.Load() || !source.handle.joined.Load() {
		t.Fatalf("canceled generation producer leaked: %v", err)
	}
}

func TestSharedSourceRequiredConsumerPreventsOptionalDegradation(t *testing.T) {
	c := NewDataSourceCatalog()
	failure := errors.New("shared source unavailable")
	src := &planManagedSource{stubSeriesSource: newStubRegistrySource("shared"), prepare: func([]*orm.Subscription, DataSink) error { return failure }}
	if err := c.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	optional := planRequest("shared", 0)
	optional.Required = false
	optional.Consumer = "optional"
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("shared", 0), optional}, SubscriptionPlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.InstallLivePlan(context.Background(), plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error {
		t.Fatal("required shared failure admitted")
		return nil
	})); !errors.Is(err, failure) {
		t.Fatalf("required consumer degraded: %v", err)
	}
}
func planRequest(name string, warm int) SubscriptionRequest {
	return SubscriptionRequest{Subscription: Subscription{Source: name, TimeFrame: "1d", ExSymbol: &orm.ExSymbol{ID: 7, Symbol: "asset"}, WarmupNum: warm, Fields: []string{"value"}}, Consumer: "factor", Required: true, MaxAgeMS: 100}
}

func TestSubscriptionPlanUnionIdentityAndImmutableViews(t *testing.T) {
	c := NewDataSourceCatalog()
	source := newStubRegistrySource("union")
	source.info.Binding.Fields = append(source.info.Binding.Fields, orm.SeriesField{Name: "custom", Type: "int"})
	if err := c.RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	first := planRequest("union", 2)
	second := planRequest("union", 5)
	second.Consumer = "legacy"
	second.Subscription.Fields = []string{"custom"}
	second.MaxAgeMS = 50
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{first, second}, SubscriptionPlanOptions{AnchorMS: 10 * 86400000, EndMS: 12 * 86400000, PageRows: 10, PrefetchRows: 3})
	if err != nil {
		t.Fatal(err)
	}
	streams := plan.Streams()
	if len(streams) != 1 || streams[0].Subscription.WarmupNum != 5 || len(streams[0].Consumers) != 2 || plan.Options().PageRows != 3 || streams[0].WarmupStartMS != 5*86400000 {
		t.Fatalf("wrong union: %+v", streams)
	}
	streams[0].Subscription.Fields[0] = "changed"
	streams[0].Subscription.ExSymbol.Symbol = "changed"
	streams[0].Consumers[0].Name = "changed"
	if reflect.DeepEqual(streams, plan.Streams()) {
		t.Fatal("mutable plan views")
	}
	source.version = "v2"
	if _, err := c.InstallLivePlan(context.Background(), plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { return nil })); err == nil {
		t.Fatal("changed source accepted")
	}
	second.Subscription.ExSymbol.Symbol = "another"
	if _, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{first, second}, plan.Options()); err == nil {
		t.Fatal("SID conflict accepted")
	}
}

func TestSubscriptionStartupFailurePublishesNothingAndJoins(t *testing.T) {
	c := NewDataSourceCatalog()
	var published atomic.Int32
	first := &planManagedSource{stubSeriesSource: newStubRegistrySource("a")}
	first.prepare = func(subs []*orm.Subscription, sink DataSink) error {
		return sink.Emit(subs[0], []*orm.DataRecord{{Sid: 7, TimeMS: 1, Values: map[string]any{"value": 1.0}}})
	}
	failure := errors.New("not ready")
	second := &planManagedSource{stubSeriesSource: newStubRegistrySource("b"), prepare: func([]*orm.Subscription, DataSink) error { return failure }}
	for _, src := range []DataSource{first, second} {
		if err := c.RegisterDataSource(src); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("a", 0), planRequest("b", 0)}, SubscriptionPlanOptions{RequireManagedLive: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.InstallLivePlan(context.Background(), plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { published.Add(1); return nil }))
	if !errors.Is(err, failure) || published.Load() != 0 || !first.handle.stopped.Load() || !first.handle.joined.Load() || !second.handle.joined.Load() {
		t.Fatalf("incomplete startup rollback: %v", err)
	}
}

func TestSubscriptionStartupClonesTypesAndBoundedRows(t *testing.T) {
	for _, budget := range []int{1, 2} {
		t.Run(string(rune('0'+budget)), func(t *testing.T) {
			c := NewDataSourceCatalog()
			src := &planManagedSource{stubSeriesSource: newStubRegistrySource("typed")}
			typed := map[string][]int64{"a": {9007199254740993}}
			values := map[string]any{"custom": typed, "null": nil, "nilmap": map[string]int(nil)}
			src.prepare = func(subs []*orm.Subscription, sink DataSink) error {
				err := sink.Emit(subs[0], []*orm.DataRecord{{Sid: 7, Values: values}, {Sid: 7, Values: nil}})
				typed["a"][0] = 3
				return err
			}
			if err := c.RegisterDataSource(src); err != nil {
				t.Fatal(err)
			}
			plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("typed", 0)}, SubscriptionPlanOptions{PrefetchRows: budget})
			if err != nil {
				t.Fatal(err)
			}
			var got []*orm.DataRecord
			install, err := c.InstallLivePlan(context.Background(), plan, planSink(func(_ *orm.Subscription, rows []*orm.DataRecord) error { got = rows; return nil }))
			if budget == 1 {
				if err == nil || len(got) != 0 {
					t.Fatal("budget failed open")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			install.Stop()
			if err = install.Join(); err != nil {
				t.Fatal(err)
			}
			if got[0].Values["custom"].(map[string][]int64)["a"][0] != 9007199254740993 || got[1].Values != nil {
				t.Fatal("concrete type/nil ownership lost")
			}
			if _, ok := got[0].Values["null"]; !ok {
				t.Fatal("NULL lost")
			}
		})
	}
}

func TestSubscriptionStopJoinWaitsAdmittedCallbacks(t *testing.T) {
	c := NewDataSourceCatalog()
	src := &planManagedSource{stubSeriesSource: newStubRegistrySource("waiting")}
	if err := c.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("waiting", 0)}, SubscriptionPlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	install, err := c.InstallLivePlan(context.Background(), plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { close(entered); <-release; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = install.Emit(&plan.Subscriptions()[0], nil) }()
	<-entered
	install.Stop()
	joined := make(chan error, 1)
	go func() { joined <- install.Join() }()
	select {
	case <-joined:
		t.Fatal("Join returned during callback")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-joined:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Join stuck")
	}
	if err := install.Emit(nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("stopped intake admitted")
	}
}

func TestSubscriptionStrictReadinessAndKlineRollback(t *testing.T) {
	c := NewDataSourceCatalog()
	legacy := newStubRegistrySource("legacy")
	if err := c.RegisterDataSource(legacy); err != nil {
		t.Fatal(err)
	}
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("legacy", 0)}, SubscriptionPlanOptions{RequireManagedLive: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.InstallLivePlan(context.Background(), plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { return nil })); err == nil || legacy.subscribeCount != 0 {
		t.Fatal("strict unmanaged source started")
	}
	managed := &planManagedSource{stubSeriesSource: newStubRegistrySource("managed")}
	if err = c.RegisterDataSource(managed); err != nil {
		t.Fatal(err)
	}
	kline := planRequest("kline", 0)
	kline.Subscription.TimeFrame = "1m"
	kline.Subscription.Fields = []string{"close"}
	plan, err = c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("managed", 0), kline}, SubscriptionPlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewLiveSourceProvider(c).InstallSubscriptionPlan(context.Background(), plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { return nil })); err == nil || !managed.handle.joined.Load() {
		t.Fatal("kline failure did not join managed sources")
	}
}

func TestManagedAsynchronousFailureFreezesReadyPlanAndJoins(t *testing.T) {
	c := NewDataSourceCatalog()
	failure := errors.New("producer failed")
	src := &planManagedSource{stubSeriesSource: newStubRegistrySource("async"), handle: &planHandle{errCh: make(chan error, 1)}}
	if err := c.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	plan, err := c.CompileSubscriptionPlan(context.Background(), []SubscriptionRequest{planRequest("async", 0)}, SubscriptionPlanOptions{RequireManagedLive: true})
	if err != nil {
		t.Fatal(err)
	}
	installation, err := c.InstallLivePlan(context.Background(), plan, planSink(func(*orm.Subscription, []*orm.DataRecord) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if err = installation.AwaitLiveReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.handle.errCh <- failure
	select {
	case err := <-installation.Errors():
		if !errors.Is(err, failure) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("async error not monitored")
	}
	if err = installation.Join(); !errors.Is(err, failure) || !src.handle.stopped.Load() || !src.handle.joined.Load() {
		t.Fatal("failed producer not joined")
	}
	if err = installation.Emit(nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("async failure did not freeze callbacks")
	}
	if err = installation.AwaitLiveReady(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatal("failed readiness remained open")
	}
}
