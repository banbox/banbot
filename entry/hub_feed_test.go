package entry

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"testing"
	"time"
)

func TestHubFeedClockRequiresContiguousCompleteBuckets(t *testing.T) {
	for _, frame := range []string{"1m", "5m", "1h"} {
		t.Run(frame, func(t *testing.T) {
			clock, err := parseHubFeed(execution.PendingFeedID("17/" + frame))
			if err != nil {
				t.Fatal(err)
			}
			base := int64(3600000)
			for at := base; at < base+clock.duration; at += 60000 {
				fact := data.ClosedSeriesFact{SID: 17, TimeFrame: "1m", TimeMS: at, EndMS: at + 60000}
				closed := clock.observe(fact)
				if closed != (fact.EndMS == base+clock.duration) {
					t.Fatal("incorrect bucket completion", fact, closed)
				}
				if clock.observe(fact) {
					t.Fatal("duplicate base row counted again")
				}
			}
		})
	}
	clock, _ := parseHubFeed("17/5m")
	base := int64(3600000)
	for offset := int64(0); offset < 300000; offset += 60000 {
		if offset == 120000 {
			continue
		}
		if clock.observe(data.ClosedSeriesFact{SID: 17, TimeFrame: "1m", TimeMS: base + offset, EndMS: base + offset + 60000}) {
			t.Fatal("gap invented a complete five-minute bar")
		}
	}
	for offset := int64(300000); offset < 600000; offset += 60000 {
		completed := clock.observe(data.ClosedSeriesFact{SID: 17, TimeFrame: "1m", TimeMS: base + offset, EndMS: base + offset + 60000})
		if completed != (offset == 540000) {
			t.Fatal("contiguous next bucket did not recover", offset)
		}
	}
}

func TestHubFeedClockDoesNotInventPartialOrForeignBars(t *testing.T) {
	clock, _ := parseHubFeed("17/5m")
	base := int64(3600000)
	for offset := int64(60000); offset < 300000; offset += 60000 {
		if clock.observe(data.ClosedSeriesFact{SID: 17, TimeFrame: "1m", TimeMS: base + offset, EndMS: base + offset + 60000}) {
			t.Fatal("initial partial bucket counted")
		}
	}
	if clock.observe(data.ClosedSeriesFact{SID: 18, TimeFrame: "1m", TimeMS: base + 300000, EndMS: base + 360000}) {
		t.Fatal("foreign SID counted")
	}
	if clock.observe(data.ClosedSeriesFact{SID: 17, TimeFrame: "5m", TimeMS: base + 300000, EndMS: base + 600000}) {
		t.Fatal("non-base stream counted")
	}
	for _, feed := range []execution.PendingFeedID{"bad", "0/1m", "1/30s", "1/invalid"} {
		if _, err := parseHubFeed(feed); err == nil {
			t.Fatal("unsupported feed accepted", feed)
		}
	}
}

func TestHubFeedBlockedAccountDoesNotBlockPublicSaveCallback(t *testing.T) {
	host := newHubFeedHost()
	host.clocks["17/1m"], _ = parseHubFeed("17/1m")
	host.wake = make(chan struct{}, 1)
	if err := host.observeClosed(context.Background(), data.ClosedSeriesFact{SID: 17, TimeFrame: "1m", TimeMS: 60000, EndMS: 120000}); err != nil {
		t.Fatal(err)
	}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var calls int
	go func() {
		done <- host.drain(context.Background(), func(context.Context, execution.PendingFeedObservation) error {
			calls++
			if calls == 1 {
				close(started)
				<-release
			}
			return nil
		})
	}()
	<-started
	callback := make(chan struct{})
	go func() {
		host.observeClosed(context.Background(), data.ClosedSeriesFact{SID: 17, TimeFrame: "1m", TimeMS: 120000, EndMS: 180000})
		host.mu.Lock()
		host.registrations[execution.AccountKey{Account: "new"}] = HubAccountConfig{}
		host.mu.Unlock()
		close(callback)
	}()
	select {
	case <-callback:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("account delivery blocked public persistence callback or registration")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("callback fact lost while account delivery was blocked", calls)
	}
}

func TestHubFeedDeliveryRetriesHeadBeforeLaterFacts(t *testing.T) {
	host := newHubFeedHost()
	host.pending["17/1m"] = []execution.PendingFeedObservation{{Feed: "17/1m", EndMS: 120000, Closed: true}, {Feed: "17/1m", EndMS: 180000, Closed: true}}
	failed := errors.New("account temporarily unavailable")
	if err := host.drain(context.Background(), func(context.Context, execution.PendingFeedObservation) error { return failed }); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	var ends []int64
	if err := host.drain(context.Background(), func(_ context.Context, fact execution.PendingFeedObservation) error {
		ends = append(ends, fact.EndMS)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ends) != 2 || ends[0] != 120000 || ends[1] != 180000 || len(host.pending) != 0 {
		t.Fatal("failed head was skipped or reordered", ends, host.pending)
	}
}

func TestHubFeedAdmissionRejectsUnsupportedOrUnreadyFeed(t *testing.T) {
	host := newHubFeedHost()
	request, cfg := hubConfigFixture()
	host.registrations[request.Account] = cfg
	for _, feed := range []execution.PendingFeedID{"abc", "17/30s", "17/invalid", "0/1m"} {
		if err := host.admit(context.Background(), nil, execution.HubPendingFeed{Account: request.Account, Instrument: "BTC", Feed: feed}); err == nil {
			t.Fatal("unobservable feed passed admission", feed)
		}
	}
	if err := host.admit(context.Background(), nil, execution.HubPendingFeed{Account: request.Account, Instrument: "BTC", Feed: "17/1m"}); err == nil {
		t.Fatal("public source was not ready but deadline was accepted")
	}
}

func TestHubFeedAdmissionValidatesEveryBindingEvenForExistingClock(t *testing.T) {
	ctx := context.Background()
	request, cfg := hubConfigFixture()
	registry := &execution.AccountRegistry{}
	owner, err := registry.Acquire(request.Account)
	if err != nil {
		t.Fatal(err)
	}
	paper, err := execution.NewPaperAdapter(cfg.PaperCash, cfg.PaperFee, cfg.PaperSlippage)
	if err != nil {
		t.Fatal(err)
	}
	service, err := execution.NewSharedAccount(owner, execution.SharedExecutionOptions{Memory: true, Adapter: paper, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := execution.NewHub(func(context.Context, execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		return execution.HubAccountBinding{Account: service, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close(); service.Close(); registry.Close() })
	request.Configuration = json.RawMessage(`{}`)
	if err := hub.Register(ctx, request); err != nil {
		t.Fatal(err)
	}
	host := newHubFeedHost()
	host.registrations[request.Account] = cfg
	host.pin = func(exchange, market, symbol string, sid int32) error {
		if exchange != "example" || market != "linear" || symbol != "BTC/USDT:USDT" || sid != 17 {
			return errors.New("public SID authority mismatch")
		}
		return nil
	}
	binding := execution.HubPendingFeed{Account: request.Account, Instrument: "BTC", Feed: "17/1m"}
	if err := host.admit(ctx, hub, binding); err != nil {
		t.Fatal(err)
	}
	cfg.Instruments = append(cfg.Instruments, execution.BanexgInstrument{Symbol: "ETH/USDT:USDT", Instrument: cfg.Instruments[0].Instrument.Clone()})
	cfg.Instruments[1].Instrument.ID = "ETH"
	host.registrations[request.Account] = cfg
	binding.Instrument = "ETH"
	if err := host.admit(ctx, hub, binding); err == nil {
		t.Fatal("existing feed ID bypassed instrument/SID validation")
	}
	if host.verified[binding] {
		t.Fatal("invalid account/instrument/feed binding became trusted")
	}
}
