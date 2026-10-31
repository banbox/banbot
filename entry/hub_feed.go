package entry

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg/log"
	exutils "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

// hubFeedHost resolves public symbols only from accepted robot registrations.
// Neither its public subscription nor callback has access to credentials.
type hubFeedHost struct {
	mu            sync.Mutex
	registrations map[execution.AccountKey]HubAccountConfig
	clocks        map[execution.PendingFeedID]*hubFeedClock
	pending       map[execution.PendingFeedID][]execution.PendingFeedObservation
	verified      map[execution.HubPendingFeed]bool
	cancel        context.CancelFunc
	done          chan struct{}
	wake          chan struct{}
	pin           func(string, string, string, int32) error
}

type hubFeedClock struct {
	sid             int32
	frame           string
	duration        int64
	lastEnd, bucket int64
	complete        bool
}

func newHubFeedHost() *hubFeedHost {
	return &hubFeedHost{registrations: map[execution.AccountKey]HubAccountConfig{}, clocks: map[execution.PendingFeedID]*hubFeedClock{}, pending: map[execution.PendingFeedID][]execution.PendingFeedObservation{}, verified: map[execution.HubPendingFeed]bool{}}
}

func (f *hubFeedHost) factory(base execution.HubAccountFactory, roots ...string) execution.HubAccountFactory {
	return func(ctx context.Context, request execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		cfg, err := decodeHubAccountConfig(request)
		if err != nil {
			return execution.HubAccountBinding{}, err
		}
		binding, err := base(ctx, request)
		if err == nil && len(roots) != 0 {
			cfg, err = readHubHostDeclaration(roots[0], request)
			if err != nil {
				if binding.Shutdown != nil {
					err = errors.Join(err, binding.Shutdown())
				}
				return execution.HubAccountBinding{}, err
			}
		}
		if err == nil {
			// Retain public identity only, never another copy of the credentials.
			cfg.Credential = nil
			f.mu.Lock()
			f.registrations[request.Account] = cfg
			f.mu.Unlock()
		}
		return binding, err
	}
}

func parseHubFeed(feed execution.PendingFeedID) (*hubFeedClock, error) {
	parts := strings.Split(string(feed), "/")
	if len(parts) != 2 {
		return nil, errors.New("hub: pending feed requires stable SID/timeframe identity")
	}
	sid, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil || sid <= 0 {
		return nil, errors.New("hub: pending feed SID invalid")
	}
	seconds, err := exutils.TFToSecSafe(parts[1])
	if err != nil || seconds < 60 || seconds%60 != 0 {
		return nil, errors.New("hub: autonomous public feed requires a whole-minute timeframe")
	}
	return &hubFeedClock{sid: int32(sid), frame: parts[1], duration: int64(seconds) * 1000}, nil
}

// observe counts only complete, contiguous base-bar buckets. A duplicate,
// warm-up or missing intermediate bar cannot invent an observed strategy bar.
func (c *hubFeedClock) observe(fact data.ClosedSeriesFact) bool {
	if fact.SID != c.sid || fact.TimeFrame != "1m" || fact.EndMS-fact.TimeMS != 60000 || fact.EndMS <= c.lastEnd {
		return false
	}
	bucket := exutils.AlignTfSecs(fact.TimeMS/1000, int(c.duration/1000)) * 1000
	if bucket != c.bucket || c.lastEnd == 0 {
		c.bucket = bucket
		c.complete = fact.TimeMS == bucket
	} else if fact.TimeMS != c.lastEnd {
		c.complete = false
	}
	c.lastEnd = fact.EndMS
	return c.complete && fact.EndMS == bucket+c.duration
}

func (f *hubFeedHost) sync(ctx context.Context, spider *data.LiveSpider, hub *execution.Hub) error {
	feeds, err := hub.PendingFeeds(ctx)
	var failures []error
	if err != nil {
		failures = append(failures, err)
	}
	for _, feed := range feeds {
		if err := f.admit(ctx, hub, feed); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// admit completes public identity/capability admission before a remote create
// can commit its StopBars deadline. It is also used to restore existing feeds.
func (f *hubFeedHost) admit(ctx context.Context, hub *execution.Hub, feed execution.HubPendingFeed) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clock, err := parseHubFeed(feed.Feed)
	if err != nil {
		return err
	}
	f.mu.Lock()
	pin := f.pin
	exists := f.verified[feed]
	cfg, registered := f.registrations[feed.Account]
	f.mu.Unlock()
	if pin == nil {
		return errors.New("hub: public execution feed is not ready")
	}
	if !registered {
		return errors.New("hub: accepted feed registration unavailable")
	}
	if exists {
		return nil
	}
	symbol := ""
	for _, unit := range cfg.Instruments {
		if unit.Instrument.ID == feed.Instrument {
			symbol = unit.Symbol
			break
		}
	}
	if symbol == "" {
		return errors.New("hub: pending feed instrument not registered")
	}
	if err := pin(cfg.Exchange, "linear", symbol, clock.sid); err != nil {
		return err
	}
	if err := hub.AuthorizePublicFeed(feed); err != nil {
		return err
	}
	f.mu.Lock()
	f.verified[feed] = true
	if f.clocks[feed.Feed] == nil {
		f.clocks[feed.Feed] = clock
	}
	f.mu.Unlock()
	return nil
}

func (f *hubFeedHost) start(parent context.Context, spider *data.LiveSpider, hub *execution.Hub) error {
	ctx, cancel := context.WithCancel(parent)
	f.cancel = cancel
	f.done = make(chan struct{})
	f.wake = make(chan struct{}, 1)
	spider.SetClosedSeriesObserver(f.observeClosed)
	f.mu.Lock()
	f.pin = spider.PinClosedFeed
	f.mu.Unlock()

	go func() {
		defer close(f.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if err := f.sync(ctx, spider, hub); err != nil && ctx.Err() == nil {
				log.Error("hub public execution feed unavailable", zap.Error(err))
			}
			err := f.drain(ctx, hub.ObservePublicFeed)
			if err != nil && ctx.Err() == nil {
				log.Error("hub public execution feed retry failed", zap.Error(err))
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-f.wake:
			}
		}
	}()
	return nil
}

func (f *hubFeedHost) close() {
	f.mu.Lock()
	f.pin = nil
	f.mu.Unlock()
	if f.cancel != nil {
		f.cancel()
		<-f.done
	}
}

// observeClosed never performs account, exchange or database work. Saving
// public data must not wait for another account's cancellation/recovery timeout.
func (f *hubFeedHost) observeClosed(_ context.Context, fact data.ClosedSeriesFact) error {
	f.mu.Lock()
	for feed, clock := range f.clocks {
		if clock.observe(fact) {
			f.pending[feed] = append(f.pending[feed], execution.PendingFeedObservation{Feed: feed, EndMS: fact.EndMS, Closed: true})
		}
	}
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
	return nil
}

// Retry accepted facts in feed order without holding the registration/observer
// lock across account work. Only this delivery worker removes queue heads.
func (f *hubFeedHost) drain(ctx context.Context, deliver func(context.Context, execution.PendingFeedObservation) error) error {
	f.mu.Lock()
	feeds := make([]execution.PendingFeedID, 0, len(f.pending))
	for feed := range f.pending {
		feeds = append(feeds, feed)
	}
	f.mu.Unlock()
	var failures []error
	for _, feed := range feeds {
		for {
			f.mu.Lock()
			queue := f.pending[feed]
			if len(queue) == 0 {
				f.mu.Unlock()
				break
			}
			head := queue[0]
			f.mu.Unlock()
			if err := deliver(ctx, head); err != nil {
				failures = append(failures, fmt.Errorf("hub: public pending feed advancement failed: %w", err))
				break
			}
			f.mu.Lock()
			queue = f.pending[feed]
			if len(queue) > 0 && queue[0] == head {
				if len(queue) == 1 {
					delete(f.pending, feed)
				} else {
					f.pending[feed] = queue[1:]
				}
			}
			f.mu.Unlock()
		}
	}
	return errors.Join(failures...)
}
