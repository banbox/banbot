package execution

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// EmbeddedHub exposes an ALREADY BUILT, host-owned SharedAccount through an
// in-process Hub, so an embedded production robot can run the same Hub-facing
// order manager as a default Hub robot. It never owns the
// account: Close stops the Hub and its pending-execution tick but leaves the
// account to its builder, which closes it exactly as before.
//
// Two duties of a Hub host are replaced here because no market stream or feed
// host exists in this composition:
//   - pending-feed admission always succeeds: the robot's own runtime observes
//     the public feeds and forwards closed bars (biz.HubOrderMgr with
//     LocalAccount);
//   - a ticker advances pending execution (resting entries and protections) on
//     the account's registered quotes, like the market-stream worker does.
type EmbeddedHub struct {
	hub    *Hub
	port   *LocalHubPort
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
	once   sync.Once
}

// NewEmbeddedHub registers account as a production Hub account. tick must be
// positive; the tick does nothing until the account is reconciled.
func NewEmbeddedHub(parent context.Context, account *SharedAccount, tick time.Duration) (*EmbeddedHub, error) {
	if parent == nil || account == nil || tick <= 0 {
		return nil, errors.New("execution: embedded hub needs a context, an account and a positive tick")
	}
	hub, err := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		// Ownership stays with the caller: shutting the hub down closes nothing.
		return HubAccountBinding{Account: account, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		return nil, err
	}
	hub.SetPendingFeedAdmission(func(context.Context, HubPendingFeed) error { return nil })
	if err := hub.Register(parent, HubAccountRegistration{Account: account.AccountKey(), Env: "prod", Configuration: json.RawMessage(`{"embedded":true}`)}); err != nil {
		return nil, errors.Join(err, hub.Close())
	}
	ctx, cancel := context.WithCancel(parent)
	e := &EmbeddedHub{hub: hub, port: NewLocalHubPort(hub), cancel: cancel, done: make(chan struct{})}
	go e.run(ctx, account, tick)
	return e, nil
}

// Port is the robot-side command surface of the embedded account.
func (e *EmbeddedHub) Port() *LocalHubPort { return e.port }

// TickError is the last pending-execution tick failure, nil after a success.
func (e *EmbeddedHub) TickError() error { e.mu.Lock(); defer e.mu.Unlock(); return e.err }

func (e *EmbeddedHub) run(ctx context.Context, account *SharedAccount, tick time.Duration) {
	defer close(e.done)
	borrow := account.Borrow()
	defer borrow.Release()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := borrow.WithState(func(service *SharedAccount) error {
			if !service.Reconciled() {
				return nil
			}
			return service.AdvancePendingExecution(ctx, time.Now().UnixMilli())
		})
		if ctx.Err() != nil {
			return
		}
		e.mu.Lock()
		previous := e.err
		e.err = err
		e.mu.Unlock()
		// The account's own report worker runs the same tick and freezes the
		// account on failure; this extra tick only reports, once per change.
		if err != nil && (previous == nil || previous.Error() != err.Error()) {
			log.Warn("embedded hub pending tick failed", zap.Error(err))
		} else if err == nil && previous != nil {
			log.Info("embedded hub pending tick recovered")
		}
	}
}

// Close stops the tick and the Hub. It is idempotent and never closes the account.
func (e *EmbeddedHub) Close() error {
	var result error
	e.once.Do(func() {
		e.cancel()
		<-e.done
		e.port.Close()
		result = e.hub.Close()
	})
	return result
}
