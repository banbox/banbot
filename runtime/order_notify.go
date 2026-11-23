package runtime

import (
	"fmt"
	"sync"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Notifications on execution-backed runtimes never touch the trading path:
// callers only enqueue into a bounded buffer (non-blocking, drop on overflow)
// and a single worker goroutine owns every call into the rpc session, including
// the (possibly slow or panicking) channel start-up.

const notifyQueueSize = 256

type notifyItem struct {
	msg   map[string]interface{}
	flush chan struct{}
}

// Notifier is the per-runtime asynchronous front of the notification session.
type Notifier struct {
	send    func(map[string]interface{})
	prepare func() error
	logger  func() *zap.Logger
	queue   chan notifyItem
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newNotifier(prepare func() error, send func(map[string]interface{}), logger func() *zap.Logger) *Notifier {
	n := &Notifier{send: send, prepare: prepare, logger: logger, queue: make(chan notifyItem, notifyQueueSize), stop: make(chan struct{}), done: make(chan struct{})}
	go n.run()
	return n
}

func (n *Notifier) warn(msg string, fields ...zap.Field) {
	defer func() { _ = recover() }()
	if n.logger != nil {
		if l := n.logger(); l != nil {
			l.Warn(msg, fields...)
		}
	}
}

// safely runs fn and converts a panic into an error: a channel that panics in
// its constructor (e.g. a bad telegram chat_id) must not end the robot.
func safely(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn()
}

func (n *Notifier) run() {
	defer close(n.done)
	ready := true
	if n.prepare != nil {
		if err := safely(n.prepare); err != nil {
			ready = false
			n.warn("notifications disabled: start rpc channels failed", zap.Error(err))
		}
	}
	for {
		select {
		case <-n.stop:
			return
		case it := <-n.queue:
			if it.msg != nil && ready {
				if err := safely(func() error { n.send(it.msg); return nil }); err != nil {
					n.warn("send notification failed", zap.Error(err))
				}
			}
			if it.flush != nil {
				close(it.flush)
			}
		}
	}
}

// Post enqueues msg without blocking. It reports false when the message was
// dropped (queue full or notifier stopped).
func (n *Notifier) Post(msg map[string]interface{}) bool {
	if n == nil || msg == nil {
		return false
	}
	select {
	case <-n.stop:
		return false
	default:
	}
	select {
	case n.queue <- notifyItem{msg: msg}:
		return true
	default:
		return false
	}
}

// Flush waits at most wait for everything queued before the call to be handed
// to the session. It reports whether the queue drained in time.
func (n *Notifier) Flush(wait time.Duration) bool {
	if n == nil {
		return true
	}
	marker := make(chan struct{})
	select {
	case n.queue <- notifyItem{flush: marker}:
	default:
		return false
	case <-n.stop:
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-marker:
		return true
	case <-n.done:
		return true
	case <-timer.C:
		return false
	}
}

// Stop ends the worker without waiting; queued messages are discarded.
func (n *Notifier) Stop() {
	if n != nil {
		n.once.Do(func() { close(n.stop) })
	}
}

var (
	notifiersMu sync.Mutex
	notifiers   = map[*Runtime]*Notifier{}
	// notifierSink overrides the session send function; tests only.
	notifierSink = map[*Runtime]func(map[string]interface{}){}
)

// Notifier returns the runtime's asynchronous notification front, creating it
// (and starting the rpc session in the background) on first use.
func (r *Runtime) Notifier() *Notifier {
	if r == nil || r.Notifications == nil {
		return nil
	}
	notifiersMu.Lock()
	if n := notifiers[r]; n != nil {
		notifiersMu.Unlock()
		return n
	}
	session := r.Notifications
	send := session.SendMsg
	// Start returns *errs.Error: returning it directly as error would turn a
	// nil pointer into a non-nil error and disable notifications on success.
	prepare := func() error {
		if startErr := session.Start(); startErr != nil {
			return startErr
		}
		return nil
	}
	if sink := notifierSink[r]; sink != nil {
		send, prepare = sink, nil
	}
	n := newNotifier(prepare, send, func() *zap.Logger { return r.Core.Log() })
	notifiers[r] = n
	notifiersMu.Unlock()
	// Registered outside the lock: OnClose runs the callback at once when the
	// runtime is already stopped, and the callback takes the lock itself.
	r.OnClose(func() {
		n.Stop()
		notifiersMu.Lock()
		delete(notifiers, r)
		delete(notifierSink, r)
		notifiersMu.Unlock()
	})
	return n
}

// OrderNotifyCallback returns the order manager callback that sends the legacy
// entry/exit message for fills. It only builds the payload and enqueues it, so
// it is safe under the managers' locks and never fails the order path.
func (r *Runtime) OrderNotifyCallback() biz.FnOdCb {
	if r == nil {
		return nil
	}
	account := r.defaultAccount
	return func(od *ormo.InOutOrder, isEnter bool) {
		defer func() { _ = recover() }()
		// Score update for strat_perf: inert unless enabled for the job.
		r.observeClosedOrder(od, isEnter)
		msg, ok := biz.NewOrderMessage(od, isEnter, account)
		if !ok {
			return
		}
		if n := r.Notifier(); n != nil && !n.Post(msg) {
			n.warn("notification dropped", zap.String("account", account), zap.String("key", od.Key()))
		}
	}
}

// ExcNotifyCore returns the exception-alert log core bound to this runtime's
// notification queue, or nil when the runtime has no session.
func (r *Runtime) ExcNotifyCore() zapcore.Core {
	n := r.Notifier()
	if n == nil {
		return nil
	}
	return rpc.NewExcNotifyWithSender(func(msg map[string]interface{}) { n.Post(msg) })
}

func (r *Runtime) openOrderCount() int {
	if r == nil || r.Orders == nil {
		return 0
	}
	odMap, lock := r.Orders.GetOpenODs(r.defaultAccount)
	lock.Lock()
	defer lock.Unlock()
	return len(odMap)
}

// NotifyStarted sends the startup status message (same wording as the legacy
// order-sync message of live.CryptoTrader).
func (r *Runtime) NotifyStarted() {
	n := r.Notifier()
	if n == nil {
		return
	}
	cnt := r.openOrderCount()
	n.Post(map[string]interface{}{
		"type": rpc.MsgTypeStatus, "account": r.defaultAccount,
		"status": fmt.Sprintf("orders: %d restored, %d deleted, %d added, %d opened", cnt, 0, 0, cnt),
	})
}

// NotifyShutdown sends the "bot stop" status message and waits at most wait for
// it to reach the rpc session. Call it before Close/Join.
func (r *Runtime) NotifyShutdown(wait time.Duration) {
	n := r.Notifier()
	if n == nil {
		return
	}
	n.Post(map[string]interface{}{
		"type": rpc.MsgTypeStatus, "account": r.defaultAccount,
		"status": fmt.Sprintf("bot stop, %d orders opened", r.openOrderCount()),
	})
	n.Flush(wait)
}
