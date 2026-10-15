package biz

import (
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"go.uber.org/zap"
)

const (
	// submitReconcileIntv is the delay before one unconfirmed submission is
	// reconciled again, and submitReconcileMaxTries bounds those attempts so a
	// broken connection cannot keep re-queueing the same order forever.
	submitReconcileIntv     = 2 * time.Second
	submitReconcileMaxTries = 4
)

// submitReconciler returns the adapter capability that can confirm an
// indeterminate submission, or nil unless the same adapter also builds the
// stable client order ids that a reconciliation retry has to re-use. Without
// both, retrying a lost response could open a second order.
func (o *LiveOrderMgr) submitReconciler() exg.SubmitReconcileCapability {
	exchange := o.exchangeClient()
	if exchange == nil || exg.GetClientOrderPurposeCapability(exchange) == nil {
		return nil
	}
	return exg.GetSubmitReconcileCapability(exchange)
}

// buildSubmitClientOrderID returns the identifier used for one submission. When
// the adapter provides per-purpose identifiers, entry and exit of the same local
// order can never collide and rebuilding the id is idempotent; otherwise the
// adapter's randomized identifier is used.
func (o *LiveOrderMgr) buildSubmitClientOrderID(od *ormo.InOutOrder, isEnter bool) (string, *errs.Error) {
	exchange := o.exchangeClient()
	if exchange == nil || exg.GetClientOrderPurposeCapability(exchange) == nil {
		return o.buildClientOrderID(od, true)
	}
	namespace, err := o.clientOrderNamespace()
	if err != nil {
		return "", err
	}
	purpose := banexg.ClientOrderPurposeEnter
	if !isEnter {
		purpose = banexg.ClientOrderPurposeExit
	}
	return exg.BuildPurposeClientOrderIDChecked(exchange, namespace, od.ID,
		od.GetInfoString(ormo.OdInfoClientID), purpose)
}

// submitUnconfirmedAction reports the queue action whose exchange result is
// still unknown. It stays empty once the submission is recorded on the order,
// so a stale marker can never re-queue an already submitted order.
func submitUnconfirmedAction(od *ormo.InOutOrder) string {
	if od == nil {
		return ""
	}
	action := od.GetInfoString(ormo.OdInfoSubmitUnconfirmed)
	switch action {
	case ormo.OdActionEnter:
		if od.Enter == nil || od.Enter.OrderID != "" {
			return ""
		}
	case ormo.OdActionExit:
		if od.Exit == nil || od.Exit.OrderID != "" || od.Status >= ormo.InOutStatusFullExit {
			return ""
		}
	default:
		return ""
	}
	return action
}

func clearSubmitUnconfirmed(od *ormo.InOutOrder) {
	if od == nil {
		return
	}
	od.SetInfo(ormo.OdInfoSubmitUnconfirmed, nil)
	od.SetInfo(ormo.OdInfoSubmitReconcileTries, nil)
}

// lookupSubmittedOrder resolves one submission by its client order id. It
// separates a confirmed absence, which makes a resubmit safe, from a failed
// lookup, which does not.
func (o *LiveOrderMgr) lookupSubmittedOrder(od *ormo.InOutOrder, subOd *ormo.ExOrder, clientID string,
	reconciler banexg.SubmitReconcileCapability) (*banexg.Order, bool, *errs.Error) {
	res, err := reconciler.FindSubmittedOrder(od.Symbol, clientID, map[string]interface{}{
		banexg.ParamAccount: o.Account,
	})
	if err != nil {
		o.Logger().Warn("lookup unconfirmed submission fail", zap.String("acc", o.Account),
			zap.String("key", od.Key()), zap.String("clientId", clientID), zap.Error(err))
		return nil, false, err
	}
	if res == nil {
		return nil, true, nil
	}
	if !matchesSubmittedOrder(od, subOd, clientID, res) {
		o.Logger().Warn("ignore mismatched client order lookup", zap.String("acc", o.Account),
			zap.String("key", od.Key()), zap.String("clientId", clientID), zap.String("orderId", res.ID))
		return nil, false, errs.NewMsg(errs.CodeRunTime, "client order lookup does not match %s", od.Key())
	}
	return res, false, nil
}

// lookupRestoredEnter resolves an entry that has no exchange order id locally,
// which is what a stalled submit followed by a restart leaves behind.
func (o *LiveOrderMgr) lookupRestoredEnter(od *ormo.InOutOrder) (*banexg.Order, *errs.Error) {
	reconciler := o.submitReconciler()
	if reconciler == nil || od.Enter == nil {
		return nil, nil
	}
	clientID, err := o.buildSubmitClientOrderID(od, true)
	if err != nil {
		return nil, err
	}
	found, _, err := o.lookupSubmittedOrder(od, od.Enter, clientID, reconciler)
	return found, err
}

func matchesSubmittedOrder(od *ormo.InOutOrder, subOd *ormo.ExOrder, clientID string, res *banexg.Order) bool {
	if od == nil || subOd == nil || res == nil || res.ID == "" || res.ClientOrderID != clientID ||
		res.Symbol != od.Symbol || (res.Side != "" && res.Side != subOd.Side) {
		return false
	}
	wantPos := banexg.PosSideLong
	if od.Short {
		wantPos = banexg.PosSideShort
	}
	return res.PositionSide == "" || res.PositionSide == wantPos
}

// scheduleSubmitReconcile re-queues an unconfirmed submission with the same
// client order id. Once the attempt budget is exhausted the order takes the
// same path a conclusive submit failure would have taken.
func (o *LiveOrderMgr) scheduleSubmitReconcile(od *ormo.InOutOrder, action string, lastErr *errs.Error) {
	tries := int64(1)
	if o.submitReconciler() != nil {
		tries = od.GetInfoInt64(ormo.OdInfoSubmitReconcileTries) + 1
		od.SetInfo(ormo.OdInfoSubmitReconcileTries, tries)
	}
	if tries < submitReconcileMaxTries {
		o.Logger().Warn("reconcile unconfirmed submission", zap.String("acc", o.Account),
			zap.String("key", od.Key()), zap.String("action", action), zap.Int64("tries", tries))
		time.AfterFunc(submitReconcileIntv*time.Duration(tries), func() {
			if submitUnconfirmedAction(od) != action {
				return
			}
			select {
			case <-core.Ctx.Done():
			case o.queue <- &OdQItem{Order: od, Action: action}:
			}
		})
		return
	}
	o.Logger().Error("give up reconciling unconfirmed submission", zap.String("acc", o.Account),
		zap.String("key", od.Key()), zap.String("action", action), zap.Int64("tries", tries))
	clearSubmitUnconfirmed(od)
	if action == ormo.OdActionEnter && od.Status < ormo.InOutStatusFullExit {
		err := o.localExit(od, 0, core.ExitTagFatalErr, od.InitPrice, lastErr.Short(), "")
		if err != nil {
			o.Logger().Error("local exit order fail", zap.String("acc", o.Account),
				zap.String("key", od.Key()), zap.Error(err))
		}
		o.fireOdChange(od, strat.OdChgExitFill)
	}
}
