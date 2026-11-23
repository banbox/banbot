package ormo

import (
	"context"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

// OdInfoArchiveKey marks a historical row written by ArchiveOrder with the
// stable identity of the position it was projected from.
const OdInfoArchiveKey = "archive_key"

// ArchiveOrder stores a finished order view as a new historical row of the
// trades database and returns its row ID. It is the read-model sink for order
// managers whose authoritative state lives in an execution ledger: od is only a
// projection, so it is copied and never modified, and the row gets a database
// ID of its own.
//
// key is the stable identity of the projected position. A row with the same
// key and close time is returned instead of inserting a second one, so a caller
// may repeat the call after a crash between this write and its own bookkeeping.
// Orders that never filled are not history and return 0.
func (s *OrderState) ArchiveOrder(od *InOutOrder, key string) (int64, *errs.Error) {
	if od == nil || od.IOrder == nil || od.Enter == nil || key == "" {
		return 0, errs.NewMsg(errs.CodeParamRequired, "archive requires a finished order and a stable key")
	}
	if od.Status != InOutStatusFullExit || od.Enter.Filled == 0 {
		return 0, nil
	}
	sess, conn, err := s.Conn(true)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	existing, err := sess.GetOrders(GetOrdersArgs{
		TaskID: od.TaskID, Status: 2, Strategy: od.Strategy, Pairs: []string{od.Symbol},
		CloseAfter: od.ExitAt - 1, CloseBefore: od.ExitAt + 1,
	})
	if err != nil {
		return 0, err
	}
	for _, row := range existing {
		if row.GetInfoString(OdInfoArchiveKey) == key {
			return row.ID, nil
		}
	}
	row := od.Clone()
	row.BindState(nil)
	row.ID = 0
	// Both legs belong to the order's task: history queries filter legs by it.
	row.Enter.ID, row.Enter.InoutID, row.Enter.TaskID = 0, 0, row.TaskID
	if row.Exit != nil {
		row.Exit.ID, row.Exit.InoutID, row.Exit.TaskID = 0, 0, row.TaskID
	}
	row.SetInfo(OdInfoArchiveKey, key)
	row.NanInfTo(0)
	if _, err = row.GetInfoText(); err != nil {
		return 0, err
	}
	tx, err_ := conn.BeginTx(context.Background(), nil)
	if err_ != nil {
		return 0, errs.New(core.ErrDbExecFail, err_)
	}
	defer tx.Rollback()
	sess = sess.WithTx(tx)
	if err = row.IOrder.saveAdd(sess); err != nil {
		return 0, err
	}
	row.Enter.InoutID = row.ID
	if err = row.Enter.saveAdd(sess); err != nil {
		return 0, err
	}
	if row.Exit != nil && row.Exit.Symbol != "" {
		row.Exit.InoutID = row.ID
		if err = row.Exit.saveAdd(sess); err != nil {
			return 0, err
		}
	}
	if err_ = tx.Commit(); err_ != nil {
		return 0, errs.New(core.ErrDbExecFail, err_)
	}
	return row.ID, nil
}
