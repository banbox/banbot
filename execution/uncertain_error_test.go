package execution

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestCancelIncludesFailedUncertaintyPersistence(t *testing.T) {
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "uncertain", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	primary := errors.New("cancel transport unavailable")
	adapter := &fakeExecutionAdapter{}
	e := executorFor(s, h, adapter)
	if err := e.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	adapter.cancel = func(context.Context, string) (bool, error) {
		err := s.transaction(context.Background(), func(tx *sql.Tx) error {
			_, err := tx.Exec(`CREATE TRIGGER fail_uncertainty BEFORE UPDATE OF state ON exec_order WHEN NEW.state='CancelPending' BEGIN SELECT RAISE(ABORT,'uncertainty disk failure'); END`)
			return err
		})
		if err != nil {
			return false, err
		}
		return false, primary
	}
	err := e.Cancel(order.ID, 12)
	if !errors.Is(err, primary) || !strings.Contains(err.Error(), "uncertainty disk failure") {
		t.Fatalf("lost transport or persistence evidence: %v", err)
	}
	if err := e.Send(order.ID, 13); err == nil {
		t.Fatal("uncertain cancellation resent order")
	}
	if len(adapter.calls()) != 2 {
		t.Fatal("unexpected transport retry", adapter.calls())
	}
}
