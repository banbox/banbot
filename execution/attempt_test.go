package execution

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestOrderAttemptAuditCommitsBeforeTransport(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		t.Run(backend, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			order := planOrder(t, s, "audit", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			transportErr := errors.New("submission acknowledgement lost")
			adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true}}
			adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
				attempts, err := s.OrderAttempts(context.Background(), order.ID)
				if err != nil || len(attempts) != 1 {
					t.Fatal("network preceded audit commit", attempts, err)
				}
				first := attempts[0]
				if first.Phase != AttemptStarted || first.Kind != SubmitAttempt || first.Number != 1 || first.Generation != h.Token().Generation || first.AtMS != 11 {
					t.Fatal(first)
				}
				return SubmitReceipt{}, transportErr
			}
			e := executorFor(s, h, adapter)
			if err := e.Send(order.ID, 11); !errors.Is(err, transportErr) {
				t.Fatal(err)
			}
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "confirmed"}}, nil
			}
			if err := e.Recover(order.ID); err != nil {
				t.Fatal(err)
			}
			attempts, err := s.OrderAttempts(context.Background(), order.ID)
			if err != nil || len(attempts) != 3 || attempts[0].Result != "Sending" || attempts[1].Result != transportErr.Error() || attempts[2].Result != "Acknowledged" {
				t.Fatal("attempt evidence overwritten", attempts, err)
			}
			adapter.cancel = func(context.Context, string) (bool, error) {
				return false, errors.New("cancellation acknowledgement lost")
			}
			if err := e.Cancel(order.ID, 20); err == nil {
				t.Fatal("cancel failure hidden")
			}
			attempts, err = s.OrderAttempts(context.Background(), order.ID)
			if err != nil || len(attempts) != 5 || attempts[3].Kind != CancelAttempt || attempts[3].Number != 2 || attempts[3].Phase != AttemptStarted || attempts[3].AtMS != 20 || attempts[4].Phase != AttemptResult {
				t.Fatal(attempts, err)
			}
			if err := e.Send(order.ID, 21); err == nil {
				t.Fatal("audit recovery repeated uncertain order")
			}
		})
	}
}

func TestAttemptUpgradeShadowVerificationAndRollback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			s, _, h, path := testStore(t)
			order := planOrder(t, s, "legacy-audit", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			if err := executorFor(s, h, &fakeExecutionAdapter{}).Send(order.ID, 11); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`CREATE TABLE exec_attempt(account TEXT,order_id TEXT,number INTEGER,kind TEXT,generation TEXT,at_ms INTEGER,result TEXT,PRIMARY KEY(account,order_id,number)); DELETE FROM exec_schema; INSERT INTO exec_schema VALUES(2);`); err != nil {
				t.Fatal(err)
			}
			for n, kind := range []AttemptKind{SubmitAttempt, CancelAttempt} {
				if _, err := s.db.Exec("INSERT INTO exec_attempt VALUES(?,?,?,?,?,?,?)", s.accountID, order.ID, n+1, string(kind), "42", 11+n, "retained-result"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.db.Exec("UPDATE exec_order SET attempt=2 WHERE account=? AND id=?", s.accountID, order.ID); err != nil {
				t.Fatal(err)
			}
			before, err := s.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if fail {
				if _, err := s.db.Exec(`CREATE TRIGGER fail_attempt_upgrade BEFORE INSERT ON exec_event WHEN NEW.kind='OrderAttempt' BEGIN SELECT RAISE(ABORT,'audit migration fault'); END`); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenStore(path, s.key)
			if fail {
				if err == nil {
					reopened.Close()
					t.Fatal("migration fault ignored")
				}
				db, dbErr := sql.Open("sqlite", path)
				if dbErr != nil {
					t.Fatal(dbErr)
				}
				var count, version int
				var checkpoint int64
				if err := db.QueryRow("SELECT count(*) FROM exec_attempt").Scan(&count); err != nil || count != 2 {
					t.Fatal("legacy audit lost", count, err)
				}
				if err := db.QueryRow("SELECT max(version) FROM exec_schema").Scan(&version); err != nil || version != 2 {
					t.Fatal(version, err)
				}
				if err := db.QueryRow("SELECT checkpoint FROM exec_account WHERE account=?", s.accountID).Scan(&checkpoint); err != nil || checkpoint != before.Checkpoint {
					t.Fatal("partial imported checkpoint escaped", checkpoint, err)
				}
				if _, err := db.Exec("DROP TRIGGER fail_attempt_upgrade"); err != nil {
					t.Fatal(err)
				}
				db.Close()
				reopened, err = OpenStore(path, s.key)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			attempts, err := reopened.OrderAttempts(context.Background(), order.ID)
			if err != nil {
				t.Fatal(err)
			}
			var copied int
			for _, attempt := range attempts {
				if attempt.Phase == AttemptImported {
					copied++
					if attempt.Generation != 42 || attempt.Result != "retained-result" || attempt.AtMS != 10+attempt.Number {
						t.Fatal("migration changed attempt fact", attempt)
					}
				}
			}
			if copied != 2 {
				t.Fatal("incomplete imported audit", attempts)
			}
			after, err := reopened.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if after.Checkpoint != before.Checkpoint+2 {
				t.Fatal("typed import events absent", before.Checkpoint, after.Checkpoint)
			}
			after.Checkpoint = before.Checkpoint
			a, _ := payload(before)
			b, _ := payload(after)
			if a != b {
				t.Fatal("audit upgrade changed account economics", a, b)
			}
			var count int
			if err := reopened.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='exec_attempt'").Scan(&count); err != nil || count != 0 {
				t.Fatal("legacy attempt table retained", count, err)
			}
		})
	}
}
