package orm

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type transferReadDB struct {
	DBTX
	queries []string
}

func (d *transferReadDB) Query(_ context.Context, sql string, _ ...interface{}) (pgx.Rows, error) {
	d.queries = append(d.queries, sql)
	if strings.Contains(sql, "pg_attribute") {
		return newInterfaceRows([][]any{{"open", "double precision", false}, {"high", "double precision", false}, {"low", "double precision", false}, {"close", "double precision", false}, {"volume", "double precision", false}, {"quote", "double precision", true}, {"buy_volume", "double precision", true}, {"trade_num", "bigint", true}, {"custom", "text", true}}), nil
	}
	return newInterfaceRows(nil), nil
}

func TestTransferExportNeedsNoExchangeAdapter(t *testing.T) {
	db := &transferReadDB{}
	q := NewWithStorage(db, &Storage{})
	p := utils.NewPrgBar(1, "test")
	defer p.Close()
	f, err := exportKlines(q, context.Background(), &ExportKlineJob{ExSymbol: &ExSymbol{ID: 1, Symbol: "BTC/USDT:USDT"}, TimeFrame: "1d", StartMS: 1735689600000, StopMS: 1736294400000}, t.TempDir(), nil, p)
	if f != nil {
		defer f.Close()
	}
	if err != nil {
		t.Fatalf("offline physical export rejected before reading rows: %v", err)
	}
}

func TestTransferImportFailureDoesNotHangWorkers(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "exInfo1.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if e := dumpProto(&EXInfo{Symbols: []*ExSymbolBlock{{Id: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}}}, f); e != nil {
		t.Fatal(e)
	}
	f.Close()
	for _, name := range []string{"kline1.dat", "kline2.dat", "kline3.dat"} {
		file, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := dumpProto(transferLegacyBlock(), file); err != nil {
			t.Fatal(err)
		}
		file.Close()
	}
	symbols := NewSymbolState()
	if err := symbols.SetExSymbols([]*ExSymbol{{ID: 9, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() {
		e := ImportDataWithDeps(dir, 1, KDataTransferDeps{Storage: &Storage{}, Symbols: symbols, KlineOptions: &KlineRuntimeOptions{}}, nil)
		done <- e != nil && strings.Contains(e.Error(), "storage pool is not configured")
	}()
	select {
	case failed := <-done:
		if !failed {
			t.Fatal("missing storage succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("import worker waits forever after cancellation")
	}
}

func transferLegacyBlock() *KlineBlock {
	return &KlineBlock{ExsId: 1, Timeframe: "1d", Start: 1735689600000, End: 1735776000000, Open: []float64{1}, High: []float64{2}, Low: []float64{0.5}, Close: []float64{1.5}, Volume: []float64{3}, Info: []float64{1}}
}

func TestTransferGenericRowsRoundTrip(t *testing.T) {
	fields := []KlineSchemaField{{Name: "open", Type: "double precision"}, {Name: "high", Type: "double precision"}, {Name: "low", Type: "double precision"}, {Name: "close", Type: "double precision"}, {Name: "volume", Type: "double precision"}, {Name: "quote", Type: "double precision"}, {Name: "buy_volume", Type: "double precision"}, {Name: "trade_num", Type: "bigint"}, {Name: "custom", Type: "text"}, {Name: "flag", Type: "boolean"}, {Name: "nullable", Type: "bigint"}, {Name: "json", Type: "jsonb"}}
	values := map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0, "quote": 1234.5, "buy_volume": 1.25, "trade_num": int64(9007199254740993), "custom": "中文", "flag": false, "nullable": nil, "json": map[string]any{"nested": []any{nil, true, "x"}}}
	rows := []*DataSeries{{TimeMS: 1735689600000, EndMS: 1735776000000, Values: values}, {TimeMS: 1735862400000, EndMS: 1735948800000, Values: values}}
	block := &KlineBlock{ExsId: 1, Timeframe: "1d"}
	if err := encodeTransferBlock(block, fields, rows, false); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "block")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := dumpProto(block, file); err != nil {
		t.Fatal(err)
	}
	file.Seek(0, 0)
	var wire KlineBlock
	if err := readProtoMessage(file, &wire); err != nil {
		t.Fatal(err)
	}
	got, schema, _, err := decodeTransferBlock(&wire, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Sid != 10001 || got[1].TimeMS != rows[1].TimeMS || !reflect.DeepEqual(got[0].Values, values) || !reflect.DeepEqual(schema, fields) {
		t.Fatalf("typed fields/NULLs/gaps/remap lost: %+v", got)
	}
}

type transferWriteTx struct {
	pgx.Tx
	schema  []KlineSchemaField
	cols    []string
	values  [][]any
	execs   []string
	batches []int
}

func (d *transferWriteTx) Exec(_ context.Context, sql string, _ ...interface{}) (pgconn.CommandTag, error) {
	d.execs = append(d.execs, sql)
	return pgconn.NewCommandTag("OK"), nil
}
func (d *transferWriteTx) Query(_ context.Context, _ string, _ ...interface{}) (pgx.Rows, error) {
	var rows [][]any
	for _, f := range d.schema {
		rows = append(rows, []any{f.Name, f.Type, true})
	}
	return newInterfaceRows(rows), nil
}
func (d *transferWriteTx) CopyFrom(_ context.Context, _ pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	d.cols = cols
	before := len(d.values)
	defer func() { d.batches = append(d.batches, len(d.values)-before) }()
	for src.Next() {
		v, e := src.Values()
		if e != nil {
			return 0, e
		}
		d.values = append(d.values, append([]any(nil), v...))
	}
	return int64(len(d.values)), src.Err()
}
func TestTransferImportUsesGenericTransactionalWriter(t *testing.T) {
	fields := []KlineSchemaField{{Name: "open", Type: "double precision"}, {Name: "high", Type: "double precision"}, {Name: "low", Type: "double precision"}, {Name: "close", Type: "double precision"}, {Name: "volume", Type: "double precision"}, {Name: "quote", Type: "double precision"}, {Name: "buy_volume", Type: "double precision"}, {Name: "trade_num", Type: "bigint"}, {Name: "custom", Type: "text"}}
	values := map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0, "quote": 1234.5, "buy_volume": 1.25, "trade_num": int64(9007199254740993), "custom": nil}
	block := &KlineBlock{ExsId: 1, Timeframe: "1d"}
	if err := encodeTransferBlock(block, fields, []*DataSeries{{TimeMS: 1735689600000, EndMS: 1735776000000, Values: values}}, false); err != nil {
		t.Fatal(err)
	}
	db := &transferWriteTx{schema: fields}
	storage := &Storage{}
	q := NewWithStorage(db, storage).WithKlineRuntimeOptions(KlineRuntimeOptions{Storage: storage, ClockValid: true, NowMS: 1736294400000})
	if err := importKlines(q, context.Background(), 10001, block); err != nil {
		t.Fatal(err)
	}
	if len(db.values) != 1 || db.values[0][0] != int32(10001) {
		t.Fatalf("wrong remap: %+v", db.values)
	}
	for i, col := range db.cols {
		if col == "sid" || col == "time" {
			continue
		}
		if !reflect.DeepEqual(db.values[0][i], values[col]) {
			t.Fatalf("column %s lost: %v != %v", col, db.values[0][i], values[col])
		}
	}
	if len(db.execs) < 2 || !strings.HasPrefix(db.execs[0], "SAVEPOINT ") || !strings.HasPrefix(db.execs[len(db.execs)-1], "RELEASE SAVEPOINT ") {
		t.Fatalf("transactional COPY path bypassed: %v", db.execs)
	}
}

type transferShortWriter struct{}

func (transferShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestTransferPropagatesShortWrite(t *testing.T) {
	if err := dumpProto(transferLegacyBlock(), transferShortWriter{}); err == nil || !strings.Contains(err.Error(), io.ErrShortWrite.Error()) {
		t.Fatalf("short write swallowed: %v", err)
	}
}
func TestTransferRejectsEmptyOrUnfinishedBeforeMutation(t *testing.T) {
	for _, version := range []uint32{0, transferArchiveVersion} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			dir := t.TempDir()
			f, _ := os.Create(filepath.Join(dir, "exInfo1.dat"))
			if err := dumpProto(&EXInfo{ArchiveVersion: version, Symbols: []*ExSymbolBlock{{Id: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}}}, f); err != nil {
				t.Fatal(err)
			}
			f.Close()
			os.WriteFile(filepath.Join(dir, "kline1.dat"), nil, 0600)
			e := ImportDataWithDeps(dir, 1, KDataTransferDeps{Storage: &Storage{}, Symbols: NewSymbolState(), KlineOptions: &KlineRuntimeOptions{}}, nil)
			if e == nil || strings.Contains(e.Error(), "storage pool") {
				t.Fatalf("invalid archive reached target storage: %v", e)
			}
		})
	}
}
func TestTransferLegacyArchiveAndMalformedArrays(t *testing.T) {
	b := transferLegacyBlock()
	rows, fields, _, err := decodeTransferBlock(b, 1730)
	if err != nil || len(rows) != 1 || len(fields) != 0 || rows[0].Sid != 1730 || rows[0].Values["buy_volume"] != 1.0 {
		t.Fatalf("legacy archive lost: %+v %v", rows, err)
	}
	b.High = nil
	if _, _, _, err := decodeTransferBlock(b, 1730); err == nil {
		t.Fatal("malformed arrays accepted")
	}
}

func TestTransferCompletionManifestDetectsRemovedBlockAndFile(t *testing.T) {
	for _, removeFile := range []bool{false, true} {
		t.Run(fmt.Sprint(removeFile), func(t *testing.T) {
			dir := t.TempDir()
			info := &EXInfo{ArchiveVersion: transferArchiveVersion, Symbols: []*ExSymbolBlock{{Id: 1}}}
			meta, _ := os.Create(filepath.Join(dir, "exInfo1.dat"))
			if err := dumpProto(info, meta); err != nil {
				t.Fatal(err)
			}
			meta.Close()
			path := filepath.Join(dir, "kline1.dat")
			f, _ := os.Create(path)
			dumpProto(transferLegacyBlock(), f)
			boundary, _ := f.Seek(0, io.SeekCurrent)
			dumpProto(transferLegacyBlock(), f)
			f.Close()
			if err := writeTransferManifest(dir, []string{path}); err != nil {
				t.Fatal(err)
			}
			if err := validateTransferArchive(dir, info, []string{path}); err != nil {
				t.Fatal(err)
			}
			files := []string{path}
			if removeFile {
				os.Remove(path)
				files = nil
			} else {
				os.Truncate(path, boundary)
			}
			if err := validateTransferArchive(dir, info, files); err == nil {
				t.Fatal("incomplete archive accepted")
			}
		})
	}
}
func TestTransferSparseRowsUseSeparateWriteBatches(t *testing.T) {
	values := map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0}
	fields := []KlineSchemaField{{Name: "open", Type: "double precision"}, {Name: "high", Type: "double precision"}, {Name: "low", Type: "double precision"}, {Name: "close", Type: "double precision"}, {Name: "volume", Type: "double precision"}}
	block := &KlineBlock{ExsId: 1, Timeframe: "1d"}
	if err := encodeTransferBlock(block, fields, []*DataSeries{{TimeMS: 1735689600000, EndMS: 1735776000000, Values: values}, {TimeMS: 1735862400000, EndMS: 1735948800000, Values: values}}, false); err != nil {
		t.Fatal(err)
	}
	db := &transferWriteTx{schema: fields}
	q := NewWithStorage(db, &Storage{}).WithKlineRuntimeOptions(KlineRuntimeOptions{ClockValid: true, NowMS: 1736294400000})
	if err := importKlines(q, context.Background(), 99, block); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(db.batches, []int{1, 1}) {
		t.Fatalf("sparse rows batched across gap: %v", db.batches)
	}
}
func TestTransferZeroLengthFrameRejected(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "bad")
	defer f.Close()
	f.Write([]byte{0, 0, 0, 0, 1})
	f.Seek(0, 0)
	if err := readProtoMessage(f, new(KlineBlock)); err == nil {
		t.Fatal("zero frame accepted")
	}
}

func TestTransferPoolWrapperBeginsBeforeSavepoint(t *testing.T) {
	q := New(&SubQueries{db: beginDelegatingDB{}})
	_, err := q.insertOHLCVSeriesPg("1d", nil)
	if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("did not call underlying Begin: %v", err)
	}
}
func TestTransferTransactionClassification(t *testing.T) {
	for _, db := range []DBTX{(*pgx.Conn)(nil), (*pgxpool.Conn)(nil), (*pgxpool.Pool)(nil), &SubQueries{db: beginDelegatingDB{}}, &SubQueries{db: &SubQueries{db: beginDelegatingDB{}}}} {
		if isOHLCVTransaction(db) {
			t.Fatalf("root %T mistaken for transaction", db)
		}
	}
	tx := &transferWriteTx{}
	if !isOHLCVTransaction(tx) || !isOHLCVTransaction(&SubQueries{db: tx}) {
		t.Fatal("actual transaction not recognized")
	}
}

type transferRootDB struct {
	DBTX
	tx     *transferCommitTx
	begins int
}

func (d *transferRootDB) Begin(context.Context) (pgx.Tx, error) { d.begins++; return d.tx, nil }

type transferCommitTx struct {
	transferWriteTx
	commits int
}

func (d *transferCommitTx) Commit(context.Context) error   { d.commits++; return nil }
func (d *transferCommitTx) Rollback(context.Context) error { return nil }
func TestTransferWrappedRootCommitsAndOuterTransactionRemainsOwned(t *testing.T) {
	tx := &transferCommitTx{}
	root := &transferRootDB{tx: tx}
	q := New(&SubQueries{db: root})
	rows := []*DataSeries{{Sid: 99, TimeMS: 1735689600000, EndMS: 1735776000000, Values: map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0}}}
	if _, err := q.insertOHLCVSeriesPg("1d", rows); err != nil {
		t.Fatal(err)
	}
	if root.begins != 1 || tx.commits != 1 || len(tx.execs) < 2 || !strings.HasPrefix(tx.execs[0], "SAVEPOINT ") {
		t.Fatalf("root transaction missing: begins=%d commits=%d execs=%v", root.begins, tx.commits, tx.execs)
	}
	outer := New(&SubQueries{db: tx})
	if _, err := outer.insertOHLCVSeriesPg("1d", rows); err != nil {
		t.Fatal(err)
	}
	if tx.commits != 1 {
		t.Fatal("writer committed caller-owned outer transaction")
	}
}

type transferContextCall struct {
	name     string
	ctx      context.Context
	err      error
	deadline time.Time
}
type transferContextTx struct {
	transferWriteTx
	calls        []transferContextCall
	block        chan struct{}
	duplicate    bool
	cancelCommit context.CancelFunc
	copies       int
}

func (d *transferContextTx) record(name string, ctx context.Context) {
	deadline, _ := ctx.Deadline()
	d.calls = append(d.calls, transferContextCall{name, ctx, ctx.Err(), deadline})
}
func (d *transferContextTx) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	d.record(sql, ctx)
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	return d.transferWriteTx.Exec(ctx, sql, args...)
}
func (d *transferContextTx) CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	d.record("COPY", ctx)
	d.copies++
	if d.block != nil {
		close(d.block)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if d.duplicate && d.copies == 1 {
		return 0, fmt.Errorf("duplicate key")
	}
	return d.transferWriteTx.CopyFrom(ctx, table, cols, src)
}
func (d *transferContextTx) Commit(ctx context.Context) error {
	d.record("COMMIT", ctx)
	if d.cancelCommit != nil {
		d.cancelCommit()
	}
	return ctx.Err()
}
func (d *transferContextTx) Rollback(ctx context.Context) error {
	d.record("ROLLBACK", ctx)
	return ctx.Err()
}

type transferContextRoot struct {
	DBTX
	tx       *transferContextTx
	beginCtx context.Context
}

func (d *transferContextRoot) Begin(ctx context.Context) (pgx.Tx, error) {
	d.beginCtx = ctx
	return d.tx, ctx.Err()
}

func TestTransferContextThroughRetryAndCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := &transferContextTx{duplicate: true}
	root := &transferContextRoot{tx: tx}
	q := NewWithStorage(&SubQueries{db: root}, &Storage{}).WithKlineRuntimeOptions(KlineRuntimeOptions{ClockValid: true, NowMS: 1736294400000})
	if err := importKlines(q, ctx, 99, transferLegacyBlock()); err != nil {
		t.Fatal(err)
	}
	if root.beginCtx != ctx {
		t.Fatal("Begin lost caller context")
	}
	var names []string
	for _, call := range tx.calls {
		names = append(names, call.name)
		if call.ctx != ctx {
			t.Fatalf("%s lost caller context", call.name)
		}
	}
	if !reflect.DeepEqual(names, []string{"SAVEPOINT banbot_kline_copy", "COPY", "ROLLBACK TO SAVEPOINT banbot_kline_copy", "DELETE FROM kline_1d WHERE sid = $1 AND time >= $2 AND time < $3", "COPY", "RELEASE SAVEPOINT banbot_kline_copy", "COMMIT"}) {
		t.Fatalf("unexpected transaction operations: %v", names)
	}
}
func TestTransferActiveCopyCancellationAndBoundedCleanup(t *testing.T) {
	for _, outer := range []bool{false, true} {
		t.Run(fmt.Sprint(outer), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx := &transferContextTx{block: make(chan struct{})}
			root := &transferContextRoot{tx: tx}
			var db DBTX = root
			if outer {
				db = tx
			}
			q := NewWithStorage(&SubQueries{db: db}, &Storage{}).WithKlineRuntimeOptions(KlineRuntimeOptions{ClockValid: true, NowMS: 1736294400000})
			done := make(chan error, 1)
			go func() {
				err := importKlines(q, ctx, 99, transferLegacyBlock())
				if err != nil {
					done <- err
				} else {
					done <- nil
				}
			}()
			select {
			case <-tx.block:
			case <-time.After(time.Second):
				t.Fatal("COPY not reached")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
					t.Fatalf("cancellation not returned: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("active COPY ignored cancellation")
			}
			rootRollbacks := 0
			cleanupCalls := 0
			for _, call := range tx.calls {
				if strings.HasPrefix(call.name, "DELETE") || call.name == "COMMIT" {
					t.Fatalf("canceled COPY continued mutation: %s", call.name)
				}
				if call.ctx != ctx {
					cleanupCalls++
					if call.err != nil || call.deadline.IsZero() || time.Until(call.deadline) > 5*time.Second {
						t.Fatalf("cleanup canceled or unbounded: %+v", call)
					}
				}
				if call.name == "ROLLBACK" {
					rootRollbacks++
				}
			}
			if cleanupCalls < 2 || (!outer && rootRollbacks != 1) || (outer && rootRollbacks != 0) {
				t.Fatalf("wrong cleanup ownership: cleanup=%d rootRollbacks=%d outer=%v", cleanupCalls, rootRollbacks, outer)
			}
		})
	}
}
func TestTransferAlreadyCanceledDoesNotBegin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root := &transferContextRoot{tx: &transferContextTx{}}
	q := NewWithStorage(root, &Storage{}).WithKlineRuntimeOptions(KlineRuntimeOptions{ClockValid: true, NowMS: 1736294400000})
	if _, err := q.InsertOHLCVSeriesContext(ctx, "1d", 99, nil); err == nil {
		t.Fatal("canceled write succeeded")
	}
	if root.beginCtx != nil {
		t.Fatal("canceled write began transaction")
	}
}

func TestTransferCommitCancellationRollsBackOwnedTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := &transferContextTx{cancelCommit: cancel}
	root := &transferContextRoot{tx: tx}
	q := NewWithStorage(root, &Storage{}).WithKlineRuntimeOptions(KlineRuntimeOptions{ClockValid: true, NowMS: 1736294400000})
	if err := importKlines(q, ctx, 99, transferLegacyBlock()); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("Commit cancellation lost: %v", err)
	}
	last := tx.calls[len(tx.calls)-1]
	if last.name != "ROLLBACK" || last.err != nil || last.ctx == ctx || last.deadline.IsZero() {
		t.Fatalf("Commit failure cleanup unsafe: %+v", last)
	}
}
