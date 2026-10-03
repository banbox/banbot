package entry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type klineEntryExchange struct{ liveEntryExchange }

func (*klineEntryExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	return &banexg.Market{Symbol: symbol, Type: "linear", Combined: true}, nil
}

// This wire fixture implements trust startup and simple SELECT only. Its two
// allowed SQL shapes are adjustment lookup and kline_1m projections for SID 1.
// It is an explicit connection/row fixture, not a PostgreSQL or WAL emulator.
func factorKlineStorageFixture(t *testing.T, minute int64) (*orm.Storage, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var adjustmentReads atomic.Int32
	var workers sync.WaitGroup
	var connections sync.Map
	queryShape := regexp.MustCompile(`(?is)^SELECT time,(.+) FROM kline_1m\s+WHERE sid=1 AND ((?:time >= \d+ AND )?time < \d+)\s+ORDER BY time(?: DESC)?(?: LIMIT \d+)?$`)
	adjustmentShape := regexp.MustCompile(`(?s)^SELECT sid, sub_id, start_ms, factor\s+FROM adj_factors WHERE sid =\s*'?1'?\s+ORDER BY start_ms$`)
	numbers := regexp.MustCompile(`time (?:>=|<) (\d+)`)
	t.Cleanup(func() {
		_ = listener.Close()
		connections.Range(func(k, _ any) bool { _ = k.(net.Conn).Close(); return true })
		workers.Wait()
	})
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(conn, true)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer connections.Delete(conn)
				defer conn.Close()
				backend := pgproto3.NewBackend(conn, conn)
				if _, err := backend.ReceiveStartupMessage(); err != nil {
					return
				}
				backend.Send(&pgproto3.AuthenticationOk{})
				backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
				backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"})
				backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if err := backend.Flush(); err != nil {
					return
				}
				for {
					message, err := backend.Receive()
					if err != nil {
						return
					}
					if _, ok := message.(*pgproto3.Terminate); ok {
						return
					}
					query, ok := message.(*pgproto3.Query)
					if !ok {
						t.Errorf("unsupported fixture protocol %T", message)
						return
					}
					sql := strings.TrimSpace(query.String)
					var fields []pgproto3.FieldDescription
					var rows [][][]byte
					if adjustmentShape.MatchString(sql) {
						adjustmentReads.Add(1)
						for i, name := range []string{"sid", "sub_id", "start_ms", "factor"} {
							oid := uint32(20)
							if i == 3 {
								oid = 701
							}
							fields = append(fields, pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: 8})
						}
						rows = [][][]byte{{[]byte("1"), []byte("0"), []byte("0"), []byte("1")}}
					} else if match := queryShape.FindStringSubmatch(sql); match != nil {
						columns := append([]string{"time"}, strings.Split(match[1], ",")...)
						for i := range columns {
							columns[i] = strings.Trim(strings.TrimSpace(columns[i]), `"`)
						}
						for _, name := range columns {
							oid := uint32(701)
							switch name {
							case "time", "integer", "nullable":
								oid = 20
							case "label":
								oid = 25
							case "flag":
								oid = 16
							case "open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num":
							default:
								t.Errorf("unexpected fixture field %s", name)
								return
							}
							fields = append(fields, pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: -1})
						}
						lower, upper := int64(0), minute
						for _, bound := range numbers.FindAllStringSubmatch(match[2], -1) {
							value, _ := strconv.ParseInt(bound[1], 10, 64)
							if strings.Contains(bound[0], ">=") {
								lower = value
							} else {
								upper = value
							}
						}
						// OHLCV coverage and projected columns describe the same
						// three visible, closed historical bars.
						starts := []int64{minute - 180000, minute - 120000, minute - 60000}
						if strings.Contains(sql, "DESC") {
							for i, j := 0, len(starts)-1; i < j; i, j = i+1, j-1 {
								starts[i], starts[j] = starts[j], starts[i]
							}
						}
						for _, at := range starts {
							if at < lower || at >= upper {
								continue
							}
							row := make([][]byte, len(columns))
							for i, name := range columns {
								switch name {
								case "time":
									row[i] = []byte(strconv.FormatInt(at, 10))
								case "integer":
									row[i] = []byte("17")
								case "label":
									row[i] = []byte("typed")
								case "flag":
									row[i] = []byte("t")
								case "nullable":
									row[i] = nil
								default:
									row[i] = []byte("100")
								}
							}
							rows = append(rows, row)
						}
					} else {
						t.Errorf("unexpected fixture SQL: %s", sql)
						return
					}
					backend.Send(&pgproto3.RowDescription{Fields: fields})
					for _, values := range rows {
						backend.Send(&pgproto3.DataRow{Values: values})
					}
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(rows)))})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err := backend.Flush(); err != nil {
						return
					}
				}
			}()
		}
	}()
	poolConfig, err := pgxpool.ParseConfig("postgres://fixture@" + listener.Addr().String() + "/fixture?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	poolConfig.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return orm.NewStorage(pool, false, t.TempDir()), &adjustmentReads
}

func TestFactorLiveEntryKlineStorageWarmupAndTypedLiveCallback(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	// Keep the fixture's historical minute stable across the real clock startup.
	if remaining := 60000 - time.Now().UnixMilli()%60000; remaining < 2000 {
		time.Sleep(time.Duration(remaining) * time.Millisecond)
	}
	minute := time.Now().UnixMilli() / 60000 * 60000
	storage, adjustmentReads := factorKlineStorageFixture(t, minute)
	spider, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer spider.Close()
	warmed := make(chan struct{})
	var warmCount, liveCount atomic.Int32
	serverDone := make(chan struct{})
	var spiderConnMu sync.Mutex
	var spiderConn net.Conn
	defer func() {
		cancel()
		_ = spider.Close()
		spiderConnMu.Lock()
		if spiderConn != nil {
			_ = spiderConn.Close()
		}
		spiderConnMu.Unlock()
		<-serverDone
	}()
	go func() {
		defer close(serverDone)
		conn, err := spider.Accept()
		if err != nil {
			return
		}
		spiderConnMu.Lock()
		spiderConn = conn
		spiderConnMu.Unlock()
		defer conn.Close()
		if ctx.Err() != nil {
			return
		}
		writer := &utils.BanConn{Conn: conn, Ready: true}
		select {
		case <-warmed:
		case <-ctx.Done():
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			msg, readErr := writer.ReadMsg()
			if readErr != nil {
				if ctx.Err() == nil {
					t.Errorf("spider subscription: %v", readErr)
				}
				return
			}
			if msg.Action == "watch_pairs" {
				break
			}
		}
		row := &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: "1m", TimeMS: minute - 60000, EndMS: minute, Closed: true, Values: map[string]any{"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0}}
		if err := writer.WriteMsg(&utils.IOMsg{Action: "ohlcv_entrytest_linear_asset-1", Data: &data.NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{row}}}); err != nil {
			if ctx.Err() == nil {
				t.Errorf("spider fixture write: %v", err)
			}
			return
		}
		<-ctx.Done()
	}()
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Chunks = nil
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Snapshot.Universe = factor.Universe{Version: "single", Static: true, Investable: []int32{1}, Reference: []int32{1}, Tradable: []int32{1}, Tracked: []int32{1}, Evaluation: []int32{1}}
	c.Snapshot.SIDMap = map[int32]string{1: "asset-1"}
	delete(c.Execution.Instruments, 2)
	delete(c.Execution.Instruments, 3)
	c.Execution.StorePath, c.Execution.SenderLeaseDir = filepath.Join(dir, "account.db"), filepath.Join(dir, "lease")
	c.DecisionInterval = 60000
	c.Prices = runner.PriceStream{Source: "kline", Frequency: "1m", Field: "close"}
	c.Plan, err = factor.New().Add("integer", factor.Lag(factor.Field("kline", "integer", "1m"), 3)).Add("label", factor.Field("kline", "label", "1m")).Add("flag", factor.Field("kline", "flag", "1m")).Add("nullable", factor.Field("kline", "nullable", "1m")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"integer"}, Weights: map[string]float64{"integer": 1}}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: c.AccountID, SettlementDomain: "USD"}
	binding := FactorLiveBinding{Account: key, Transport: &liveEntryTransport{key: key}, Symbols: map[int32]*orm.ExSymbol{1: {ID: 1, Symbol: "asset-1", Exchange: "entrytest", Market: "linear", Combined: true}}, VerifyFunding: func(context.Context, string) (string, error) { return "fixture-explicit-zero", nil }, Record: func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		if s.Values["integer"] != int64(17) || s.Values["label"] != "typed" || s.Values["flag"] != true {
			return factor.VersionRecord{}, fmt.Errorf("typed projection lost: %#v", s.Values)
		}
		if value, ok := s.Values["nullable"]; !ok || value != nil {
			return factor.VersionRecord{}, errors.New("NULL projection lost")
		}
		if s.IsWarmUp {
			if warmCount.Add(1) == 3 {
				close(warmed)
			}
		} else {
			liveCount.Add(1)
			cancel()
		}
		return factor.VersionRecord{Series: *s, EventTime: s.EndMS, AvailableAt: s.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}}
	exchange := &klineEntryExchange{liveEntryExchange: liveEntryExchange{trades: make(chan *banexg.MyTrade)}}
	cfg := &config.Config{Exchange: &config.ExchangeConfig{Name: "entrytest"}, MarketType: "linear", SpiderAddr: spider.Addr().String()}
	snapshot := config.NewSnapshotWithDirs(cfg, dir, "", nil)
	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	session := &explicitEntrySession{process: runtime.NewProcess(), ctx: sessionCtx, cancel: sessionCancel, exchange: exchange, storage: storage}
	defer session.close()
	err = session.runFactorLive(ctx, snapshot, c, binding, io.Discard)
	session.close()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("kline entry: %v", err)
	}
	if adjustmentReads.Load() == 0 || warmCount.Load() != 3 || liveCount.Load() != 1 {
		t.Fatalf("incomplete constructor: adjustments=%d warm=%d live=%d", adjustmentReads.Load(), warmCount.Load(), liveCount.Load())
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("spider fixture not joined")
	}
}
