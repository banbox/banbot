package base

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

const runtimeKlineStartMS int64 = 1775001600000

var runtimeKlineCoverageSQL = regexp.MustCompile(`^SELECT start_ms, stop_ms, has_data, ts FROM \( SELECT start_ms, stop_ms, has_data, is_deleted, ts FROM \(SELECT \* FROM sranges_q LATEST BY sid, tbl, timeframe, start_ms WHERE sid = '([12])' AND tbl = 'kline_1m' AND timeframe = '1m' AND start_ms < '1775002200000' \) WHERE sid = '[12]' AND tbl = 'kline_1m' AND timeframe = '1m' AND stop_ms > '1775001600000' AND start_ms < '1775002200000' \) WHERE coalesce\(is_deleted, false\) = false ORDER BY start_ms, stop_ms$`)
var runtimeKlineInsertSQL = regexp.MustCompile(`^INSERT INTO ins_kline_q \(sid, timeframe, ts, start_ms, stop_ms, is_deleted\) VALUES \( '[12]' , '1m' , '[^']+' , '1775001600000' , '1775002200000' , false\)$`)

// Only metadata operations necessary for an empty-range download are allowed.
// The adapter intentionally fails before any K-line INSERT can take place.
func runtimeKlineDownloadSQL(backend *pgproto3.Backend, sql string) bool {
	var fields []pgproto3.FieldDescription
	var values [][]byte
	switch {
	case runtimeKlineCoverageSQL.MatchString(sql):
		for i, name := range []string{"start_ms", "stop_ms", "has_data", "ts"} {
			oid := uint32(20)
			if i == 2 {
				oid = 16
			}
			if i == 3 {
				oid = 1114
			}
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: -1})
		}
	case sql == "SELECT sequencerTxn, writerTxn, writerLagTxnCount, suspended FROM wal_tables() WHERE name = 'ins_kline_q'":
		for i, name := range []string{"sequencerTxn", "writerTxn", "writerLagTxnCount", "suspended"} {
			oid := uint32(20)
			if i == 3 {
				oid = 16
			}
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: -1})
		}
		values = [][]byte{[]byte("0"), []byte("0"), []byte("0"), []byte("f")}
	case sql == `SELECT coalesce(cast(max(ts) as long), 0) FROM "ins_kline_q"`:
		fields = []pgproto3.FieldDescription{{Name: []byte("coalesce"), DataTypeOID: 20, DataTypeSize: 8}}
		values = [][]byte{[]byte("0")}
	case runtimeKlineInsertSQL.MatchString(sql):
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return true
	default:
		return false
	}
	backend.Send(&pgproto3.RowDescription{Fields: fields})
	count := 0
	if values != nil {
		backend.Send(&pgproto3.DataRow{Values: values})
		count = 1
	}
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", count))})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return true
}

type runtimeKlineDownloadExchange struct {
	banexg.BanExchange
	called    int
	symbol    string
	timeframe string
	since     int64
	limit     int
}

func (e *runtimeKlineDownloadExchange) FetchOHLCV(symbol, timeframe string, since int64, limit int, params map[string]interface{}) ([]*banexg.Kline, *errs.Error) {
	e.called++
	e.symbol = symbol
	e.timeframe = timeframe
	e.since = since
	e.limit = limit
	return nil, errs.NewMsg(errs.CodeParamInvalid, "runtime adapter download sentinel")
}

// Trust startup, a narrowly matched 5m SELECT and required download metadata
// exercise the real pool, ORM and HTTP route without external services.
func runtimeKlineStorage(t *testing.T, price int) *orm.Storage {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	var connections sync.Map
	shape := regexp.MustCompile(`^select cast\(ts as long\)/1000,(.+) from kline_5m where sid=([12]) and ts >= cast\(1775001600000000 as timestamp\) and ts < cast\(1775002200000000 as timestamp\) order by ts limit (?:3|100)$`)
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
				for name, value := range map[string]string{"client_encoding": "UTF8", "server_version": "15.0", "standard_conforming_strings": "on"} {
					backend.Send(&pgproto3.ParameterStatus{Name: name, Value: value})
				}
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
						t.Errorf("unexpected fixture message %T", message)
						return
					}
					sql := strings.Join(strings.Fields(query.String), " ")
					if runtimeKlineDownloadSQL(backend, sql) {
						if err := backend.Flush(); err != nil {
							return
						}
						continue
					}
					match := shape.FindStringSubmatch(sql)
					if match == nil {
						t.Errorf("unexpected fixture SQL: %s", sql)
						return
					}
					columns := append([]string{"time"}, strings.Split(match[1], ",")...)
					var fields []pgproto3.FieldDescription
					for i, name := range columns {
						name = strings.Trim(name, `"`)
						columns[i] = name
						oid := uint32(701)
						switch name {
						case "time", "trade_num", "nullable":
							oid = 20
						case "label":
							oid = 25
						case "flag":
							oid = 16
						case "open", "high", "low", "close", "volume", "quote", "buy_volume":
						default:
							t.Errorf("unexpected fixture column %q", name)
							return
						}
						fields = append(fields, pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: -1})
					}
					backend.Send(&pgproto3.RowDescription{Fields: fields})
					sid, _ := strconv.Atoi(match[2])
					for rowIndex := 0; rowIndex < 2; rowIndex++ {
						values := make([][]byte, len(columns))
						for i, name := range columns {
							switch name {
							case "time":
								values[i] = []byte(strconv.FormatInt(runtimeKlineStartMS+int64(rowIndex)*300000, 10))
							case "trade_num":
								values[i] = []byte("17")
							case "label":
								values[i] = []byte("runtime")
							case "flag":
								values[i] = []byte("t")
							case "nullable":
								values[i] = nil
							default:
								values[i] = []byte(strconv.Itoa(price + sid + rowIndex))
							}
						}
						backend.Send(&pgproto3.DataRow{Values: values})
					}
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err := backend.Flush(); err != nil {
						return
					}
				}
			}()
		}
	}()
	cfg, err := pgxpool.ParseConfig("postgres://fixture@" + listener.Addr().String() + "/fixture?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return orm.NewStorage(pool, true, t.TempDir())
}

func runtimeKlineDeps(t *testing.T, price int) data.RuntimeDeps {
	t.Helper()
	symbols := orm.NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	if err := symbols.SetExSymbols([]*orm.ExSymbol{
		{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "BTC/USDT:USDT"},
		{ID: 2, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "SNDK/USDT:USDT"},
	}); err != nil {
		t.Fatal(err)
	}
	clock := btime.NewClockState(true, time.UTC)
	clock.SetTimeMS(runtimeKlineStartMS + 3600000)
	deps := data.RuntimeDeps{
		Core:  &core.State{ExgName: "binance", Market: banexg.MarketLinear, BackTestMode: true},
		Clock: clock, Config: config.NewSnapshot(&config.Config{BTNoKlineDownload: true}),
		Symbols: symbols, Storage: runtimeKlineStorage(t, price), Catalog: data.NewDataSourceCatalog(),
		Exchange: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}},
	}
	return deps
}

func runtimeKlineResponse(t *testing.T, app *fiber.App, path string) []byte {
	t.Helper()
	response, err := app.Test(httptest.NewRequest("GET", path, nil), 5000)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		t.Fatalf("GET %s: HTTP %d: %s", path, response.StatusCode, body)
	}
	return body
}

func TestRuntimeKlineHTTPReadsStoredData(t *testing.T) {
	for _, price := range []int{100, 200} {
		app := fiber.New()
		RegApiKlineWithRuntimeDeps(app.Group("/api/kline"), runtimeKlineDeps(t, price))
		for sid, symbol := range []string{"BTC/USDT:USDT", "SNDK/USDT:USDT"} {
			for _, suffix := range []string{"", "&strategy"} {
				t.Run(fmt.Sprintf("price%d/%s/strategy%s", price, symbol, suffix), func(t *testing.T) {
					path := fmt.Sprintf("/api/kline/hist?exchange=binance&symbol=%s&timeframe=5m&from=%d&to=%d%s", url.QueryEscape(symbol), runtimeKlineStartMS, runtimeKlineStartMS+600000, suffix)
					var got struct {
						Data [][]float64 `json:"data"`
					}
					if err := json.Unmarshal(runtimeKlineResponse(t, app, path), &got); err != nil {
						t.Fatal(err)
					}
					if len(got.Data) != 2 || len(got.Data[0]) < 6 || got.Data[0][0] != float64(runtimeKlineStartMS) || got.Data[0][1] != float64(price+sid+1) || got.Data[1][1] != float64(price+sid+2) {
						t.Fatalf("unexpected historical bars: %v", got.Data)
					}
				})
			}
			t.Run(fmt.Sprintf("price%d/%s/series", price, symbol), func(t *testing.T) {
				path := fmt.Sprintf("/api/kline/series?source=kline&sid=%d&timeframe=5m&fields=close,trade_num,label,flag,nullable&start=%d&end=%d", sid+1, runtimeKlineStartMS, runtimeKlineStartMS+600000)
				var got struct {
					Data []*orm.DataSeries `json:"data"`
				}
				if err := json.Unmarshal(runtimeKlineResponse(t, app, path), &got); err != nil {
					t.Fatal(err)
				}
				if len(got.Data) != 2 || got.Data[0].TimeMS != runtimeKlineStartMS {
					t.Fatalf("unexpected series: %#v", got.Data)
				}
				v := got.Data[0].Values
				if v["close"] != float64(price+sid+1) || v["trade_num"] != float64(17) || v["label"] != "runtime" || v["flag"] != true {
					t.Fatalf("unexpected fields: %#v", v)
				}
				if nullable, ok := v["nullable"]; !ok || nullable != nil {
					t.Fatalf("NULL field lost: %#v", v)
				}
			})
		}
	}
}

func TestRuntimeKlineHTTPMissingDataUsesRuntimeAdapter(t *testing.T) {
	for _, symbol := range []string{"BTC/USDT:USDT", "SNDK/USDT:USDT"} {
		t.Run(symbol, func(t *testing.T) {
			deps := runtimeKlineDeps(t, 100)
			deps.Config = config.NewSnapshot(&config.Config{})
			adapter := &runtimeKlineDownloadExchange{BanExchange: deps.Exchange}
			deps.Exchange = adapter
			app := fiber.New()
			RegApiKlineWithRuntimeDeps(app.Group("/api/kline"), deps)
			path := fmt.Sprintf("/api/kline/hist?exchange=binance&symbol=%s&timeframe=5m&from=%d&to=%d&strategy", url.QueryEscape(symbol), runtimeKlineStartMS, runtimeKlineStartMS+600000)
			response, err := app.Test(httptest.NewRequest("GET", path, nil), 5000)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != 500 || !strings.Contains(string(body), "runtime adapter download sentinel") || adapter.called != 1 || adapter.symbol != symbol || adapter.timeframe != "1m" || adapter.since != runtimeKlineStartMS || adapter.limit <= 0 {
				t.Fatalf("download did not reach runtime adapter: HTTP %d, calls=%d, symbol=%q, body=%s", response.StatusCode, adapter.called, adapter.symbol, body)
			}
		})
	}
}
