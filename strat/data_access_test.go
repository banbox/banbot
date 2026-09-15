package strat

import (
	"fmt"
	"testing"

	"github.com/banbox/banbot/orm"
)

func TestNewDataSubDerivesIdentity(t *testing.T) {
	info := orm.NewSeriesInfo("macro", "1d", nil)
	sub := NewDataSub(info)
	if sub == nil || sub.Source != "macro" || sub.TimeFrame != "1d" {
		t.Fatalf("unexpected sub: %#v", sub)
	}
	sub.Fields = []string{"value"}
	sub.SeriesFields = []string{"value"}
	sub.WarmupNum = 3
	if got := NewDataSub(nil); got != nil {
		t.Fatalf("NewDataSub(nil) = %#v", got)
	}
	// Source identity comes from the definition, never a physical-table guess.
	extension := orm.NewKLineSeriesInfo("open_interest", "1h", nil)
	if got := NewDataSub(extension); got.Source != "open_interest" {
		t.Fatalf("unexpected implicit source rewriting: %+v", got)
	}
}

func TestStratJobDataUsesCanonicalIdentityAndPreservesValues(t *testing.T) {
	canonical := &orm.ExSymbol{ID: 7, Exchange: "x", Market: "spot", Symbol: "BTC/USDT"}
	job := &StratJob{Symbol: canonical, DataHub: NewDataHub()}
	sub := &DataSub{Source: "macro", TimeFrame: "1d"}
	job.DataHub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeFrame: "1d", Values: map[string]any{"n": int64(9007199254740993), "nil": nil}})
	fields := job.Data(sub)
	if fields == nil || fields.Sid != 7 || fields.Source != "macro" {
		t.Fatalf("job.Data returned %#v", fields)
	}
	if v, ok := fields.RawValue("n"); !ok || v != int64(9007199254740993) {
		t.Fatalf("int64 value lost: %#v %v", v, ok)
	}
	if v, ok := fields.RawValue("nil"); !ok || v != nil {
		t.Fatalf("explicit nil lost: %#v %v", v, ok)
	}
	if got := job.Data(nil); got != nil {
		t.Fatalf("job.Data(nil) = %#v", got)
	}
}

func TestStratJobDataIdentityIsolation(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("binance", "spot")
	if err := state.SetExSymbols([]*orm.ExSymbol{
		{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		{ID: 9, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"},
	}); err != nil {
		t.Fatal(err)
	}
	canonical := state.GetSymbolByID(7)
	job := &StratJob{Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}, DataHub: NewDataHub(), symbols: state}
	job.DataHub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeFrame: "1d", Values: map[string]any{"v": 1}})
	job.DataHub.Set(&orm.DataSeries{Source: "macro", Sid: 9, TimeFrame: "1d", Values: map[string]any{"v": 2}})
	other := &orm.ExSymbol{ID: 8, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	sameSIDDifferentName := &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	sameNameDifferentSID := &orm.ExSymbol{ID: 8, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	cases := []struct {
		name string
		sub  *DataSub
		want bool
	}{
		{"current", &DataSub{Source: "macro", TimeFrame: "1d"}, true},
		{"explicit current", &DataSub{Source: "macro", ExSymbol: job.Symbol, TimeFrame: "1d"}, true},
		{"subscribed other", &DataSub{Source: "macro", ExSymbol: state.GetSymbolByID(9), TimeFrame: "1d"}, true},
		{"other sid", &DataSub{Source: "macro", ExSymbol: other, TimeFrame: "1d"}, false},
		{"same sid different symbol", &DataSub{Source: "macro", ExSymbol: sameSIDDifferentName, TimeFrame: "1d"}, false},
		{"same symbol different sid", &DataSub{Source: "macro", ExSymbol: sameNameDifferentSID, TimeFrame: "1d"}, false},
		{"other source", &DataSub{Source: "flow", TimeFrame: "1d"}, false},
		{"other timeframe", &DataSub{Source: "macro", TimeFrame: "1h"}, false},
		{"nil sub", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if (job.Data(tc.sub) != nil) != tc.want {
				t.Fatalf("Data(%#v) presence mismatch", tc.sub)
			}
		})
	}
	if got := (&StratJob{Symbol: canonical, DataHub: nil}).Data(&DataSub{Source: "macro", TimeFrame: "1d"}); got != nil {
		t.Fatal("nil hub returned data")
	}
	if got := (&StratJob{DataHub: NewDataHub()}).Data(&DataSub{Source: "macro", TimeFrame: "1d"}); got != nil {
		t.Fatal("nil symbol returned data")
	}
	if got := (*StratJob)(nil).Data(&DataSub{Source: "macro", TimeFrame: "1d"}); got != nil {
		t.Fatal("nil job returned data")
	}
	otherJob := &StratJob{Symbol: canonical, DataHub: NewDataHub(), symbols: state}
	otherJob.DataHub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeFrame: "1d", Values: map[string]any{"v": 99}})
	sub := &DataSub{Source: "macro", TimeFrame: "1d"}
	if job.Data(sub).Raw("v") != 1 || otherJob.Data(sub).Raw("v") != 99 {
		t.Fatal("data leaked between jobs with identical subscription identities")
	}
}

func BenchmarkDataHubConsume(b *testing.B) {
	benchmarkDataHubConsume(b, false)
}

func BenchmarkDataHubConsumeCrossSymbol(b *testing.B) {
	benchmarkDataHubConsume(b, true)
}

func benchmarkDataHubConsume(b *testing.B, crossSymbol bool) {
	for _, width := range []int{8, 32} {
		b.Run(fmt.Sprintf("explicit-%d-fields", width), func(b *testing.B) {
			state := orm.NewSymbolStateWithIdentity("binance", "spot")
			if err := state.SetExSymbols([]*orm.ExSymbol{
				{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
				{ID: 8, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"},
			}); err != nil {
				b.Fatal(err)
			}
			values := map[string]any{"i": int64(9007199254740993), "s": "x", "b": true, "n": nil}
			for j := 4; j < width; j++ {
				values[fmt.Sprintf("f%d", j)] = float64(j)
			}
			for _, method := range []string{"Get", "Data"} {
				b.Run(method, func(b *testing.B) {
					job := &StratJob{Symbol: state.GetSymbolByID(7), DataHub: NewDataHub(), symbols: state}
					sub := &DataSub{Source: "macro", TimeFrame: "1d"}
					if crossSymbol {
						job.Symbol = state.GetSymbolByID(8)
						sub.ExSymbol = state.GetSymbolByID(7)
					}
					evt := &orm.DataSeries{Source: "macro", Sid: 7, TimeFrame: "1d", Values: values}
					read := func() *DataFields { return job.DataHub.Get("1d", "macro", 7) }
					if method == "Data" {
						read = func() *DataFields { return job.Data(sub) }
					}
					job.DataHub.Set(evt)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						evt.TimeMS = int64(i+1) * 86400_000
						evt.EndMS = evt.TimeMS + 86400_000
						job.DataHub.Set(evt)
						if value, ok := read().RawValue("i"); !ok || value != int64(9007199254740993) {
							b.Fatal("raw value lost")
						}
					}
				})
			}
		})
	}
}

func BenchmarkDataAccessGet(b *testing.B) {
	job := &StratJob{Symbol: &orm.ExSymbol{ID: 7}, DataHub: NewDataHub()}
	sub := &DataSub{Source: "macro", TimeFrame: "1d", ExSymbol: job.Symbol}
	job.DataHub.Set(&orm.DataSeries{Source: sub.Source, Sid: 7, TimeFrame: sub.TimeFrame, Values: map[string]any{"v": int64(1)}})
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = job.DataHub.Get(sub.TimeFrame, sub.Source, sub.ExSymbol.ID)
		}
	})
	b.Run("Data", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = job.Data(sub)
		}
	})
}

func BenchmarkDataAccessExplicitRuntime(b *testing.B) {
	state := orm.NewSymbolStateWithIdentity("binance", "spot")
	if err := state.SetExSymbols([]*orm.ExSymbol{{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}}); err != nil {
		b.Fatal(err)
	}
	job := &StratJob{Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}, DataHub: NewDataHub(), symbols: state}
	sub := &DataSub{Source: "macro", TimeFrame: "1d"}
	job.DataHub.Set(&orm.DataSeries{Source: sub.Source, Sid: 7, TimeFrame: sub.TimeFrame, Values: map[string]any{"v": int64(1)}})
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = job.DataHub.Get(sub.TimeFrame, sub.Source, job.Symbol.ID)
		}
	})
	b.Run("Data", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = job.Data(sub)
		}
	})
}

func ExampleNewDataSub() {
	info := orm.NewSeriesInfo("macro", "1d", nil)
	sub := NewDataSub(info)
	sub.Fields = []string{"value"}
	sub.SeriesFields = []string{"value"}
	sub.WarmupNum = 10
	strategy := &TradeStrat{
		OnDataSubs: func(*StratJob) []*DataSub { return []*DataSub{sub} },
		OnData: func(job *StratJob, _ DataEvent) {
			fmt.Println(job.Data(sub).Float64("value"))
		},
	}
	job := &StratJob{Strat: strategy, Symbol: &orm.ExSymbol{ID: 1}, DataHub: NewDataHub()}
	fields := job.SetData(&orm.DataSeries{Source: "macro", Sid: 1, TimeFrame: "1d", Values: map[string]any{"value": 1.0}})
	strategy.OnData(job, DataEvent{DataFields: fields, Role: DataRoleCustom, Symbol: job.Symbol})
	// Output:
	// 1
}
