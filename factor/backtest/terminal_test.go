package backtest

import (
	"github.com/banbox/banbot/factor"
	"github.com/shopspring/decimal"
	"testing"
)

func TestTerminalSettlementSignedCashZeroAndNoResurrection(t *testing.T) {
	for _, qty := range []string{"2", "-2"} {
		for _, value := range []int64{0, 60} {
			t.Run(qty+decimal.NewFromInt(value).String(), func(t *testing.T) {
				b, _ := NewBook(1000)
				p := allocationPortfolio(t, 1, factor.Full, 1000, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: qty}, 2: {Basis: factor.AbsoluteQuantity, Value: "1"}})
				quotes := map[int32]Quote{1: {Price: 100, AtMS: 11, AvailableAt: 11}, 2: {Price: 100, AtMS: 11, AvailableAt: 11}}
				if err := b.ExecuteAllocation(p, quotes, 11, .001, 0); err != nil {
					t.Fatal(err)
				}
				before := b.State()
				e := TerminalSettlement{Source: "terminal", SourceVersion: "v1", SID: 1, CashValue: decimal.NewFromInt(value), EventMS: 20, PublishedMS: 21, ReceivedMS: 25}
				if _, err := b.ApplyTerminalSettlement(e, 24); err == nil {
					t.Fatal("future receipt accepted")
				}
				if ok, err := b.ApplyTerminalSettlement(e, 25); err != nil || !ok {
					t.Fatal(ok, err)
				}
				want := before.Cash + before.Quantities[1]*float64(value)
				after := b.State()
				if after.Cash != want || after.NAV != want+100 || after.Quantities[1] != 0 || after.Quantities[2] != 1 || after.Fees != before.Fees || after.Turnover != before.Turnover {
					t.Fatal(after, before, want)
				}
				e.ReceivedMS++
				if ok, err := b.ApplyTerminalSettlement(e, 27); err != nil || ok {
					t.Fatal("retry", ok, err)
				}
				e.CashValue = e.CashValue.Add(decimal.NewFromInt(1))
				if _, err := b.ApplyTerminalSettlement(e, 27); err == nil {
					t.Fatal("conflict accepted")
				}
				flat := allocationPortfolio(t, 2, factor.Full, after.NAV, map[int32]factor.Allocation{2: {Basis: factor.AbsoluteQuantity, Value: "1"}})
				if err := b.ExecuteAllocation(flat, map[int32]Quote{2: {Price: 100, AtMS: 12, AvailableAt: 12}}, 27, 0, 0); err != nil {
					t.Fatal("settled Full omission required quote", err)
				}
				rebuy := allocationPortfolio(t, 3, factor.Patch, after.NAV, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "1"}})
				if err := b.ExecuteAllocation(rebuy, quotes, 27, 0, 0); err == nil {
					t.Fatal("terminal resurrected")
				}
				if err := b.Mark(1, Quote{Price: 0, AtMS: 27, AvailableAt: 27}, 27); err == nil {
					t.Fatal("zero quote admitted")
				}
			})
		}
	}
}
