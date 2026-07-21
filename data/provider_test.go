package data

import (
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banexg/errs"
)

type histFeederBatch struct {
	symbol string
	timeMS int64
}

func (b histFeederBatch) TimeMS() int64 {
	return b.timeMS
}

type stubHistFeeder struct {
	name   string
	symbol string
	times  []int64
	index  int
	runs   *[]histFeederBatch
	onRun  func()
}

func (f *stubHistFeeder) getNextMS() int64 {
	if f.index >= len(f.times) {
		return math.MaxInt64
	}
	return f.times[f.index]
}

func (f *stubHistFeeder) SetSeek(int64) {}

func (f *stubHistFeeder) SetEndMS(int64) {}

func (f *stubHistFeeder) GetBatch() Batch {
	if f.index >= len(f.times) {
		return nil
	}
	name := f.name
	if name == "" {
		name = f.symbol
	}
	return histFeederBatch{symbol: name, timeMS: f.times[f.index]}
}

func (f *stubHistFeeder) RunBatch(batch Batch) *errs.Error {
	*f.runs = append(*f.runs, batch.(histFeederBatch))
	if f.onRun != nil {
		f.onRun()
	}
	return nil
}

func (f *stubHistFeeder) CallNext() {
	f.index++
}

func (f *stubHistFeeder) Type() string {
	return "stub"
}

func (f *stubHistFeeder) getSymbol() string {
	return f.symbol
}

func TestRunHistFeedersRotatesExactTies(t *testing.T) {
	var runs []histFeederBatch
	feeders := []IHistFeeder{
		&stubHistFeeder{name: "first", symbol: "same", times: []int64{100, 100}, runs: &runs},
		&stubHistFeeder{name: "second", symbol: "same", times: []int64{100, 100}, runs: &runs},
	}

	err := RunHistFeeders(func() []IHistFeeder { return feeders }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "first", timeMS: 100},
		{symbol: "second", timeMS: 100},
		{symbol: "first", timeMS: 100},
		{symbol: "second", timeMS: 100},
	}
	if !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected exact-tie order: got %v, want %v", runs, want)
	}
}

func TestRunHistFeedersOrdersByTimeAndSymbol(t *testing.T) {
	var runs []histFeederBatch
	feeders := []IHistFeeder{
		&stubHistFeeder{symbol: "z", times: []int64{100, 300}, runs: &runs},
		&stubHistFeeder{symbol: "a", times: []int64{100, 200}, runs: &runs},
	}

	err := RunHistFeeders(func() []IHistFeeder { return feeders }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "a", timeMS: 100},
		{symbol: "z", timeMS: 100},
		{symbol: "a", timeMS: 200},
		{symbol: "z", timeMS: 300},
	}
	if !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected replay order: got %v, want %v", runs, want)
	}
}

func TestRunHistFeedersRebuildsHeapOnNewVersion(t *testing.T) {
	versions := make(chan int, 1)
	var runs []histFeederBatch
	base := &stubHistFeeder{symbol: "base", times: []int64{100, 300}, runs: &runs}
	added := &stubHistFeeder{symbol: "added", times: []int64{200}, runs: &runs}
	base.onRun = func() {
		base.onRun = nil
		versions <- 1
	}
	makeCalls := 0

	err := RunHistFeeders(func() []IHistFeeder {
		makeCalls++
		if makeCalls == 1 {
			return []IHistFeeder{base}
		}
		return []IHistFeeder{base, added}
	}, versions, nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "base", timeMS: 100},
		{symbol: "added", timeMS: 200},
		{symbol: "base", timeMS: 300},
	}
	if makeCalls != 2 || !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected version reload: calls=%d, got %v, want %v", makeCalls, runs, want)
	}
}

func TestRunHistFeedersHandlesEmptyFeeders(t *testing.T) {
	err := RunHistFeeders(func() []IHistFeeder { return nil }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}
}
