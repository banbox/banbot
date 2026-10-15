package statistics

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatchReduceOrderBoundAndErrors(t *testing.T) {
	var active, peak atomic.Int32
	out, err := DispatchReduce(context.Background(), 13, 3, []int(nil), func(ctx context.Context, i int) (int, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for p := peak.Load(); n > p; p = peak.Load() {
			if peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(time.Duration(3-i%3) * time.Millisecond)
		return i, nil
	}, func(a []int, b int) ([]int, error) { return append(a, b), nil })
	if err != nil || len(out) != 13 || peak.Load() > 3 || peak.Load() < 2 || active.Load() != 0 {
		t.Fatalf("out=%v error=%v peak=%d", out, err, peak.Load())
	}
	for i, v := range out {
		if i != v {
			t.Fatal(out)
		}
	}
	sentinel := errors.New("mapper failed")
	failed, err := DispatchReduce(context.Background(), 20, 3, 0, func(ctx context.Context, i int) (int, error) {
		if i == 2 {
			return 0, sentinel
		}
		return i, nil
	}, func(a, b int) (int, error) { return a + b, nil })
	if failed != 0 || !errors.Is(err, sentinel) {
		t.Fatalf("partial result %d %v", failed, err)
	}
	failed, err = DispatchReduce(context.Background(), 4, 2, 0, func(ctx context.Context, i int) (int, error) { return i, nil }, func(a, b int) (int, error) { return 123, sentinel })
	if failed != 0 || !errors.Is(err, sentinel) {
		t.Fatalf("reducer partial %d %v", failed, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 2)
	done := make(chan error, 1)
	go func() {
		_, err := DispatchReduce(ctx, 4, 2, 0, func(ctx context.Context, i int) (int, error) {
			started <- struct{}{}
			<-ctx.Done()
			return 0, ctx.Err()
		}, func(a, b int) (int, error) { return a + b, nil })
		done <- err
	}()
	<-started
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers not joined")
	}
}

func TestDispatchReducePreservesOriginatingFailureBehindEarlierCancelledJob(t *testing.T) {
	sentinel := errors.New("underlying source disk failure")
	started := make(chan struct{})
	var active atomic.Int32
	value, err := DispatchReduce(context.Background(), 5, 2, 0, func(ctx context.Context, i int) (int, error) {
		active.Add(1)
		defer active.Add(-1)
		if i == 0 {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}
		<-started
		return 0, sentinel
	}, func(a, b int) (int, error) { return a + b, nil })
	if value != 0 || !errors.Is(err, sentinel) || active.Load() != 0 {
		t.Fatal("mapper cause masked by indexed cancellation", value, err, active.Load())
	}
}
func TestStreamingCovarianceAndPCA(t *testing.T) {
	rows := [][]float64{{1, 4, 3}, {2, 1, 5}, {4, 3, 2}, {7, 6, 9}, {3, 8, 7}, {9, 2, 4}}
	var all, left, right CovarianceStream
	if err := all.Add(rows); err != nil {
		t.Fatal(err)
	}
	left.Add(rows[:2])
	right.Add(rows[2:])
	if err := left.Merge(&right); err != nil {
		t.Fatal(err)
	}
	mean, cov, err := left.Covariance()
	if err != nil {
		t.Fatal(err)
	}
	am, ac, _ := all.Covariance()
	for i := range mean {
		if math.Abs(mean[i]-am[i]) > 1e-12 {
			t.Fatal(mean, am)
		}
		for j := range mean {
			expected := 0.
			for _, row := range rows {
				expected += (row[i] - am[i]) * (row[j] - am[j]) / float64(len(rows)-1)
			}
			if math.Abs(cov[i][j]-expected) > 1e-12 || math.Abs(cov[i][j]-ac[i][j]) > 1e-12 {
				t.Fatal(cov, ac)
			}
		}
	}
	before := left.Count()
	if err := left.Add([][]float64{{1, 2, 3}, {math.NaN(), 1, 2}}); err == nil || left.Count() != before {
		t.Fatal("non-atomic invalid chunk")
	}
	mean[0] = 999
	cov[0][0] = 999
	owned, _, _ := left.Covariance()
	if owned[0] == 999 {
		t.Fatal("aliased snapshot")
	}
	for _, standardize := range []bool{false, true} {
		index := 0
		stream, err := FitStreamingPCA(context.Background(), func(context.Context) ([][]float64, error) {
			if index >= len(rows) {
				return nil, io.EOF
			}
			end := min(index+2, len(rows))
			chunk := rows[index:end]
			index = end
			return chunk, nil
		}, 3, standardize)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := FitPCA(rows, 3, standardize)
		if err != nil {
			t.Fatal(err)
		}
		for i, v := range batch.Eigenvalues {
			if math.Abs(v-stream.Eigenvalues[i]) > 1e-10 {
				t.Fatal(stream, batch)
			}
		}
		blocks := [][]int{{2}, {0, 1}}
		scores, err := ProjectPCAColumnBlocks(context.Background(), stream, blocks, 2, func(ctx context.Context, cols []int) ([][]float64, error) {
			result := make([][]float64, len(rows))
			for i, row := range rows {
				for _, j := range cols {
					result[i] = append(result[i], row[j])
				}
			}
			cols[0] = 99
			return result, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(blocks, [][]int{{2}, {0, 1}}) {
			t.Fatal("loader mutated caller columns")
		}
		for i, row := range rows {
			want, _ := batch.Transform(row)
			for j, v := range want {
				if math.Abs(scores[i][j]-v) > 1e-10 {
					t.Fatalf("projection%d: %v versus %v", i, scores[i], want)
				}
			}
			residual, _ := stream.Residual(row)
			for _, v := range residual {
				if math.Abs(v) > 1e-10 {
					t.Fatal("full rank reconstruction", residual)
				}
			}
		}
	}
	_, err = FitStreamingPCA(context.Background(), func(context.Context) ([][]float64, error) { return rows, io.EOF }, 2, false)
	if err == nil {
		t.Fatal("ambiguous EOF accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = FitStreamingPCA(ctx, func(context.Context) ([][]float64, error) { t.Fatal("called canceled loader"); return nil, nil }, 2, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	model, _ := FitPCA(rows, 2, false)
	for _, blocks := range [][][]int{{{0}, {1}}, {{0, 1}, {1, 2}}} {
		_, err := ProjectPCAColumnBlocks(context.Background(), model, blocks, 2, func(context.Context, []int) ([][]float64, error) {
			t.Fatal("invalid partition loaded")
			return nil, nil
		})
		if err == nil {
			t.Fatal("invalid partition accepted")
		}
	}
	_, err = ProjectPCAColumnBlocks(context.Background(), model, [][]int{{0}, {1, 2}}, 2, func(ctx context.Context, cols []int) ([][]float64, error) {
		if cols[0] == 0 {
			return [][]float64{{1}}, nil
		}
		return [][]float64{{1, 2}, {3, 4}}, nil
	})
	if err == nil {
		t.Fatal("misaligned rows accepted")
	}
}
func TestPurgedSearchAutoF1(t *testing.T) {
	spans := make([]EventSpan, 12)
	labels := make([]int, 12)
	weights := make([]float64, 12)
	for i := range spans {
		spans[i] = EventSpan{Start: i * 2, End: i * 2}
		weights[i] = 1
		if i%4 == 0 {
			labels[i] = 1
		}
	}
	candidates := []map[string]float64{{"predict": 0}, {"predict": 1}}
	fit := func(params map[string]float64, train, test []int, w []float64, classes []int) ([][]float64, error) {
		out := make([][]float64, len(test))
		for i := range out {
			if params["predict"] == 0 {
				out[i] = []float64{.9, .1}
			} else {
				out[i] = []float64{.1, .9}
			}
		}
		return out, nil
	}
	result, err := PurgedParameterSearch(candidates, 2, spans, labels, []int{0, 1}, weights, 3, 0, "auto", fit)
	if err != nil || result.Best != 1 {
		t.Fatalf("%+v %v", result, err)
	}
	for _, v := range result.Trials[0].FoldScores {
		if v != 0 {
			t.Fatal("all negative F1", v)
		}
	}
	for _, v := range result.Trials[1].FoldScores {
		if math.Abs(v-.4) > 1e-12 {
			t.Fatal("source F1", v)
		}
	}
	labels[0] = -1
	result, err = PurgedParameterSearch(candidates[:1], 1, spans, labels, []int{-1, 0, 1}, weights, 3, 0, "auto", func(p map[string]float64, tr, te []int, w []float64, c []int) ([][]float64, error) {
		out := make([][]float64, len(te))
		for i := range out {
			out[i] = []float64{1. / 3, 1. / 3, 1. / 3}
		}
		return out, nil
	})
	if err != nil || math.Abs(result.Trials[0].MeanScore+math.Log(3)) > 1e-12 {
		t.Fatal(result, err)
	}
}

func TestStreamingOwnsReusedBuffers(t *testing.T) {
	raw := [][]float64{{1, 2}, {3, 5}, {5, 4}, {7, 9}}
	buffer := [][]float64{{0, 0}, {0, 0}}
	cursor := 0
	model, err := FitStreamingPCA(context.Background(), func(context.Context) ([][]float64, error) {
		if cursor == len(raw) {
			for _, row := range buffer {
				for j := range row {
					row[j] = 999
				}
			}
			return nil, io.EOF
		}
		for i := range buffer {
			copy(buffer[i], raw[cursor+i])
		}
		cursor += 2
		return buffer, nil
	}, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	batch, _ := FitPCA(raw, 2, false)
	for i, v := range batch.Eigenvalues {
		if math.Abs(v-model.Eigenvalues[i]) > 1e-11 {
			t.Fatal(model, batch)
		}
	}
	originalMean := append([]float64(nil), model.Mean...)
	blocks := [][]int{{0}, {1}}
	got, err := ProjectPCAColumnBlocks(context.Background(), model, blocks, 1, func(ctx context.Context, cols []int) ([][]float64, error) {
		model.Mean[0] = 1000 // callback cannot corrupt the projection's frozen model.
		out := make([][]float64, len(raw))
		for i, row := range raw {
			out[i] = []float64{row[cols[0]]}
		}
		return out, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	model.Mean = originalMean
	for i, row := range raw {
		want, _ := model.Transform(row)
		for j, v := range want {
			if math.Abs(v-got[i][j]) > 1e-10 {
				t.Fatal(got, want)
			}
		}
	}
	var huge CovarianceStream
	if err := huge.Add([][]float64{{1e308}}); err != nil {
		t.Fatal("finite one-row mean", err)
	}
	var other CovarianceStream
	other.Add(raw)
	huge = CovarianceStream{}
	huge.Merge(&other)
	other.Add(raw)
	if huge.Count() != 4 {
		t.Fatal("merge aliases source")
	}
}
