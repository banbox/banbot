package research

import (
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestVisibleTrainingRowsRejectsPublicationBeforeMaturity(t *testing.T) {
	w := TrainingWindow{Start: 100, End: 200, FitAsOf: 220, TestStart: 230, TestEnd: 240, MinSamples: 1}
	row := TrainingSample{SID: 1, DecisionTime: 100, FeaturesAvailableAt: 100, LabelEnd: 110, MatureAt: 120, AvailableAt: 119, Features: []float64{1}, Label: .1}
	if _, err := VisibleTrainingRows([]TrainingSample{row}, w); err == nil {
		t.Fatal("forged publication before maturity admitted to training")
	}
	row.AvailableAt = row.MatureAt
	for _, publication := range []int64{120, 150} {
		row.AvailableAt = publication
		future := row
		future.DecisionTime, future.FeaturesAvailableAt = 200, 200
		future.LabelEnd, future.MatureAt, future.AvailableAt = 210, 221, 225
		future.Features, future.Label = []float64{math.NaN()}, math.NaN()
		outside := TrainingSample{DecisionTime: 250}
		got, err := VisibleTrainingRows([]TrainingSample{future, outside, row}, w)
		if err != nil || !reflect.DeepEqual(got, []TrainingSample{row}) {
			t.Fatalf("mature/delayed row or unseen future filtering changed: %v %v", got, err)
		}
	}
}

func TestReturnLabelPublicationCannotPrecedeEnd(t *testing.T) {
	for _, kind := range []LabelKind{CloseToClose, ExecutableReturn} {
		t.Run(string(kind), func(t *testing.T) {
			spec := LabelSpec{Name: "return", Kind: kind, Horizon: 10, PeriodsPerYear: 365}
			begin := int64(100)
			if kind == ExecutableReturn {
				begin++
			}
			end := begin + spec.Horizon
			if _, err := ReturnLabel(spec, 1, 100, begin, end, end-1, number(10), number(11)); err == nil {
				t.Fatal("return published before observing the horizon end")
			}
			for _, publication := range []int64{end, end + 10} {
				label, err := ReturnLabel(spec, 1, 100, begin, end, publication, number(10), number(11))
				if err != nil || label.MatureAt != end || label.AvailableAt != publication {
					t.Fatalf("valid return publication rejected: %+v %v", label, err)
				}
			}
		})
	}
}

func TestLabelQueueRejectsEarlyPublicationWithoutMutation(t *testing.T) {
	spec := LabelSpec{Name: "forward", Kind: ExecutableReturn, Horizon: 10, PeriodsPerYear: 365}
	q, err := NewLabelQueue([]LabelSpec{spec}, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]factor.Numeric{"feature": number(1)}
	if err := q.Schedule(spec.Name, 1, 100, 101, values); err != nil {
		t.Fatal(err)
	}
	before := q.rows[0]
	before.Factors = map[string]factor.Numeric{"feature": number(1)}
	label := before.Label
	label.Resolved, label.Value, label.MatureAt = true, number(.1), 115
	for _, publication := range []int64{110, 114} {
		label.AvailableAt = publication
		if err := q.Resolve(label); err == nil {
			t.Fatal("forged early resolution admitted")
		}
		unchanged := q.rows[0]
		if !math.IsNaN(unchanged.Label.Value.Value) || unchanged.Label.Value.Validity != factor.Missing {
			t.Fatal("failed resolve changed unresolved value")
		}
		// Missing scheduled values contain NaN, which is unequal to itself.
		unchanged.Label.Value.Value, before.Label.Value.Value = 0, 0
		if q.Len() != 1 || !reflect.DeepEqual(unchanged, before) || q.watermark != 0 {
			t.Fatal("failed resolve changed queued features, label or watermark")
		}
		newRow := LabelObservation{Label: label, Factors: values}
		newRow.Label.SID = 2
		if err := q.Add(newRow); err == nil || q.Len() != 1 {
			t.Fatal("early label publication added to queue")
		}
	}
	label.AvailableAt = 120
	if err := q.Resolve(label); err != nil {
		t.Fatal(err)
	}
	if ready, err := q.Drain(119); err != nil || len(ready) != 0 || q.Len() != 1 {
		t.Fatalf("delayed publication drained early: %v %v", ready, err)
	}
	ready, err := q.Drain(120)
	if err != nil || len(ready) != 1 || ready[0].Label != label || !reflect.DeepEqual(ready[0].Factors, values) || q.Len() != 0 {
		t.Fatalf("valid delayed resolution did not preserve frozen features: %v %v", ready, err)
	}
}
