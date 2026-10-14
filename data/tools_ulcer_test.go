package data

import (
	"math"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/utils"
	"go.uber.org/zap"
)

func TestFilePerformanceUlcerMetrics(t *testing.T) {
	for _, kind := range []string{"price", "ratio"} {
		t.Run(kind, func(t *testing.T) {
			rows := [][]string{{"date", "test"}, {"2024-01-01", "100"}, {"2024-01-02", "90"}, {"2024-01-03", "95"}, {"2024-01-04", "105"}}
			if kind == "ratio" {
				rows = [][]string{{"date", "test"}, {"2024-01-02", "-0.1"}, {"2024-01-03", "0.05555555555555555"}, {"2024-01-04", "0.10526315789473684"}}
			}
			dir := t.TempDir()
			input := filepath.Join(dir, "input.csv")
			output := filepath.Join(dir, "output.csv")
			if err := utils.WriteCsvFile(input, rows, false); err != nil {
				t.Fatal(err)
			}
			args := &config.CmdArgs{InPath: input, OutPath: output, InType: kind}
			if err := CalcFilePerfsWithLogger(args, zap.NewNop()); err != nil {
				t.Fatal(err)
			}
			out, err := utils.ReadCSV(output)
			if err != nil {
				t.Fatal(err)
			}
			values := map[string]float64{}
			for _, row := range out {
				if row[0] == "ulcerIndex" || row[0] == "upi" {
					values[row[0]], _ = strconv.ParseFloat(row[1], 64)
				}
			}
			var ui, upi *float64
			if kind == "price" {
				ui, upi, _ = utils.CalcUlcerPerformance([]float64{100, 90, 95, 105}, 3/utils.UlcerYearDays, 0)
			} else {
				ui, upi, _ = utils.CalcUlcerPerformanceByReturns([]float64{-.1, 95.0/90 - 1, 105.0/95 - 1}, 3/utils.UlcerYearDays, 0)
			}
			if math.Abs(values["ulcerIndex"]-*ui) > 1e-10 || math.Abs(values["upi"]-*upi) > 1e-8 {
				t.Fatalf("metrics=%v expected UI=%v UPI=%v", values, *ui, *upi)
			}
		})
	}
}

func TestFilePerformanceAnnualization(t *testing.T) {
	rows := [][]string{{"date"}, {"2024-01-01"}, {"2024-01-03"}, {"2024-01-04"}}
	if got := filePerfYears(rows, &config.CmdArgs{}, false); math.Abs(got-3/utils.UlcerYearDays) > 1e-12 {
		t.Fatalf("years=%v", got)
	}
	if got := filePerfYears(rows, &config.CmdArgs{}, true); got != 0 {
		t.Fatal("irregular returns cannot infer baseline interval")
	}
	unknown := [][]string{{"index"}, {"a"}, {"b"}, {"c"}}
	if got := filePerfYears(unknown, &config.CmdArgs{}, false); got != 0 {
		t.Fatal("unknown time labels assumed annualization")
	}
	if got := filePerfYears(unknown, &config.CmdArgs{TimeFrames: []string{"1d"}}, true); math.Abs(got-3/utils.UlcerYearDays) > 1e-12 {
		t.Fatalf("explicit timeframe years=%v", got)
	}
}
