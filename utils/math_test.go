package utils

import (
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/shopspring/decimal"
)

func TestDecSortinoRatio(t *testing.T) {
	t.Parallel()
	rfr := decimal.NewFromFloat(0.001)
	figures := []decimal.Decimal{
		decimal.NewFromFloat(0.10),
		decimal.NewFromFloat(0.04),
		decimal.NewFromFloat(0.15),
		decimal.NewFromFloat(-0.05),
		decimal.NewFromFloat(0.20),
		decimal.NewFromFloat(-0.02),
		decimal.NewFromFloat(0.08),
		decimal.NewFromFloat(-0.06),
		decimal.NewFromFloat(0.13),
		decimal.NewFromFloat(0.23),
	}
	var err error
	_, err = DecSortinoRatio(nil, rfr)
	if !errors.Is(err, errZeroValue) {
		t.Errorf("expected: %v, received %v", errZeroValue, err)
	}

	var r decimal.Decimal
	r, err = DecSortinoRatio(figures, rfr)
	if err != nil {
		t.Fatal(err)
	}
	if rounded := r.Round(2); !rounded.Equal(decimal.NewFromFloat(49.81)) {
		t.Errorf("expected annualized Sortino ratio 49.81, received %v", rounded)
	}

	// this follows and matches the example calculation from
	// https://www.wallstreetmojo.com/sortino-ratio/
	example := []decimal.Decimal{
		decimal.NewFromFloat(0.1),
		decimal.NewFromFloat(0.12),
		decimal.NewFromFloat(0.07),
		decimal.NewFromFloat(-0.03),
		decimal.NewFromFloat(0.08),
		decimal.NewFromFloat(-0.04),
		decimal.NewFromFloat(0.15),
		decimal.NewFromFloat(0.2),
		decimal.NewFromFloat(0.12),
		decimal.NewFromFloat(0.06),
		decimal.NewFromFloat(-0.03),
		decimal.NewFromFloat(0.02),
	}
	r, err = DecSortinoRatio(example, decimal.NewFromFloat(0.06))
	if err != nil {
		t.Error(err)
	}
	if rounded := r.Round(1); !rounded.Equal(decimal.NewFromFloat(63.8)) {
		t.Errorf("expected annualized Sortino ratio 63.8, received %v", rounded)
	}
}

func TestSortinoRatioCSV(t *testing.T) {
	returns := readReturnsCSV(t, []float64{0.1, 0.12, 0.07, -0.03, 0.08, -0.04, 0.15, 0.2, 0.12, 0.06, -0.03, 0.02})
	riskFree := decimal.NewFromFloat(0.06)
	result, err := DecSortinoRatio(returns, riskFree)
	if err != nil {
		t.Fatal(err)
	}
	if rounded := result.Round(1); !rounded.Equal(decimal.NewFromFloat(63.8)) {
		t.Fatalf("expected annualized Sortino ratio 63.8, got %s", rounded)
	}
}

func TestDecSharpeRatio(t *testing.T) {
	t.Parallel()
	result, err := DecSharpeRatio(nil, decimal.Zero)
	if !errors.Is(err, errZeroValue) {
		t.Error(err)
	}
	if !result.IsZero() {
		t.Error("expected 0")
	}

	result, err = DecSharpeRatio([]decimal.Decimal{decimal.NewFromFloat(0.026)}, decimal.NewFromFloat(0.017))
	if err != nil {
		t.Error(err)
	}
	if !result.IsZero() {
		t.Error("expected 0")
	}

	// this follows and matches the example calculation (without rounding) from
	// https://www.educba.com/sharpe-ratio-formula/
	returns := []decimal.Decimal{
		decimal.NewFromFloat(-0.0005),
		decimal.NewFromFloat(-0.0065),
		decimal.NewFromFloat(-0.0113),
		decimal.NewFromFloat(0.0031),
		decimal.NewFromFloat(-0.0112),
		decimal.NewFromFloat(0.0056),
		decimal.NewFromFloat(0.0156),
		decimal.NewFromFloat(0.0048),
		decimal.NewFromFloat(0.0012),
		decimal.NewFromFloat(0.0038),
		decimal.NewFromFloat(-0.0008),
		decimal.NewFromFloat(0.0032),
		decimal.Zero,
		decimal.NewFromFloat(-0.0128),
		decimal.NewFromFloat(-0.0058),
		decimal.NewFromFloat(0.003),
		decimal.NewFromFloat(0.0042),
		decimal.NewFromFloat(0.0055),
		decimal.NewFromFloat(0.0009),
	}
	result, err = DecSharpeRatio(returns, decimal.NewFromFloat(-0.0017))
	if err != nil {
		t.Error(err)
	}
	result = result.Round(2)
	if !result.Equal(decimal.NewFromFloat(0.26)) {
		t.Errorf("expected 0.26, received %v", result)
	}
}

func readReturnsCSV(t *testing.T, values []float64) []decimal.Decimal {
	// stock:META 2023-09-01:2024-09-01 daily return, length: 251
	/*
		import quantstats as qs
		stock = qs.utils.download_returns('META')
		df = stock.loc['2023-09-01':'2024-09-01']
		df.to_csv(path)
	*/
	t.Helper()
	path := filepath.Join(t.TempDir(), "returns.csv")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	if err = writer.Write([]string{"date", "return"}); err != nil {
		t.Fatal(err)
	}
	for i, value := range values {
		if err = writer.Write([]string{strconv.Itoa(i), strconv.FormatFloat(value, 'f', -1, 64)}); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	records, readErr := ReadCSV(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(records) <= 1 {
		t.Fatalf("data.csv is empty")
	}

	var returns []decimal.Decimal
	for _, record := range records[1:] {
		val, err_ := decimal.NewFromString(record[1])
		if err_ != nil {
			t.Fatal(err_)
		}
		returns = append(returns, val)
	}
	return returns
}

func TestSharpeRatioCSV(t *testing.T) {
	returns := readReturnsCSV(t, []float64{
		-0.0005, -0.0065, -0.0113, 0.0031, -0.0112, 0.0056, 0.0156,
		0.0048, 0.0012, 0.0038, -0.0008, 0.0032, 0, -0.0128, -0.0058,
		0.003, 0.0042, 0.0055, 0.0009,
	})
	riskFree := decimal.NewFromFloat(-0.0017)
	result, err := DecSharpeRatio(returns, riskFree)
	if err != nil {
		t.Fatal(err)
	}
	if rounded := result.Round(2); !rounded.Equal(decimal.NewFromFloat(0.26)) {
		t.Fatalf("expected rounded Sharpe ratio 0.26, got %s", rounded)
	}
}
