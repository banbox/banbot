package utils

import (
	"errors"
	"github.com/shopspring/decimal"
	"testing"
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
	avg, err := DecArithMean(figures)
	if err != nil {
		t.Error(err)
	}
	_, err = DecSortinoRatio(nil, rfr, avg)
	if !errors.Is(err, errZeroValue) {
		t.Errorf("expected: %v, received %v", errZeroValue, err)
	}

	var r decimal.Decimal
	r, err = DecSortinoRatio(figures, rfr, avg)
	if err != nil {
		t.Error(err)
	}
	rf, exact := r.Float64()
	if !exact && rf != 3.0377875479459906 {
		t.Errorf("expected 3.0377875479459906, received %v", r)
	} else if rf != 3.0377875479459907 {
		t.Errorf("expected 3.0377875479459907, received %v", r)
	}

	avg, err = DecFinaGeomMean(figures)
	if err != nil {
		t.Error(err)
	}

	r, err = DecSortinoRatio(figures, rfr, avg)
	if err != nil {
		t.Error(err)
	}
	if !r.Equal(decimal.NewFromFloat(2.8712802265603243)) {
		t.Errorf("expected 2.525203164136098, received %v", r)
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
	avg, err = DecArithMean(example)
	if err != nil {
		t.Error(err)
	}
	r, err = DecSortinoRatio(example, decimal.NewFromFloat(0.06), avg)
	if err != nil {
		t.Error(err)
	}
	if rr := r.Round(1); !rr.Equal(decimal.NewFromFloat(0.2)) {
		t.Errorf("expected 0.2, received %v", rr)
	}
}

func TestDecSharpeRatio(t *testing.T) {
	t.Parallel()
	result, err := DecSharpeRatio(nil, decimal.Zero, decimal.Zero)
	if !errors.Is(err, errZeroValue) {
		t.Error(err)
	}
	if !result.IsZero() {
		t.Error("expected 0")
	}

	result, err = DecSharpeRatio([]decimal.Decimal{decimal.NewFromFloat(0.026)}, decimal.NewFromFloat(0.017), decimal.NewFromFloat(0.026))
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
	var avg decimal.Decimal
	avg, err = DecArithMean(returns)
	if err != nil {
		t.Error(err)
	}
	result, err = DecSharpeRatio(returns, decimal.NewFromFloat(-0.0017), avg)
	if err != nil {
		t.Error(err)
	}
	result = result.Round(2)
	if !result.Equal(decimal.NewFromFloat(0.26)) {
		t.Errorf("expected 0.26, received %v", result)
	}
}