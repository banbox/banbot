package statistics

import "fmt"

// ARIMA supports integration orders0/1 using the existing conditional Gaussian
// ARMA likelihood. Integration does not introduce an independent drift term:
// the ARMA intercept is the intercept of the differenced process.
type ARIMA struct {
	ARMA        ARMA
	Integration int
}

func differenceOnce(values []float64) ([]float64, error) {
	if len(values) < 2 {
		return nil, fmt.Errorf("statistics: integrated series needs two levels")
	}
	out := make([]float64, len(values)-1)
	for i := range values {
		if !finite(values[i]) {
			return nil, fmt.Errorf("statistics: nonfinite integrated series")
		}
		if i > 0 {
			out[i-1] = values[i] - values[i-1]
			if !finite(out[i-1]) {
				return nil, fmt.Errorf("statistics: integrated difference overflow")
			}
		}
	}
	return out, nil
}

func FitARIMA(values []float64, integration int, spec ARMAFitSpec) (ARIMA, error) {
	if integration != 0 && integration != 1 {
		return ARIMA{}, fmt.Errorf("statistics: integration order must be0/1")
	}
	var err error
	if integration == 1 {
		values, err = differenceOnce(values)
		if err != nil {
			return ARIMA{}, err
		}
	}
	model, err := FitARMA(values, spec)
	return ARIMA{model, integration}, err
}

type ARIMAFilter struct {
	filter      *ARMAFilter
	lastLevel   float64
	integration int
}

func (m ARIMA) Filter(values []float64) (*ARIMAFilter, error) {
	if m.Integration != 0 && m.Integration != 1 {
		return nil, fmt.Errorf("statistics: invalid ARIMA integration")
	}
	last := 0.0
	var err error
	if m.Integration == 1 {
		if len(values) == 0 {
			return nil, fmt.Errorf("statistics: empty integrated calibration")
		}
		last = values[len(values)-1]
		values, err = differenceOnce(values)
		if err != nil {
			return nil, err
		}
	}
	filter, err := m.ARMA.Filter(values)
	if err != nil {
		return nil, err
	}
	return &ARIMAFilter{filter, last, m.Integration}, nil
}

func (f *ARIMAFilter) Observe(level float64) error {
	if f == nil || f.filter == nil || !finite(level) {
		return fmt.Errorf("statistics: invalid ARIMA observation/filter")
	}
	value := level
	if f.integration == 1 {
		value -= f.lastLevel
	}
	if err := f.filter.Observe(value); err != nil {
		return err
	}
	f.lastLevel = level
	return nil
}

func (f *ARIMAFilter) Forecast() (float64, error) {
	if f == nil || f.filter == nil {
		return 0, fmt.Errorf("statistics: invalid ARIMA filter")
	}
	prediction, err := f.filter.Forecast()
	if err != nil {
		return 0, err
	}
	if f.integration == 1 {
		prediction += f.lastLevel
	}
	if !finite(prediction) {
		return 0, fmt.Errorf("statistics: integrated forecast overflow")
	}
	return prediction, nil
}
