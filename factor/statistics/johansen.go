package statistics

import (
	"errors"
	"math"

	"github.com/banbox/banbot/factor"
	"gonum.org/v1/gonum/mat"
)

type JohansenSpec struct {
	DifferenceLags   int
	Intercept, Trend bool // unrestricted nuisance terms in both regressions
}

type JohansenResult struct {
	Eigenvalues, Trace, MaximumEigenvalue   []float64
	CointegratingVectors, AdjustmentVectors [][]float64
	Samples, NuisanceRank                   int
}

// Johansen computes the Gaussian reduced-rank VEC canonical correlations.
// Delta levels and lagged levels are residualized against past differences and
// explicitly selected deterministic terms. Vectors satisfy beta'Skk beta=I.
// Asymptotic Osterwald-Lenum / MacKinnon-Haug-Michelis critical values for the
// matching deterministic specification are available via JohansenTraceCriticalValues,
// JohansenMaxEigCriticalValues, CointegrationRank, and IsCointegrated. This
// operator does not select a trading policy.
func Johansen(levels [][]float64, spec JohansenSpec) (JohansenResult, error) {
	if len(levels) == 0 || len(levels[0]) < 2 || len(levels[0]) > 64 || spec.DifferenceLags < 0 || spec.DifferenceLags > 128 || spec.Trend && !spec.Intercept {
		return JohansenResult{}, errors.New("statistics: invalid Johansen dimensions/specification")
	}
	d, lag := len(levels[0]), spec.DifferenceLags
	n := len(levels) - lag - 1
	nuisanceCount := d * lag
	if spec.Intercept {
		nuisanceCount++
	}
	if spec.Trend {
		nuisanceCount++
	}
	if n <= nuisanceCount+d {
		return JohansenResult{}, errors.New("statistics: insufficient Johansen observations")
	}
	for _, row := range levels {
		if len(row) != d {
			return JohansenResult{}, errors.New("statistics: ragged Johansen levels")
		}
		for _, v := range row {
			if !finite(v) {
				return JohansenResult{}, errors.New("statistics: nonfinite Johansen levels")
			}
		}
	}
	z := make([][]float64, n)
	r0, rk := make([][]float64, n), make([][]float64, n)
	for i := range n {
		t := lag + 1 + i
		z[i] = make([]float64, 0, nuisanceCount)
		if spec.Intercept {
			z[i] = append(z[i], 1)
		}
		if spec.Trend {
			z[i] = append(z[i], float64(t))
		}
		for l := 1; l <= lag; l++ {
			for asset := range d {
				z[i] = append(z[i], levels[t-l][asset]-levels[t-l-1][asset])
			}
		}
		r0[i], rk[i] = make([]float64, d), make([]float64, d)
		for asset := range d {
			r0[i][asset] = levels[t][asset] - levels[t-1][asset]
			rk[i][asset] = levels[t-1][asset]
		}
	}
	rank := 0
	for _, matrix := range [][][]float64{r0, rk} {
		for asset := range d {
			y := make([]float64, n)
			for i := range n {
				y[i] = matrix[i][asset]
			}
			if nuisanceCount > 0 {
				coefficients, r, err := factor.LeastSquares(z, y, nil)
				if err != nil {
					return JohansenResult{}, err
				}
				rank = r
				for i := range n {
					for j, c := range coefficients {
						matrix[i][asset] -= z[i][j] * c
					}
				}
			}
		}
	}
	s00, skk := mat.NewSymDense(d, nil), mat.NewSymDense(d, nil)
	s0k := mat.NewDense(d, d, nil)
	for a := range d {
		for b := range d {
			v00, vkk, v0k := 0.0, 0.0, 0.0
			for i := range n {
				v00 += r0[i][a] * r0[i][b] / float64(n)
				vkk += rk[i][a] * rk[i][b] / float64(n)
				v0k += r0[i][a] * rk[i][b] / float64(n)
			}
			if !finite(v00) || !finite(vkk) || !finite(v0k) {
				return JohansenResult{}, errors.New("statistics: Johansen covariance overflow")
			}
			if b <= a {
				s00.SetSym(a, b, v00)
				skk.SetSym(a, b, vkk)
			}
			s0k.Set(a, b, v0k)
		}
	}
	root, err := covarianceInverseRoot(skk)
	if err != nil {
		return JohansenResult{}, err
	}
	var chol mat.Cholesky
	if !chol.Factorize(s00) {
		return JohansenResult{}, errors.New("statistics: singular Johansen difference covariance")
	}
	var solve, canonical, intermediate mat.Dense
	if err := chol.SolveTo(&solve, s0k); err != nil {
		return JohansenResult{}, err
	}
	intermediate.Mul(s0k.T(), &solve)
	canonical.Mul(root, &intermediate)
	intermediate.Mul(&canonical, root)
	sym := mat.NewSymDense(d, nil)
	for a := range d {
		for b := 0; b <= a; b++ {
			v := .5*intermediate.At(a, b) + .5*intermediate.At(b, a)
			if !finite(v) {
				return JohansenResult{}, errors.New("statistics: Johansen canonical covariance overflow")
			}
			sym.SetSym(a, b, v)
		}
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(sym, true) {
		return JohansenResult{}, errors.New("statistics: Johansen eigendecomposition failed")
	}
	values := eigen.Values(nil)
	var vectors mat.Dense
	eigen.VectorsTo(&vectors)
	out := JohansenResult{Samples: n, NuisanceRank: rank, Eigenvalues: make([]float64, d), Trace: make([]float64, d), MaximumEigenvalue: make([]float64, d), CointegratingVectors: make([][]float64, d), AdjustmentVectors: make([][]float64, d)}
	for component := range d {
		column := d - 1 - component
		value := values[column]
		if value < -1e-10 || value >= 1 {
			return JohansenResult{}, errors.New("statistics: degenerate Johansen canonical correlation")
		}
		out.Eigenvalues[component] = max(0, value)
		out.MaximumEigenvalue[component] = -float64(n) * math.Log1p(-out.Eigenvalues[component])
		beta := make([]float64, d)
		for a := range d {
			for b := range d {
				beta[a] += root.At(a, b) * vectors.At(b, column)
			}
		}
		out.CointegratingVectors[component] = beta
	}
	canonicalLoadingSignsTranspose(out.CointegratingVectors)
	for component, beta := range out.CointegratingVectors {
		out.AdjustmentVectors[component] = make([]float64, d)
		for a := range d {
			for b := range d {
				out.AdjustmentVectors[component][a] += s0k.At(a, b) * beta[b]
			}
		}
	}
	for rank := d - 1; rank >= 0; rank-- {
		out.Trace[rank] = out.MaximumEigenvalue[rank]
		if rank+1 < d {
			out.Trace[rank] += out.Trace[rank+1]
		}
	}
	return out, nil
}

func covarianceInverseRoot(cov *mat.SymDense) (*mat.SymDense, error) {
	n := cov.SymmetricDim()
	var eigen mat.EigenSym
	if !eigen.Factorize(cov, true) {
		return nil, errors.New("statistics: covariance inverse-root decomposition failed")
	}
	values := eigen.Values(nil)
	var vectors mat.Dense
	eigen.VectorsTo(&vectors)
	root := mat.NewSymDense(n, nil)
	for component, v := range values {
		if v <= 1e-12*values[n-1] || !finite(v) {
			return nil, errors.New("statistics: singular Johansen level covariance")
		}
		for a := range n {
			for b := 0; b <= a; b++ {
				root.SetSym(a, b, root.At(a, b)+vectors.At(a, component)*vectors.At(b, component)/math.Sqrt(v))
			}
		}
	}
	return root, nil
}

func canonicalLoadingSignsTranspose(vectors [][]float64) {
	for _, vector := range vectors {
		pivot := 0
		for i, v := range vector {
			if math.Abs(v) > math.Abs(vector[pivot]) {
				pivot = i
			}
		}
		if vector[pivot] < 0 {
			for i := range vector {
				vector[i] = -vector[i]
			}
		}
	}
}

// TraceRank sequentially rejects the supplied rank-r critical values. A rejected
// rank0 alone does not establish full rank or choose a hedge vector automatically.
func (j JohansenResult) TraceRank(criticalValues []float64) (int, error) {
	if len(j.Trace) == 0 || len(criticalValues) != len(j.Trace) {
		return 0, errors.New("statistics: Johansen rank thresholds must match dimension")
	}
	for i, v := range criticalValues {
		if !finite(v) || v <= 0 || !finite(j.Trace[i]) || j.Trace[i] < 0 {
			return 0, errors.New("statistics: invalid Johansen rank threshold/statistic")
		}
	}
	rank := 0
	for i, v := range criticalValues {
		if j.Trace[i] <= v {
			break
		}
		rank++
	}
	return rank, nil
}

// Asymptotic Johansen Trace and Maximum-Eigenvalue critical values [90%, 95%, 99%]
// indexed by m-1 where m = d - r (m = 1..12) is the number of stochastic trends
// under H0: rank <= r (Osterwald-Lenum 1992 / MacKinnon-Haug-Michelis 1999).
var (
	// Case 1: Intercept=false, Trend=false (no constant, no trend in VECM).
	johansenTraceNoDet = [12][3]float64{
		{2.9762, 4.1296, 6.9406},
		{10.4741, 12.3212, 16.3640},
		{21.7781, 24.2761, 29.5147},
		{37.0339, 40.1749, 46.5716},
		{56.2839, 60.0627, 67.6367},
		{79.5329, 83.9383, 92.7136},
		{106.7351, 111.7797, 121.7375},
		{137.9954, 143.6691, 154.7977},
		{173.2292, 179.5199, 191.8122},
		{212.4721, 219.4051, 232.8291},
		{255.6732, 263.2603, 277.9962},
		{302.9054, 311.1288, 326.9716},
	}
	johansenMaxEigNoDet = [12][3]float64{
		{2.9762, 4.1296, 6.9406},
		{9.4748, 11.2246, 15.0923},
		{15.7175, 17.7961, 22.2519},
		{21.8370, 24.1592, 29.0609},
		{27.9160, 30.4428, 35.7359},
		{33.9271, 36.6301, 42.2333},
		{39.9085, 42.7679, 48.6606},
		{45.8930, 48.8795, 55.0335},
		{51.8528, 54.9629, 61.3449},
		{57.7954, 61.0404, 67.6415},
		{63.7248, 67.0756, 73.8856},
		{69.6513, 73.0946, 80.0937},
	}

	// Case 3: Intercept=true, Trend=false (unrestricted constant in VECM, Osterwald-Lenum Table 1 / MHM Case 3).
	johansenTraceIntercept = [12][3]float64{
		{2.7055, 3.8415, 6.6349},
		{13.4294, 15.4943, 19.9349},
		{27.0669, 29.7961, 35.4628},
		{44.4929, 47.8545, 54.6815},
		{65.8202, 69.8189, 77.8202},
		{91.1090, 95.7542, 104.9637},
		{120.3673, 125.6185, 135.9825},
		{153.6341, 159.5290, 171.0905},
		{190.8714, 197.3772, 210.0366},
		{232.1030, 239.2468, 253.2526},
		{277.3740, 285.1402, 300.2821},
		{326.5354, 334.9795, 351.2150},
	}
	johansenMaxEigIntercept = [12][3]float64{
		{2.7055, 3.8415, 6.6349},
		{12.2971, 14.2639, 18.5200},
		{18.8928, 21.1314, 25.8650},
		{25.1236, 27.5858, 32.7172},
		{31.2379, 33.8777, 39.3693},
		{37.2786, 40.0763, 45.8662},
		{43.2947, 46.2299, 52.3069},
		{49.2855, 52.3622, 58.6634},
		{55.2412, 58.4332, 64.9960},
		{61.2041, 64.5040, 71.2525},
		{67.1307, 70.5392, 77.4877},
		{73.0563, 76.5734, 83.7105},
	}

	// Case 5: Intercept=true, Trend=true (unrestricted constant + unrestricted linear trend in VECM, MHM Case 5).
	johansenTraceInterceptTrend = [12][3]float64{
		{2.7055, 3.8415, 6.6349},
		{15.9095, 18.1763, 23.1485},
		{31.8831, 34.5559, 40.3039},
		{51.6492, 55.2459, 62.5202},
		{75.1027, 79.3422, 87.7748},
		{102.4674, 107.3429, 116.9829},
		{133.7852, 139.2780, 150.0778},
		{168.9218, 175.1584, 187.1891},
		{208.0567, 215.1268, 228.2226},
		{251.2650, 259.0267, 273.3838},
		{298.4145, 306.8988, 322.4264},
		{349.5562, 358.7190, 375.3203},
	}
	johansenMaxEigInterceptTrend = [12][3]float64{
		{2.7055, 3.8415, 6.6349},
		{14.9965, 17.1481, 21.7465},
		{21.5308, 23.9753, 29.1060},
		{27.8518, 30.6335, 36.2256},
		{34.0365, 36.9873, 42.9901},
		{40.1587, 43.2529, 49.5892},
		{46.2314, 49.4825, 56.0825},
		{52.2562, 55.6634, 62.5015},
		{58.2673, 61.8051, 68.8679},
		{64.2486, 67.9040, 75.1887},
		{70.2078, 73.9958, 81.4816},
		{76.1558, 80.0645, 87.7495},
	}
)

func johansenConfidenceIndex(confidence float64) (int, error) {
	if !finite(confidence) {
		return -1, errors.New("statistics: nonfinite Johansen confidence level")
	}
	switch {
	case math.Abs(confidence-0.90) <= 1e-6:
		return 0, nil
	case math.Abs(confidence-0.95) <= 1e-6:
		return 1, nil
	case math.Abs(confidence-0.99) <= 1e-6:
		return 2, nil
	default:
		return -1, errors.New("statistics: unsupported Johansen confidence level (must be 0.90, 0.95, or 0.99)")
	}
}

func selectJohansenTables(spec JohansenSpec) (*[12][3]float64, *[12][3]float64, error) {
	if spec.Trend && !spec.Intercept || spec.DifferenceLags < 0 || spec.DifferenceLags > 128 {
		return nil, nil, errors.New("statistics: invalid Johansen deterministic/lag specification")
	}
	switch {
	case !spec.Intercept && !spec.Trend:
		return &johansenTraceNoDet, &johansenMaxEigNoDet, nil
	case spec.Intercept && !spec.Trend:
		return &johansenTraceIntercept, &johansenMaxEigIntercept, nil
	default:
		return &johansenTraceInterceptTrend, &johansenMaxEigInterceptTrend, nil
	}
}

// JohansenTraceCriticalValues returns asymptotic Trace-statistic critical values
// of length d ordered by r = 0..d-1 (where m = d - r is the number of stochastic
// trends under H0: rank <= r) for confidence in {0.90, 0.95, 0.99}. Exact
// MacKinnon-Haug-Michelis / Osterwald-Lenum tables are used for m <= 12 and
// Brownian-moment quadratic extrapolation for 13 <= m <= 64.
func JohansenTraceCriticalValues(d int, spec JohansenSpec, confidence float64) ([]float64, error) {
	if d < 2 || d > 64 {
		return nil, errors.New("statistics: invalid Johansen critical-value dimension")
	}
	ci, err := johansenConfidenceIndex(confidence)
	if err != nil {
		return nil, err
	}
	traceTbl, _, err := selectJohansenTables(spec)
	if err != nil {
		return nil, err
	}
	out := make([]float64, d)
	for r := range d {
		m := d - r
		if m <= 12 {
			out[r] = traceTbl[m-1][ci]
			continue
		}
		// Asymptotic trace mean grows as 2*m^2 + O(m), so second difference is 4.0.
		k := float64(m - 12)
		step12 := traceTbl[11][ci] - traceTbl[10][ci]
		out[r] = traceTbl[11][ci] + k*(step12+4.0) + 2.0*k*(k-1)
	}
	return out, nil
}

// JohansenMaxEigCriticalValues returns asymptotic Maximum-Eigenvalue critical
// values of length d ordered by r = 0..d-1 (m = d - r) for confidence in
// {0.90, 0.95, 0.99}.
func JohansenMaxEigCriticalValues(d int, spec JohansenSpec, confidence float64) ([]float64, error) {
	if d < 2 || d > 64 {
		return nil, errors.New("statistics: invalid Johansen critical-value dimension")
	}
	ci, err := johansenConfidenceIndex(confidence)
	if err != nil {
		return nil, err
	}
	_, maxTbl, err := selectJohansenTables(spec)
	if err != nil {
		return nil, err
	}
	out := make([]float64, d)
	for r := range d {
		m := d - r
		if m <= 12 {
			out[r] = maxTbl[m-1][ci]
			continue
		}
		k := float64(m - 12)
		step12 := maxTbl[11][ci] - maxTbl[10][ci]
		out[r] = maxTbl[11][ci] + k*step12
	}
	return out, nil
}

// MaxEigRank sequentially rejects H0: rank = r against H1: rank = r + 1 using
// the supplied Maximum-Eigenvalue critical values.
func (j JohansenResult) MaxEigRank(criticalValues []float64) (int, error) {
	if len(j.MaximumEigenvalue) == 0 || len(criticalValues) != len(j.MaximumEigenvalue) {
		return 0, errors.New("statistics: Johansen rank thresholds must match dimension")
	}
	for i, v := range criticalValues {
		if !finite(v) || v <= 0 || !finite(j.MaximumEigenvalue[i]) || j.MaximumEigenvalue[i] < 0 {
			return 0, errors.New("statistics: invalid Johansen rank threshold/statistic")
		}
	}
	rank := 0
	for i, v := range criticalValues {
		if j.MaximumEigenvalue[i] <= v {
			break
		}
		rank++
	}
	return rank, nil
}

// CointegrationRank determines the sequential Trace-test cointegration rank r
// in [0, d] using asymptotic critical values matching spec and confidence
// (0.90, 0.95, or 0.99).
func (j JohansenResult) CointegrationRank(spec JohansenSpec, confidence float64) (int, error) {
	if len(j.Trace) < 2 {
		return 0, errors.New("statistics: invalid Johansen result dimension")
	}
	cv, err := JohansenTraceCriticalValues(len(j.Trace), spec, confidence)
	if err != nil {
		return 0, err
	}
	return j.TraceRank(cv)
}

// IsCointegrated reports whether H0: rank == 0 (no cointegration) is rejected
// at the requested confidence level (0.90, 0.95, or 0.99) for spec.
func (j JohansenResult) IsCointegrated(spec JohansenSpec, confidence float64) (bool, error) {
	rank, err := j.CointegrationRank(spec, confidence)
	if err != nil {
		return false, err
	}
	return rank >= 1, nil
}
