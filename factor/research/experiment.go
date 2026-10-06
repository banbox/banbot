package research

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"maps"
	"math/rand"
	"os"
	"slices"
	"sort"
	"sync"

	"github.com/banbox/banbot/factor"
)

type FactorMetadata struct {
	Name, Version, Description, Direction, Owner string
	Sources, Tags                                []string
}

// RandomBaselineScores produces a reproducible shuffled score cross-section,
// using only declared SIDs. Caller can derive a per-round seed explicitly and
// record that seed/schedule in its trial; no future returns select members.
func RandomBaselineScores(sids []int32, seed int64) (map[int32]factor.Numeric, error) {
	pool := uniqueSIDs(sids)
	if len(pool) == 0 {
		return nil, errors.New("research: random baseline needs a declared pool")
	}
	for _, sid := range pool {
		if sid <= 0 {
			return nil, errors.New("research: invalid baseline SID")
		}
	}
	random := rand.New(rand.NewSource(seed))
	permutation := random.Perm(len(pool))
	scores := make(map[int32]factor.Numeric, len(pool))
	for i, sid := range pool {
		scores[sid] = factor.Numeric{Value: float64(permutation[i]), Validity: factor.Valid}
	}
	return scores, nil
}

type FactorRegistry struct {
	sync.RWMutex
	metadata map[string]FactorMetadata
}

func (r *FactorRegistry) Register(meta FactorMetadata) error {
	if meta.Name == "" || meta.Version == "" || (meta.Direction != "positive" && meta.Direction != "negative" && meta.Direction != "unsigned") {
		return errors.New("research: invalid factor metadata")
	}
	r.Lock()
	defer r.Unlock()
	if r.metadata == nil {
		r.metadata = map[string]FactorMetadata{}
	}
	if _, exists := r.metadata[meta.Name]; exists {
		return errors.New("research: duplicate factor metadata")
	}
	meta.Sources = slices.Clone(meta.Sources)
	meta.Tags = slices.Clone(meta.Tags)
	r.metadata[meta.Name] = meta
	return nil
}
func (r *FactorRegistry) Snapshot() []FactorMetadata {
	r.RLock()
	defer r.RUnlock()
	var result []FactorMetadata
	for _, meta := range r.metadata {
		meta.Sources = slices.Clone(meta.Sources)
		meta.Tags = slices.Clone(meta.Tags)
		result = append(result, meta)
	}
	slices.SortFunc(result, func(a, b FactorMetadata) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return result
}

type Trial struct {
	ID, Name, ManifestID, AlgorithmVersion, Baseline                    string
	Seed                                                                int64
	TrainStart, TrainEnd, SampleCutoff, AvailableAt, TestStart, TestEnd int64
	Scans                                                               int
	Parameters                                                          map[string]float64
	Metrics, OutOfSample                                                map[string]float64
	Factors                                                             []FactorMetadata
}

func cloneTrial(t Trial) Trial {
	raw, _ := json.Marshal(t)
	var copy Trial
	_ = json.Unmarshal(raw, &copy)
	return copy
}
func normalizeTrial(t Trial) (Trial, error) {
	if t.Name == "" || t.ManifestID == "" || t.AlgorithmVersion == "" || t.TrainStart <= 0 || t.TrainEnd < t.TrainStart || t.SampleCutoff > t.TrainEnd || t.AvailableAt < t.TrainEnd || t.Scans < 1 || (t.TestStart != 0 && (t.TestStart <= t.TrainEnd || t.TestEnd < t.TestStart)) {
		return Trial{}, errors.New("research: invalid trial lineage/window")
	}
	for _, values := range []map[string]float64{t.Parameters, t.Metrics, t.OutOfSample} {
		for _, v := range values {
			if !finiteValues(v) {
				return Trial{}, errors.New("research: nonfinite trial metric")
			}
		}
	}
	id := t.ID
	t.ID = ""
	hash, err := hashJSON(t)
	if err != nil {
		return Trial{}, err
	}
	if id != "" && id != hash {
		return Trial{}, errors.New("research: conflicting trial identity")
	}
	t.ID = hash
	return cloneTrial(t), nil
}

// TrialLedger is a bounded single-process append-only ledger. Open rejects
// torn/corrupt records; it never silently discards evidence after a crash.
type TrialLedger struct {
	sync.Mutex
	path    string
	maximum int
	trials  map[string]Trial
}

func OpenTrialLedger(path string, maximum int) (*TrialLedger, error) {
	if path == "" || maximum < 1 {
		return nil, errors.New("research: trial ledger needs path and limit")
	}
	l := &TrialLedger{path: path, maximum: maximum, trials: map[string]Trial{}}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() > 0 {
		tail := make([]byte, 1)
		if _, err = file.ReadAt(tail, stat.Size()-1); err != nil {
			return nil, err
		}
		if tail[0] != '\n' {
			return nil, errors.New("research: unterminated trial ledger record; retain file for recovery")
		}
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var trial Trial
		if err := json.Unmarshal(scanner.Bytes(), &trial); err != nil {
			return nil, err
		}
		trial, err = normalizeTrial(trial)
		if err != nil {
			return nil, err
		}
		if _, exists := l.trials[trial.ID]; exists {
			return nil, errors.New("research: duplicate ledger trial")
		}
		if len(l.trials) >= maximum {
			return nil, errors.New("research: trial ledger budget exceeded")
		}
		l.trials[trial.ID] = trial
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return l, nil
}
func (l *TrialLedger) Append(trial Trial) (string, error) {
	trial, err := normalizeTrial(trial)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(trial)
	if err != nil {
		return "", err
	}
	if len(raw) > 1<<20 {
		return "", errors.New("research: trial record size exceeded")
	}
	l.Lock()
	defer l.Unlock()
	if _, exists := l.trials[trial.ID]; exists {
		return trial.ID, nil
	}
	if len(l.trials) >= l.maximum {
		return "", errors.New("research: trial ledger budget exceeded")
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if _, err = file.Write(append(raw, '\n')); err != nil {
		return "", err
	}
	if err = file.Sync(); err != nil {
		return "", err
	}
	l.trials[trial.ID] = trial
	return trial.ID, nil
}
func (l *TrialLedger) Trials() []Trial {
	l.Lock()
	defer l.Unlock()
	var result []Trial
	for _, trial := range l.trials {
		result = append(result, cloneTrial(trial))
	}
	slices.SortFunc(result, func(a, b Trial) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return result
}

type TrialComparison struct {
	ID, Name, Baseline    string
	Training, OutOfSample float64
	HasOutOfSample        bool
	Scans                 int
}

// CompareTrials exposes training and held-out results in separate fields; it
// sorts by ID so comparing does not turn test performance into a selector.
func CompareTrials(trials []Trial, metric string) ([]TrialComparison, error) {
	if metric == "" {
		return nil, errors.New("research: comparison metric required")
	}
	var result []TrialComparison
	for _, trial := range trials {
		trial, err := normalizeTrial(trial)
		if err != nil {
			return nil, err
		}
		value, exists := trial.Metrics[metric]
		if !exists {
			continue
		}
		test, exists := trial.OutOfSample[metric]
		result = append(result, TrialComparison{trial.ID, trial.Name, trial.Baseline, value, test, exists, trial.Scans})
	}
	slices.SortFunc(result, func(a, b TrialComparison) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return result, nil
}

type ParameterObservation struct {
	SID                         int32
	Group, Candidate            string
	BeginAt, EndAt, AvailableAt int64
	NetReturn                   float64
}
type ParameterSelectionSpec struct {
	TrainStart, TrainEnd, AvailableAt int64
	MinIndependentSamples             int
	PriorSamples, ConfidencePenalty   float64
	HACLag                            int
	ManifestID, AlgorithmVersion      string
	Candidates                        map[string]map[string]float64
}
type ParameterRecommendation struct {
	Candidate, Scope                      string
	Samples, IndependentSamples           int
	Mean, StandardError, Score, Shrinkage float64
}
type ParameterArtifact struct {
	ID, ManifestID, AlgorithmVersion                string
	TrainStart, TrainEnd, SampleCutoff, AvailableAt int64
	Scans                                           int
	Global                                          ParameterRecommendation
	ByGroup                                         map[string]ParameterRecommendation
	ByAsset                                         map[int32]ParameterRecommendation
	Candidates                                      map[string]map[string]float64 `json:",omitempty"`
}

// SelectParameters fits on visible training observations, counts nonoverlap
// episodes and shrinks sparse asset estimates toward group/global estimates.
func SelectParameters(observations []ParameterObservation, spec ParameterSelectionSpec) (ParameterArtifact, error) {
	if spec.TrainStart <= 0 || spec.TrainEnd < spec.TrainStart || spec.AvailableAt < spec.TrainEnd || spec.MinIndependentSamples < 2 || spec.PriorSamples < 0 || spec.ConfidencePenalty < 0 || spec.HACLag < 0 || !finiteValues(spec.PriorSamples, spec.ConfidencePenalty) || spec.ManifestID == "" || spec.AlgorithmVersion == "" {
		return ParameterArtifact{}, errors.New("research: invalid parameter training specification")
	}
	rows := slices.Clone(observations)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EndAt != rows[j].EndAt {
			return rows[i].EndAt < rows[j].EndAt
		}
		if rows[i].BeginAt != rows[j].BeginAt {
			return rows[i].BeginAt < rows[j].BeginAt
		}
		return rows[i].SID < rows[j].SID
	})
	global := map[string][]ParameterObservation{}
	groups := map[string]map[string][]ParameterObservation{}
	assets := map[int32]map[string][]ParameterObservation{}
	assetGroup := map[int32]string{}
	a := ParameterArtifact{ManifestID: spec.ManifestID, AlgorithmVersion: spec.AlgorithmVersion, TrainStart: spec.TrainStart, TrainEnd: spec.TrainEnd, AvailableAt: spec.AvailableAt, ByGroup: map[string]ParameterRecommendation{}, ByAsset: map[int32]ParameterRecommendation{}}
	for _, row := range rows {
		if row.BeginAt < spec.TrainStart || row.EndAt > spec.TrainEnd || row.AvailableAt > spec.AvailableAt {
			continue
		}
		if row.SID <= 0 || row.Candidate == "" || row.EndAt <= row.BeginAt || row.AvailableAt <= 0 || !finiteValues(row.NetReturn) {
			return ParameterArtifact{}, errors.New("research: invalid parameter observation")
		}
		global[row.Candidate] = append(global[row.Candidate], row)
		if groups[row.Group] == nil {
			groups[row.Group] = map[string][]ParameterObservation{}
		}
		groups[row.Group][row.Candidate] = append(groups[row.Group][row.Candidate], row)
		if assets[row.SID] == nil {
			assets[row.SID] = map[string][]ParameterObservation{}
		}
		assets[row.SID][row.Candidate] = append(assets[row.SID][row.Candidate], row)
		assetGroup[row.SID] = row.Group
		a.SampleCutoff = max(a.SampleCutoff, row.EndAt)
	}
	a.Scans = len(global)
	if a.Scans == 0 {
		return ParameterArtifact{}, errors.New("research: no visible parameter scans")
	}
	if spec.Candidates != nil {
		a.Candidates = map[string]map[string]float64{}
		for candidate := range global {
			params, exists := spec.Candidates[candidate]
			if !exists {
				return ParameterArtifact{}, errors.New("research: parameter candidate not declared")
			}
			for key, value := range params {
				if key == "" || !finiteValues(value) {
					return ParameterArtifact{}, errors.New("research: invalid candidate parameters")
				}
			}
			a.Candidates[candidate] = maps.Clone(params)
		}
	}
	estimate := func(candidate, scope string, rows []ParameterObservation, prior *ParameterRecommendation) ParameterRecommendation {
		values := make([]float64, len(rows))
		independent, lastEnd := 0, int64(0)
		for i, row := range rows {
			values[i] = row.NetReturn
			if row.BeginAt >= lastEnd {
				independent++
				lastEnd = row.EndAt
			}
		}
		mean, se := 0.0, 0.0
		if len(values) == 1 {
			mean = values[0]
		} else {
			mean, se, _ = HACMean(values, min(spec.HACLag, len(values)-1))
		}
		weight := 1.0
		if prior != nil {
			weight = float64(independent) / (float64(independent) + spec.PriorSamples)
			mean = weight*mean + (1-weight)*prior.Mean
			se = weight*se + (1-weight)*prior.StandardError
		}
		return ParameterRecommendation{Candidate: candidate, Scope: scope, Samples: len(rows), IndependentSamples: independent, Mean: mean, StandardError: se, Score: mean - spec.ConfidencePenalty*se, Shrinkage: weight}
	}
	globalEstimates := map[string]ParameterRecommendation{}
	var best *ParameterRecommendation
	for _, name := range sortedKeys(global) {
		r := estimate(name, "global", global[name], nil)
		globalEstimates[name] = r
		if r.IndependentSamples >= spec.MinIndependentSamples && (best == nil || r.Score > best.Score) {
			copy := r
			best = &copy
		}
	}
	if best == nil {
		return ParameterArtifact{}, errors.New("research: insufficient independent parameter samples")
	}
	a.Global = *best
	groupEstimates := map[string]map[string]ParameterRecommendation{}
	for _, group := range sortedKeys(groups) {
		groupEstimates[group] = map[string]ParameterRecommendation{}
		choice := a.Global
		for _, candidate := range sortedKeys(groups[group]) {
			prior := globalEstimates[candidate]
			r := estimate(candidate, "group:"+group, groups[group][candidate], &prior)
			groupEstimates[group][candidate] = r
			if r.IndependentSamples >= spec.MinIndependentSamples && (choice.Scope == "global" || r.Score > choice.Score) {
				choice = r
			}
		}
		a.ByGroup[group] = choice
	}
	for sid, candidates := range assets {
		choice := a.ByGroup[assetGroup[sid]]
		for _, candidate := range sortedKeys(candidates) {
			prior, ok := groupEstimates[assetGroup[sid]][candidate]
			if !ok {
				prior = globalEstimates[candidate]
			}
			r := estimate(candidate, "asset", candidates[candidate], &prior)
			if r.IndependentSamples >= spec.MinIndependentSamples && (choice.Scope != "asset" || r.Score > choice.Score) {
				choice = r
			}
		}
		a.ByAsset[sid] = choice
	}
	a.ID = ""
	id, err := hashJSON(a)
	a.ID = id
	return a, err
}
func ResolveParameters(artifacts []ParameterArtifact, asof int64, manifest string) (ParameterArtifact, error) {
	var selected *ParameterArtifact
	for _, artifact := range artifacts {
		if artifact.ManifestID != manifest || artifact.AvailableAt > asof || artifact.TrainEnd >= asof {
			continue
		}
		id := artifact.ID
		artifact.ID = ""
		hash, err := hashJSON(artifact)
		if err != nil || id != hash || artifact.AlgorithmVersion == "" || artifact.TrainStart <= 0 || artifact.TrainEnd < artifact.TrainStart || artifact.SampleCutoff < artifact.TrainStart || artifact.Scans < 1 || artifact.Global.Candidate == "" || artifact.SampleCutoff > artifact.TrainEnd || artifact.AvailableAt < artifact.TrainEnd {
			return ParameterArtifact{}, errors.New("research: corrupt parameter artifact")
		}
		artifact.ID = id
		if selected == nil || artifact.AvailableAt > selected.AvailableAt || (artifact.AvailableAt == selected.AvailableAt && artifact.ID > selected.ID) {
			copy := artifact
			selected = &copy
		}
	}
	if selected == nil {
		return ParameterArtifact{}, errors.New("research: no PIT-visible parameter artifact")
	}
	raw, _ := json.Marshal(selected)
	var result ParameterArtifact
	_ = json.Unmarshal(raw, &result)
	return result, nil
}
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

type CacheIdentity struct{ ManifestID, AlgorithmVersion, SchemaVersion, UniverseVersion, SourceRevision, Window string }

func (identity CacheIdentity) Key() (string, error) {
	if identity.ManifestID == "" || identity.AlgorithmVersion == "" || identity.SchemaVersion == "" || identity.UniverseVersion == "" || identity.SourceRevision == "" || identity.Window == "" {
		return "", errors.New("research: incomplete cache invalidation identity")
	}
	return hashJSON(identity)
}

type cacheEntry struct {
	key   string
	value []byte
}
type ResearchCache struct {
	sync.Mutex
	maximum, used int
	entries       map[string]*list.Element
	lru           *list.List
}

func NewResearchCache(maximumBytes int) (*ResearchCache, error) {
	if maximumBytes < 1 {
		return nil, errors.New("research: cache byte budget required")
	}
	return &ResearchCache{maximum: maximumBytes, entries: map[string]*list.Element{}, lru: list.New()}, nil
}
func (c *ResearchCache) Put(identity CacheIdentity, value []byte) error {
	key, err := identity.Key()
	if err != nil {
		return err
	}
	c.Lock()
	defer c.Unlock()
	if len(value)+len(key) > c.maximum {
		return errors.New("research: cache value exceeds budget")
	}
	if entry := c.entries[key]; entry != nil {
		old := entry.Value.(cacheEntry)
		c.used -= len(old.value) + len(old.key)
		c.lru.Remove(entry)
		delete(c.entries, key)
	}
	for c.used+len(value)+len(key) > c.maximum {
		entry := c.lru.Back()
		old := entry.Value.(cacheEntry)
		c.used -= len(old.value) + len(old.key)
		delete(c.entries, old.key)
		c.lru.Remove(entry)
	}
	c.entries[key] = c.lru.PushFront(cacheEntry{key, bytes.Clone(value)})
	c.used += len(value) + len(key)
	return nil
}
func (c *ResearchCache) Get(identity CacheIdentity) ([]byte, bool) {
	key, err := identity.Key()
	if err != nil {
		return nil, false
	}
	c.Lock()
	defer c.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil, false
	}
	c.lru.MoveToFront(entry)
	return bytes.Clone(entry.Value.(cacheEntry).value), true
}
func (c *ResearchCache) Bytes() int { c.Lock(); defer c.Unlock(); return c.used }
