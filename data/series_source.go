package data

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
)

type DataSink interface {
	Emit(sub *strat.DataSub, rows []*orm.DataRecord) error
}

type DataSource interface {
	Info() *orm.SeriesInfo
	FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)
	SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error
}

type DataSourceVersioner interface {
	Version() string
}

type DataSourceOpStatus struct {
	State     string `json:"state"`
	Error     string `json:"error,omitempty"`
	AtMS      int64  `json:"at_ms,omitempty"`
	Sid       int32  `json:"sid,omitempty"`
	TimeFrame string `json:"timeframe,omitempty"`
	StartMS   int64  `json:"start_ms,omitempty"`
	EndMS     int64  `json:"end_ms,omitempty"`
	Rows      int    `json:"rows,omitempty"`
	Subs      int    `json:"subs,omitempty"`
}

type DataSourceStatus struct {
	Name                   string             `json:"name"`
	Health                 string             `json:"health"`
	RegisteredAtMS         int64              `json:"registered_at_ms"`
	RegisterSource         string             `json:"register_source"`
	Version                string             `json:"version,omitempty"`
	Type                   string             `json:"type"`
	TimeFrame              string             `json:"timeframe"`
	Table                  string             `json:"table"`
	DuplicateRegistrations int                `json:"duplicate_registrations,omitempty"`
	LastError              string             `json:"last_error,omitempty"`
	LastBackfill           DataSourceOpStatus `json:"last_backfill"`
	Subscription           DataSourceOpStatus `json:"subscription"`
}

type SeriesRuntime struct {
	Repo       orm.SeriesRepo
	Sink       DataSink
	EnsureFunc func(ctx context.Context, plan *SeriesPlan) *errs.Error
	ActivateFn func(ctx context.Context, subs []*strat.DataSub, sink DataSink) ([]*strat.DataSub, error)

	active map[string][]string
}

func NewSeriesRuntime(sink DataSink) *SeriesRuntime {
	return &SeriesRuntime{Sink: sink}
}

func (r *SeriesRuntime) Plan(jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	return NewSeriesPlan(jobs, warmupAnchorMS, endMS)
}

func (r *SeriesRuntime) Sync(ctx context.Context, jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	plan, err := r.Plan(jobs, warmupAnchorMS, endMS)
	if err != nil {
		return nil, err
	}
	return plan, r.Apply(ctx, plan)
}

func (r *SeriesRuntime) SyncLive(ctx context.Context, jobs []*strat.StratJob, nowMS int64) (*SeriesPlan, error) {
	return r.Sync(ctx, jobs, nowMS, nowMS)
}

func (r *SeriesRuntime) Apply(ctx context.Context, plan *SeriesPlan) error {
	if plan == nil || !plan.HasSubs() {
		return nil
	}
	if err := r.Ensure(ctx, plan); err != nil {
		return err
	}
	return r.ActivateNew(ctx, plan.Subs)
}

func (r *SeriesRuntime) Ensure(ctx context.Context, plan *SeriesPlan) error {
	if plan == nil || !plan.HasSubs() {
		return nil
	}
	ensureFn := r.EnsureFunc
	if ensureFn == nil {
		ensureFn = func(ctx context.Context, plan *SeriesPlan) *errs.Error {
			return plan.Ensure(ctx, r.repo())
		}
	}
	if err := ensureFn(ctx, plan); err != nil {
		return err
	}
	return nil
}

func (r *SeriesRuntime) ActivateNew(ctx context.Context, subs []*strat.DataSub) error {
	newSubs := r.newSubs(subs)
	if len(newSubs) == 0 {
		return nil
	}
	activateFn := r.ActivateFn
	if activateFn == nil {
		activateFn = ActivateDataSources
	}
	activated, err := activateFn(ctx, newSubs, r.Sink)
	r.markActive(activated)
	if err != nil {
		return wrapBootstrapActivateErr(newSubs, err)
	}
	return nil
}

func (r *SeriesRuntime) Active(key string) bool {
	if r == nil || len(r.active) == 0 {
		return false
	}
	_, ok := r.active[key]
	return ok
}

func (r *SeriesRuntime) repo() orm.SeriesRepo {
	if r != nil && r.Repo != nil {
		return r.Repo
	}
	return nil
}

func (r *SeriesRuntime) newSubs(subs []*strat.DataSub) []*strat.DataSub {
	if len(subs) == 0 {
		return nil
	}
	if r.active == nil {
		r.active = make(map[string][]string)
	}
	items := make([]*strat.DataSub, 0, len(subs))
	for _, sub := range subs {
		key, ok := runtimeSubKey(sub)
		have, active := r.active[key]
		if !ok || active && fieldsContain(have, sub.Fields) {
			continue
		}
		items = append(items, sub)
	}
	return items
}

func (r *SeriesRuntime) markActive(subs []*strat.DataSub) {
	if len(subs) == 0 {
		return
	}
	if r.active == nil {
		r.active = make(map[string][]string)
	}
	for _, sub := range subs {
		key, ok := runtimeSubKey(sub)
		if ok {
			r.active[key] = orm.MergeSeriesFields(r.active[key], sub.Fields)
		}
	}
}

func fieldsContain(have, want []string) bool {
	if len(want) == 0 {
		return true
	}
	seen := make(map[string]bool, len(have))
	for _, field := range have {
		seen[field] = true
	}
	for _, field := range want {
		if !seen[field] {
			return false
		}
	}
	return true
}

func runtimeSubKey(sub *strat.DataSub) (string, bool) {
	if sub == nil || sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return "", false
	}
	source := orm.NormalizeSeriesSource(sub.Source)
	if source == orm.SeriesSourceKline {
		return "", false
	}
	return strat.DataSubKey(source, sub.ExSymbol.ID, sub.TimeFrame), true
}

type SeriesPlan struct {
	Subs    []*strat.DataSub
	StartMS int64
	EndMS   int64
}

type ThirdPartySeriesBootstrap = SeriesPlan

func NewSeriesPlan(jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	if warmupAnchorMS <= 0 {
		return nil, fmt.Errorf("bootstrap collect phase=collect: warmup anchor is required")
	}
	if endMS <= 0 {
		return nil, fmt.Errorf("bootstrap collect phase=collect: end time is required")
	}
	if warmupAnchorMS > endMS {
		return nil, fmt.Errorf("bootstrap collect phase=collect: warmup anchor must not exceed end time")
	}
	subs, err := CollectRuntimeDataSubs(jobs)
	if err != nil {
		return nil, err
	}
	startMS := warmupAnchorMS
	if len(subs) > 0 {
		startMS, err = ThirdPartyWarmupStart(subs, warmupAnchorMS)
		if err != nil {
			return nil, err
		}
	}
	return &SeriesPlan{
		Subs:    subs,
		StartMS: startMS,
		EndMS:   endMS,
	}, nil
}

func NewThirdPartySeriesBootstrap(jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	return NewSeriesPlan(jobs, warmupAnchorMS, endMS)
}

func (p *SeriesPlan) HasSubs() bool {
	return p != nil && len(p.Subs) > 0
}

func (p *SeriesPlan) HasHistoryRange() bool {
	return p != nil && len(p.Subs) > 0 && p.StartMS < p.EndMS
}

func (p *SeriesPlan) Ensure(ctx context.Context, repo orm.SeriesRepo) *errs.Error {
	if p == nil {
		return nil
	}
	return EnsureSeriesSubsRange(ctx, repo, p.Subs, p.StartMS, p.EndMS)
}

func (p *SeriesPlan) Activate(ctx context.Context, sink DataSink) ([]*strat.DataSub, error) {
	if p == nil {
		return nil, nil
	}
	return ActivateDataSources(ctx, p.Subs, sink)
}

var (
	dataSourcesMu sync.RWMutex
	dataSources   = make(map[string]DataSource)
	dataSourceSta = make(map[string]*DataSourceStatus)
)

func RegisterDataSource(src DataSource) error {
	if src == nil {
		return fmt.Errorf("data source is nil")
	}
	info := src.Info()
	if err := orm.ValidateSeriesInfo(info); err != nil {
		return err
	}
	if info.Name == "" {
		return fmt.Errorf("data source name is required")
	}
	dataSourcesMu.Lock()
	defer dataSourcesMu.Unlock()
	if _, ok := dataSources[info.Name]; ok {
		updateDataSourceStatusLocked(info.Name, func(st *DataSourceStatus) {
			st.DuplicateRegistrations++
			st.LastError = dataSourceLastError(st, "duplicate registration")
			st.Health = dataSourceHealth(st)
		})
		return fmt.Errorf("data source %q already registered", info.Name)
	}
	dataSources[info.Name] = src
	dataSourceSta[info.Name] = newDataSourceStatus(src, info)
	return nil
}

func GetDataSource(name string) DataSource {
	dataSourcesMu.RLock()
	defer dataSourcesMu.RUnlock()
	return dataSources[name]
}

func ListDataSources() []string {
	dataSourcesMu.RLock()
	defer dataSourcesMu.RUnlock()
	items := make([]string, 0, len(dataSources))
	for name := range dataSources {
		items = append(items, name)
	}
	sort.Strings(items)
	return items
}

func ListDataSourceStatus() []*DataSourceStatus {
	dataSourcesMu.RLock()
	defer dataSourcesMu.RUnlock()
	names := make([]string, 0, len(dataSourceSta))
	for name := range dataSourceSta {
		names = append(names, name)
	}
	sort.Strings(names)
	items := make([]*DataSourceStatus, 0, len(names))
	for _, name := range names {
		cp := *dataSourceSta[name]
		items = append(items, &cp)
	}
	return items
}

func ActivateDataSources(ctx context.Context, subs []*strat.DataSub, sink DataSink) ([]*strat.DataSub, error) {
	if len(subs) == 0 {
		return nil, nil
	}
	if sink == nil {
		return nil, fmt.Errorf("data sink is required")
	}
	seen := make(map[string]*strat.DataSub)
	for _, sub := range subs {
		normalized, err := validateActivationSub(sub)
		if err != nil {
			return nil, err
		}
		src := GetDataSource(normalized.Source)
		if src == nil {
			return nil, fmt.Errorf("data source %q is not registered", normalized.Source)
		}
		info := src.Info()
		if err := orm.ValidateSeriesInfo(info); err != nil {
			return nil, err
		}
		if normalized.TimeFrame != info.TimeFrame {
			return nil, fmt.Errorf("sub timeframe %s does not match source timeframe %s", normalized.TimeFrame, info.TimeFrame)
		}
		if err = normalizeDataSubFields(info, normalized); err != nil {
			return nil, err
		}
		mergeDataSub(seen, normalized)
	}
	grouped := make(map[string][]*strat.DataSub)
	for _, sub := range sortedDataSubs(seen) {
		grouped[sub.Source] = append(grouped[sub.Source], sub)
	}
	activated := make([]*strat.DataSub, 0, len(seen))
	for _, name := range sortedSourceNames(grouped) {
		markDataSourceSubscription(name, DataSourceOpStatus{
			State: "subscribing",
			AtMS:  time.Now().UnixMilli(),
			Subs:  len(grouped[name]),
		}, "")
		if err := GetDataSource(name).SubscribeLive(ctx, grouped[name], sink); err != nil {
			markDataSourceSubscription(name, DataSourceOpStatus{
				State: "error",
				Error: err.Error(),
				AtMS:  time.Now().UnixMilli(),
				Subs:  len(grouped[name]),
			}, err.Error())
			return activated, fmt.Errorf("activate data source %q: %w", name, err)
		}
		activated = append(activated, grouped[name]...)
		markDataSourceSubscription(name, DataSourceOpStatus{
			State: "subscribed",
			AtMS:  time.Now().UnixMilli(),
			Subs:  len(grouped[name]),
		}, "")
	}
	return activated, nil
}

func validateActivationSub(sub *strat.DataSub) (*strat.DataSub, error) {
	if sub == nil {
		return nil, fmt.Errorf("data sub is required")
	}
	if sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return nil, fmt.Errorf("data sub exsymbol is required")
	}
	normalized := *sub
	normalized.Source = orm.NormalizeSeriesSource(sub.Source)
	if normalized.TimeFrame == "" {
		return nil, fmt.Errorf("data sub timeframe is required")
	}
	return &normalized, nil
}

func validateBootstrapSub(sub *strat.DataSub) (*strat.DataSub, error) {
	normalized, err := validateActivationSub(sub)
	if err != nil {
		return nil, err
	}
	if normalized.Source == "" {
		return nil, fmt.Errorf("data sub source is required")
	}
	if normalized.Source != orm.SeriesSourceKline {
		if src := GetDataSource(normalized.Source); src != nil {
			if err := normalizeDataSubFields(src.Info(), normalized); err != nil {
				return nil, err
			}
		}
	}
	return normalized, nil
}

func sortedSourceNames(grouped map[string][]*strat.DataSub) []string {
	items := make([]string, 0, len(grouped))
	for name := range grouped {
		items = append(items, name)
	}
	sort.Strings(items)
	return items
}

func CollectRuntimeDataSubs(jobs []*strat.StratJob) ([]*strat.DataSub, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	seen := make(map[string]*strat.DataSub)
	for _, job := range jobs {
		if job == nil {
			continue
		}
		for _, sub := range strat.CollectDataSubs(job) {
			normalized, err := validateBootstrapSub(sub)
			if err != nil {
				return nil, fmt.Errorf("bootstrap collect: %w", err)
			}
			if normalized.Source == orm.SeriesSourceKline {
				continue
			}
			mergeDataSub(seen, normalized)
		}
	}
	return sortedDataSubs(seen), nil
}

func mergeDataSub(seen map[string]*strat.DataSub, sub *strat.DataSub) {
	if seen == nil || sub == nil || sub.ExSymbol == nil {
		return
	}
	key := strat.DataSubKey(sub.Source, sub.ExSymbol.ID, sub.TimeFrame)
	if existing, ok := seen[key]; ok {
		if sub.WarmupNum > existing.WarmupNum {
			existing.WarmupNum = sub.WarmupNum
		}
		existing.Fields = orm.MergeSeriesFields(existing.Fields, sub.Fields)
		return
	}
	cp := *sub
	cp.Fields = append([]string(nil), sub.Fields...)
	seen[key] = &cp
}

func EnsureThirdPartySeriesRange(ctx context.Context, repo orm.SeriesRepo, jobs []*strat.StratJob, startMS, endMS int64) ([]*strat.DataSub, *errs.Error) {
	return EnsureRuntimeSeriesRange(ctx, repo, jobs, startMS, endMS)
}

func EnsureRuntimeSeriesRange(ctx context.Context, repo orm.SeriesRepo, jobs []*strat.StratJob, startMS, endMS int64) ([]*strat.DataSub, *errs.Error) {
	if startMS >= endMS {
		return nil, nil
	}
	subs, err := CollectRuntimeDataSubs(jobs)
	if err != nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "%v", err)
	}
	if err := EnsureSeriesSubsRange(ctx, repo, subs, startMS, endMS); err != nil {
		return nil, err
	}
	return subs, nil
}

func EnsureThirdPartySeriesSubsRange(ctx context.Context, repo orm.SeriesRepo, subs []*strat.DataSub, startMS, endMS int64) *errs.Error {
	return EnsureSeriesSubsRange(ctx, repo, subs, startMS, endMS)
}

func EnsureSeriesSubsRange(ctx context.Context, repo orm.SeriesRepo, subs []*strat.DataSub, startMS, endMS int64) *errs.Error {
	if startMS >= endMS {
		return nil
	}
	for _, sub := range subs {
		normalized, err := validateBootstrapSub(sub)
		if err != nil {
			return errs.NewMsg(core.ErrBadConfig, "bootstrap collect: %v", err)
		}
		if normalized.Source == orm.SeriesSourceKline {
			continue
		}
		src := GetDataSource(normalized.Source)
		if src == nil {
			return errs.NewMsg(core.ErrBadConfig,
				"bootstrap ensure source=%s sid=%d tf=%s phase=ensure: data source is not registered",
				normalized.Source, normalized.ExSymbol.ID, normalized.TimeFrame)
		}
		if err := EnsureSeriesRangeWithRepo(ctx, repoOrDefault(repo), src, normalized, startMS, endMS); err != nil {
			return wrapBootstrapEnsureErr(normalized, err)
		}
	}
	return nil
}

func sortedDataSubs(seen map[string]*strat.DataSub) []*strat.DataSub {
	if len(seen) == 0 {
		return nil
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]*strat.DataSub, 0, len(keys))
	for _, key := range keys {
		items = append(items, seen[key])
	}
	return items
}

func ThirdPartyWarmupStart(subs []*strat.DataSub, endMS int64) (int64, error) {
	if len(subs) == 0 {
		return endMS, nil
	}
	startMS := endMS
	for _, sub := range subs {
		if sub == nil {
			continue
		}
		if sub.TimeFrame == "" {
			return 0, fmt.Errorf("bootstrap collect source=%s sid=%d tf=%s phase=collect: data sub timeframe is required", sub.Source, bootstrapSubSID(sub), sub.TimeFrame)
		}
		if sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
			return 0, fmt.Errorf("bootstrap collect source=%s sid=%d tf=%s phase=collect: data sub exsymbol is required", sub.Source, bootstrapSubSID(sub), sub.TimeFrame)
		}
		tfMS := int64(utils.TFToSecs(sub.TimeFrame)) * 1000
		if tfMS <= 0 {
			return 0, fmt.Errorf("bootstrap collect source=%s sid=%d tf=%s phase=collect: invalid timeframe", sub.Source, sub.ExSymbol.ID, sub.TimeFrame)
		}
		if sub.WarmupNum < 0 {
			return 0, fmt.Errorf("bootstrap collect source=%s sid=%d tf=%s phase=collect: warmup must not be negative", sub.Source, sub.ExSymbol.ID, sub.TimeFrame)
		}
		candidate := endMS - int64(sub.WarmupNum)*tfMS
		if candidate < startMS {
			startMS = candidate
		}
	}
	return startMS, nil
}

func bootstrapSubSID(sub *strat.DataSub) int32 {
	if sub == nil || sub.ExSymbol == nil {
		return 0
	}
	return sub.ExSymbol.ID
}

func repoOrDefault(repo orm.SeriesRepo) orm.SeriesRepo {
	if repo != nil {
		return repo
	}
	return orm.DefaultSeriesRepo()
}

func wrapBootstrapEnsureErr(sub *strat.DataSub, err *errs.Error) *errs.Error {
	if sub == nil || sub.ExSymbol == nil {
		return err
	}
	return errs.NewMsg(err.Code,
		"bootstrap ensure source=%s sid=%d tf=%s phase=ensure: %s",
		sub.Source, sub.ExSymbol.ID, sub.TimeFrame, err.Short())
}

func wrapBootstrapActivateErr(subs []*strat.DataSub, err error) error {
	if err == nil || len(subs) == 0 {
		return err
	}
	bySource := make(map[string]*strat.DataSub)
	for _, sub := range subs {
		if sub == nil {
			continue
		}
		name := orm.NormalizeSeriesSource(sub.Source)
		if _, ok := bySource[name]; !ok {
			bySource[name] = sub
		}
	}
	for _, name := range sortedSubSources(bySource) {
		if strings.Contains(err.Error(), name) {
			sub := bySource[name]
			return fmt.Errorf("bootstrap activate source=%s sid=%d tf=%s phase=activate: %w", name, bootstrapSubSID(sub), sub.TimeFrame, err)
		}
	}
	first := subs[0]
	return fmt.Errorf("bootstrap activate source=%s sid=%d tf=%s phase=activate: %w",
		orm.NormalizeSeriesSource(first.Source), bootstrapSubSID(first), first.TimeFrame, err)
}

func sortedSubSources(items map[string]*strat.DataSub) []string {
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func EnsureSeriesRange(ctx context.Context, src DataSource, sub *strat.DataSub, startMS, endMS int64) *errs.Error {
	return EnsureSeriesRangeWithRepo(ctx, orm.DefaultSeriesRepo(), src, sub, startMS, endMS)
}

func EnsureSeriesRangeWithRepo(ctx context.Context, repo orm.SeriesRepo, src DataSource, sub *strat.DataSub, startMS, endMS int64) *errs.Error {
	if startMS >= endMS {
		return nil
	}
	if src == nil {
		return errs.NewMsg(core.ErrBadConfig, "data source is required")
	}
	if repo == nil {
		return errs.NewMsg(core.ErrBadConfig, "series repository is required")
	}
	if sub == nil || sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "data sub exsymbol is required")
	}
	info := src.Info()
	if err := orm.ValidateSeriesInfo(info); err != nil {
		return err
	}
	tf := sub.TimeFrame
	if tf == "" {
		tf = info.TimeFrame
	}
	if tf != info.TimeFrame {
		return errs.NewMsg(core.ErrBadConfig, "sub timeframe %s does not match source timeframe %s", tf, info.TimeFrame)
	}
	if sub.Source != "" && sub.Source != info.Name {
		return errs.NewMsg(core.ErrBadConfig, "sub source %s does not match data source %s", sub.Source, info.Name)
	}
	store := orm.NewSeriesStore(repo)
	rows := 0
	err := store.FillMissing(ctx, info, sub.ExSymbol, startMS, endMS,
		func(ctx context.Context, _ *orm.ExSymbol, gapStartMS, gapEndMS int64) ([]*orm.DataRecord, error) {
			got, err := src.FetchHistory(ctx, sub, gapStartMS, gapEndMS)
			rows += len(got)
			return got, err
		})
	status := DataSourceOpStatus{
		State:     "ok",
		AtMS:      time.Now().UnixMilli(),
		Sid:       sub.ExSymbol.ID,
		TimeFrame: tf,
		StartMS:   startMS,
		EndMS:     endMS,
		Rows:      rows,
	}
	errText := ""
	if err != nil {
		status.State = "error"
		status.Error = err.Short()
		errText = err.Short()
	}
	markDataSourceBackfill(info.Name, status, errText)
	return err
}

func newDataSourceStatus(src DataSource, info *orm.SeriesInfo) *DataSourceStatus {
	version := ""
	if v, ok := src.(DataSourceVersioner); ok {
		version = v.Version()
	}
	return &DataSourceStatus{
		Name:           info.Name,
		Health:         "registered",
		RegisteredAtMS: time.Now().UnixMilli(),
		RegisterSource: callerSource(),
		Version:        version,
		Type:           reflect.TypeOf(src).String(),
		TimeFrame:      info.TimeFrame,
		Table:          info.Binding.Table,
		LastBackfill:   DataSourceOpStatus{State: "idle"},
		Subscription:   DataSourceOpStatus{State: "idle"},
	}
}

func callerSource() string {
	for i := 2; i < 12; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			continue
		}
		fn := runtime.FuncForPC(pc)
		if fn == nil || strings.Contains(fn.Name(), "github.com/banbox/banbot/data.RegisterDataSource") || strings.Contains(fn.Name(), "github.com/banbox/banbot/data.RegisterFuncDataSource") {
			continue
		}
		return fmt.Sprintf("%s:%d", file, line)
	}
	return ""
}

func markDataSourceBackfill(name string, status DataSourceOpStatus, lastErr string) {
	dataSourcesMu.Lock()
	defer dataSourcesMu.Unlock()
	updateDataSourceStatusLocked(name, func(st *DataSourceStatus) {
		st.LastBackfill = status
		st.LastError = dataSourceLastError(st, lastErr)
		st.Health = dataSourceHealth(st)
	})
}

func markDataSourceSubscription(name string, status DataSourceOpStatus, lastErr string) {
	dataSourcesMu.Lock()
	defer dataSourcesMu.Unlock()
	updateDataSourceStatusLocked(name, func(st *DataSourceStatus) {
		st.Subscription = status
		st.LastError = dataSourceLastError(st, lastErr)
		st.Health = dataSourceHealth(st)
	})
}

func updateDataSourceStatusLocked(name string, cb func(*DataSourceStatus)) {
	st := dataSourceSta[name]
	if st == nil {
		return
	}
	cb(st)
}

func dataSourceHealth(st *DataSourceStatus) string {
	if st == nil {
		return "unknown"
	}
	if st.LastBackfill.State == "error" || st.Subscription.State == "error" {
		return "error"
	}
	if st.Subscription.State == "subscribing" {
		return "starting"
	}
	if st.LastBackfill.State == "ok" || st.Subscription.State == "subscribed" {
		return "ok"
	}
	return "registered"
}

func dataSourceLastError(st *DataSourceStatus, latest string) string {
	if latest != "" {
		return latest
	}
	if st == nil {
		return ""
	}
	if st.Subscription.Error != "" {
		return st.Subscription.Error
	}
	return st.LastBackfill.Error
}
