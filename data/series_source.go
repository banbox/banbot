package data

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type DataSink interface {
	Emit(sub *orm.Subscription, rows []*orm.DataRecord) error
}

type DataSource interface {
	Info() *orm.SeriesInfo
	FetchHistory(ctx context.Context, sub *orm.Subscription, startMS, endMS int64) ([]*orm.DataRecord, error)
	SubscribeLive(ctx context.Context, subs []*orm.Subscription, sink DataSink) error
}

type DataSourceVersioner interface {
	Version() string
}

// ObservationWarmupSource resolves an irregular stream's lookback from actual
// observations. It must fail when the requested visible history is incomplete.
// A bar count is never multiplied by an invented event interval.
type ObservationWarmupSource interface {
	WarmupStart(context.Context, *orm.Subscription, int64) (int64, error)
}

func (c *DataSourceCatalog) SubscriptionWarmupStart(ctx context.Context, subs []*orm.Subscription, anchorMS int64) (int64, error) {
	start := anchorMS
	for _, sub := range subs {
		if sub == nil {
			continue
		}
		var at int64
		var err error
		if sub.TimeFrame == "event" && sub.WarmupNum > 0 {
			source, ok := c.GetDataSource(sub.Source).(ObservationWarmupSource)
			if !ok {
				return 0, fmt.Errorf("source %s: event count warmup requires an observation-based source bootstrap", sub.Source)
			}
			at, err = source.WarmupStart(ctx, sub, anchorMS)
			if err == nil && (at < 0 || at >= anchorMS) {
				err = fmt.Errorf("source %s: invalid observation warmup start", sub.Source)
			}
		} else {
			at, err = orm.SubscriptionWarmupStart(*sub, anchorMS)
		}
		if err != nil {
			return 0, err
		}
		start = min(start, at)
	}
	return start, nil
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
	Catalog    *DataSourceCatalog
	Repo       orm.SeriesRepo
	Sink       DataSink
	EnsureFunc func(ctx context.Context, plan *SeriesPlan) *errs.Error
	ActivateFn func(ctx context.Context, subs []*orm.Subscription, sink DataSink) ([]*orm.Subscription, error)

	active map[string][]string
}

func NewSeriesRuntime(sink DataSink) *SeriesRuntime {
	return &SeriesRuntime{Catalog: legacyDataSourceCatalog, Sink: sink}
}

func NewSeriesRuntimeWithCatalog(catalog *DataSourceCatalog, sink DataSink) *SeriesRuntime {
	return &SeriesRuntime{Catalog: catalog, Sink: sink}
}

func NewSeriesRuntimeWithRuntimeDeps(deps *RuntimeDeps, sink DataSink) *SeriesRuntime {
	if deps == nil {
		return NewSeriesRuntime(sink)
	}
	return NewSeriesRuntimeWithCatalog(deps.Catalog, sink)
}

func (r *SeriesRuntime) Plan(jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	return newSeriesPlan(r.catalog(), jobs, warmupAnchorMS, endMS)
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
			return plan.ensure(ctx, r.catalog(), r.repo())
		}
	}
	if err := ensureFn(ctx, plan); err != nil {
		return err
	}
	return nil
}

func (r *SeriesRuntime) ActivateNew(ctx context.Context, subs []*orm.Subscription) error {
	newSubs := r.newSubs(subs)
	if len(newSubs) == 0 {
		return nil
	}
	activateFn := r.ActivateFn
	if activateFn == nil {
		activateFn = func(ctx context.Context, subs []*orm.Subscription, sink DataSink) ([]*orm.Subscription, error) {
			return r.catalog().ActivateDataSources(ctx, subs, sink)
		}
	}
	activated, err := activateFn(ctx, newSubs, r.Sink)
	r.markActive(activated)
	if err != nil {
		return wrapBootstrapActivateErr(newSubs, err)
	}
	return nil
}

func (r *SeriesRuntime) catalog() *DataSourceCatalog {
	if r == nil {
		return nil
	}
	return r.Catalog
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

func (r *SeriesRuntime) newSubs(subs []*orm.Subscription) []*orm.Subscription {
	if len(subs) == 0 {
		return nil
	}
	if r.active == nil {
		r.active = make(map[string][]string)
	}
	items := make([]*orm.Subscription, 0, len(subs))
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

func (r *SeriesRuntime) markActive(subs []*orm.Subscription) {
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

func runtimeSubKey(sub *orm.Subscription) (string, bool) {
	if sub == nil || sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return "", false
	}
	source := orm.NormalizeSeriesSource(sub.Source)
	if source == orm.SeriesSourceKline {
		return "", false
	}
	return sub.Key().String(), true
}

type SeriesPlan struct {
	Subs    []*orm.Subscription
	StartMS int64
	EndMS   int64
}

type ThirdPartySeriesBootstrap = SeriesPlan

func NewSeriesPlan(jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	return newSeriesPlan(legacyDataSourceCatalog, jobs, warmupAnchorMS, endMS)
}

func NewSeriesPlanWithCatalog(catalog *DataSourceCatalog, jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	return newSeriesPlan(catalog, jobs, warmupAnchorMS, endMS)
}

func newSeriesPlan(catalog *DataSourceCatalog, jobs []*strat.StratJob, warmupAnchorMS, endMS int64) (*SeriesPlan, error) {
	if warmupAnchorMS <= 0 {
		return nil, fmt.Errorf("bootstrap collect phase=collect: warmup anchor is required")
	}
	if endMS <= 0 {
		return nil, fmt.Errorf("bootstrap collect phase=collect: end time is required")
	}
	if warmupAnchorMS > endMS {
		return nil, fmt.Errorf("bootstrap collect phase=collect: warmup anchor must not exceed end time")
	}
	subs, err := collectRuntimeDataSubs(catalog, jobs)
	if err != nil {
		return nil, err
	}
	startMS := warmupAnchorMS
	if len(subs) > 0 {
		startMS, err = catalog.SubscriptionWarmupStart(context.Background(), subs, warmupAnchorMS)
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

func (p *SeriesPlan) ensure(ctx context.Context, catalog *DataSourceCatalog, repo orm.SeriesRepo) *errs.Error {
	if p == nil {
		return nil
	}
	return ensureSeriesSubsRange(catalog, ctx, repo, p.Subs, p.StartMS, p.EndMS)
}

func (p *SeriesPlan) Activate(ctx context.Context, sink DataSink) ([]*orm.Subscription, error) {
	if p == nil {
		return nil, nil
	}
	return ActivateDataSources(ctx, p.Subs, sink)
}

var legacyDataSourceCatalog = NewDataSourceCatalog()

func LegacyDataSourceCatalog() *DataSourceCatalog {
	return legacyDataSourceCatalog
}

func RegisterDataSource(src DataSource) error {
	// Direct registration is retained for legacy callers only. Explicit Runtime
	// construction rejects it because an arbitrary interface value cannot be
	// cloned safely; register a factory for runtime-capable providers instead.
	return legacyDataSourceCatalog.RegisterDataSource(src)
}

func RegisterDataSourceFactory(name string, factory DataSourceFactory) error {
	return legacyDataSourceCatalog.RegisterDataSourceFactory(name, factory)
}

func GetDataSource(name string) DataSource {
	return legacyDataSourceCatalog.GetDataSource(name)
}

func ListDataSources() []string {
	return legacyDataSourceCatalog.ListDataSources()
}

func ListDataSourceStatus() []*DataSourceStatus {
	return legacyDataSourceCatalog.ListDataSourceStatus()
}

func ActivateDataSources(ctx context.Context, subs []*orm.Subscription, sink DataSink) ([]*orm.Subscription, error) {
	return legacyDataSourceCatalog.ActivateDataSources(ctx, subs, sink)
}

func (c *DataSourceCatalog) ActivateDataSources(ctx context.Context, subs []*orm.Subscription, sink DataSink) ([]*orm.Subscription, error) {
	if len(subs) == 0 {
		return nil, nil
	}
	if sink == nil {
		return nil, fmt.Errorf("data sink is required")
	}
	seen := make(map[string]*orm.Subscription)
	for _, sub := range subs {
		normalized, err := validateActivationSub(sub)
		if err != nil {
			return nil, err
		}
		src := c.GetDataSource(normalized.Source)
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
		if len(normalized.Fields) > 0 {
			normalized.Projection = orm.ProjectionSelected
		}
		mergeDataSub(seen, normalized)
	}
	grouped := make(map[string][]*orm.Subscription)
	for _, sub := range sortedDataSubs(seen) {
		grouped[sub.Source] = append(grouped[sub.Source], sub)
	}
	activated := make([]*orm.Subscription, 0, len(seen))
	for _, name := range sortedSourceNames(grouped) {
		c.markDataSourceSubscription(name, DataSourceOpStatus{
			State: "subscribing",
			AtMS:  time.Now().UnixMilli(),
			Subs:  len(grouped[name]),
		}, "")
		if err := c.GetDataSource(name).SubscribeLive(ctx, grouped[name], sink); err != nil {
			c.markDataSourceSubscription(name, DataSourceOpStatus{
				State: "error",
				Error: err.Error(),
				AtMS:  time.Now().UnixMilli(),
				Subs:  len(grouped[name]),
			}, err.Error())
			return activated, fmt.Errorf("activate data source %q: %w", name, err)
		}
		activated = append(activated, grouped[name]...)
		c.markDataSourceSubscription(name, DataSourceOpStatus{
			State: "subscribed",
			AtMS:  time.Now().UnixMilli(),
			Subs:  len(grouped[name]),
		}, "")
	}
	return activated, nil
}

func validateActivationSub(sub *orm.Subscription) (*orm.Subscription, error) {
	if sub == nil {
		return nil, fmt.Errorf("data sub is required")
	}
	normalized, err := orm.NormalizeSubscription(*sub)
	return &normalized, err
}

func validateBootstrapSub(sub *orm.Subscription) (*orm.Subscription, error) {
	return validateBootstrapSubWithCatalog(legacyDataSourceCatalog, sub)
}

func validateBootstrapSubWithCatalog(catalog *DataSourceCatalog, sub *orm.Subscription) (*orm.Subscription, error) {
	normalized, err := validateActivationSub(sub)
	if err != nil {
		return nil, err
	}
	if normalized.Source == "" {
		return nil, fmt.Errorf("data sub source is required")
	}
	if normalized.Source != orm.SeriesSourceKline {
		if src := catalog.GetDataSource(normalized.Source); src != nil {
			if normalized.Frequency == orm.FrequencyEvent && normalized.WarmupNum > 0 {
				if _, ok := src.(ObservationWarmupSource); !ok {
					return nil, fmt.Errorf("source %s: event count warmup requires an observation-based source bootstrap", normalized.Source)
				}
			}
			info := src.Info()
			if err := orm.ValidateSeriesInfo(info); err != nil {
				return nil, err
			}
			if normalized.Projection == orm.ProjectionAll {
				normalized.Fields = nil
			}
			if err := normalizeDataSubFields(info, normalized); err != nil {
				return nil, err
			}
		}
	} else {
		if normalized.Projection == orm.ProjectionAll {
			return nil, fmt.Errorf("all kline fields require an explicit source schema")
		}
		normalized.Fields = orm.MergeSeriesFields(orm.NormalizeSeriesFields(normalized.Source, normalized.Fields), normalized.SeriesFields)
	}
	// The projection has been expanded before merging with other consumers.
	if len(normalized.Fields) > 0 {
		normalized.Projection = orm.ProjectionSelected
	}
	return normalized, nil
}

func sortedSourceNames(grouped map[string][]*orm.Subscription) []string {
	items := make([]string, 0, len(grouped))
	for name := range grouped {
		items = append(items, name)
	}
	sort.Strings(items)
	return items
}

func CollectRuntimeDataSubs(jobs []*strat.StratJob) ([]*orm.Subscription, error) {
	return collectRuntimeDataSubs(legacyDataSourceCatalog, jobs)
}

func (c *DataSourceCatalog) CollectRuntimeDataSubs(jobs []*strat.StratJob) ([]*orm.Subscription, error) {
	return collectRuntimeDataSubs(c, jobs)
}

func collectRuntimeDataSubs(catalog *DataSourceCatalog, jobs []*strat.StratJob) ([]*orm.Subscription, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	seen := make(map[string]*orm.Subscription)
	for _, job := range jobs {
		if job == nil {
			continue
		}
		for _, sub := range strat.CollectDataSubs(job) {
			normalized, err := validateBootstrapSubWithCatalog(catalog, sub)
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

func mergeDataSub(seen map[string]*orm.Subscription, sub *orm.Subscription) {
	if seen == nil || sub == nil || sub.ExSymbol == nil {
		return
	}
	key := sub.Key().String()
	if existing, ok := seen[key]; ok {
		if sub.WarmupNum > existing.WarmupNum {
			existing.WarmupNum = sub.WarmupNum
		}
		existing.Fields = orm.MergeSeriesFields(existing.Fields, sub.Fields)
		existing.SeriesFields = orm.MergeSeriesFields(existing.SeriesFields, sub.SeriesFields)
		return
	}
	cp := *sub
	cp.Fields = append([]string(nil), sub.Fields...)
	cp.SeriesFields = append([]string(nil), sub.SeriesFields...)
	seen[key] = &cp
}

func EnsureThirdPartySeriesRange(ctx context.Context, repo orm.SeriesRepo, jobs []*strat.StratJob, startMS, endMS int64) ([]*orm.Subscription, *errs.Error) {
	return EnsureRuntimeSeriesRange(ctx, repo, jobs, startMS, endMS)
}

func EnsureRuntimeSeriesRange(ctx context.Context, repo orm.SeriesRepo, jobs []*strat.StratJob, startMS, endMS int64) ([]*orm.Subscription, *errs.Error) {
	return ensureRuntimeSeriesRange(legacyDataSourceCatalog, ctx, repo, jobs, startMS, endMS)
}

func (c *DataSourceCatalog) EnsureRuntimeSeriesRange(ctx context.Context, repo orm.SeriesRepo, jobs []*strat.StratJob, startMS, endMS int64) ([]*orm.Subscription, *errs.Error) {
	return ensureRuntimeSeriesRange(c, ctx, repo, jobs, startMS, endMS)
}

func ensureRuntimeSeriesRange(catalog *DataSourceCatalog, ctx context.Context, repo orm.SeriesRepo, jobs []*strat.StratJob, startMS, endMS int64) ([]*orm.Subscription, *errs.Error) {
	if startMS >= endMS {
		return nil, nil
	}
	subs, err := collectRuntimeDataSubs(catalog, jobs)
	if err != nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "%v", err)
	}
	if err := ensureSeriesSubsRange(catalog, ctx, repo, subs, startMS, endMS); err != nil {
		return nil, err
	}
	return subs, nil
}

func EnsureThirdPartySeriesSubsRange(ctx context.Context, repo orm.SeriesRepo, subs []*orm.Subscription, startMS, endMS int64) *errs.Error {
	return EnsureSeriesSubsRange(ctx, repo, subs, startMS, endMS)
}

func EnsureSeriesSubsRange(ctx context.Context, repo orm.SeriesRepo, subs []*orm.Subscription, startMS, endMS int64) *errs.Error {
	return ensureSeriesSubsRange(legacyDataSourceCatalog, ctx, repo, subs, startMS, endMS)
}

func (c *DataSourceCatalog) EnsureSeriesSubsRange(ctx context.Context, repo orm.SeriesRepo, subs []*orm.Subscription, startMS, endMS int64) *errs.Error {
	return ensureSeriesSubsRange(c, ctx, repo, subs, startMS, endMS)
}

func ensureSeriesSubsRange(catalog *DataSourceCatalog, ctx context.Context, repo orm.SeriesRepo, subs []*orm.Subscription, startMS, endMS int64) *errs.Error {
	if startMS >= endMS {
		return nil
	}
	for _, sub := range subs {
		normalized, err := validateBootstrapSubWithCatalog(catalog, sub)
		if err != nil {
			return errs.NewMsg(core.ErrBadConfig, "bootstrap collect: %v", err)
		}
		if normalized.Source == orm.SeriesSourceKline {
			continue
		}
		src := catalog.GetDataSource(normalized.Source)
		if src == nil {
			return errs.NewMsg(core.ErrBadConfig,
				"bootstrap ensure source=%s sid=%d tf=%s phase=ensure: data source is not registered",
				normalized.Source, normalized.ExSymbol.ID, normalized.TimeFrame)
		}
		if err := ensureSeriesRangeWithRepo(catalog, ctx, repoOrDefault(repo), src, normalized, startMS, endMS); err != nil {
			return wrapBootstrapEnsureErr(normalized, err)
		}
	}
	return nil
}

func sortedDataSubs(seen map[string]*orm.Subscription) []*orm.Subscription {
	if len(seen) == 0 {
		return nil
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]*orm.Subscription, 0, len(keys))
	for _, key := range keys {
		items = append(items, seen[key])
	}
	return items
}

func ThirdPartyWarmupStart(subs []*orm.Subscription, endMS int64) (int64, error) {
	if len(subs) == 0 {
		return endMS, nil
	}
	startMS := endMS
	for _, sub := range subs {
		if sub == nil {
			continue
		}
		candidate, err := orm.SubscriptionWarmupStart(*sub, endMS)
		if err != nil {
			return 0, fmt.Errorf("bootstrap collect source=%s sid=%d tf=%s phase=collect: %w", sub.Source, bootstrapSubSID(sub), sub.TimeFrame, err)
		}
		if candidate < startMS {
			startMS = candidate
		}
	}
	return startMS, nil
}

func bootstrapSubSID(sub *orm.Subscription) int32 {
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

func wrapBootstrapEnsureErr(sub *orm.Subscription, err *errs.Error) *errs.Error {
	if sub == nil || sub.ExSymbol == nil {
		return err
	}
	return errs.NewMsg(err.Code,
		"bootstrap ensure source=%s sid=%d tf=%s phase=ensure: %s",
		sub.Source, sub.ExSymbol.ID, sub.TimeFrame, err.Short())
}

func wrapBootstrapActivateErr(subs []*orm.Subscription, err error) error {
	if err == nil || len(subs) == 0 {
		return err
	}
	bySource := make(map[string]*orm.Subscription)
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

func sortedSubSources(items map[string]*orm.Subscription) []string {
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func EnsureSeriesRange(ctx context.Context, src DataSource, sub *orm.Subscription, startMS, endMS int64) *errs.Error {
	return ensureSeriesRangeWithRepo(legacyDataSourceCatalog, ctx, orm.DefaultSeriesRepo(), src, sub, startMS, endMS)
}

func EnsureSeriesRangeWithRepo(ctx context.Context, repo orm.SeriesRepo, src DataSource, sub *orm.Subscription, startMS, endMS int64) *errs.Error {
	return ensureSeriesRangeWithRepo(legacyDataSourceCatalog, ctx, repo, src, sub, startMS, endMS)
}

func ensureSeriesRangeWithRepo(catalog *DataSourceCatalog, ctx context.Context, repo orm.SeriesRepo, src DataSource, sub *orm.Subscription, startMS, endMS int64) *errs.Error {
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
	if _, paged := src.(PagedHistorySource); paged || orm.SeriesReadByteLimit(ctx) > 0 {
		return ensurePagedSeriesRange(catalog, ctx, store, src, sub, startMS, endMS)
	}
	if tf == "event" {
		// Sparse rows do not describe coverage between observations. Fetch the
		// declared interval directly; do not manufacture bar coverage holes.
		rows, err := src.FetchHistory(ctx, sub, startMS, endMS)
		if err != nil {
			return errs.New(core.ErrDbReadFail, err)
		}
		for _, row := range rows {
			if row != nil && (row.TimeMS < startMS || row.TimeMS >= endMS) {
				return errs.NewMsg(core.ErrBadConfig, "event history source returned an observation outside its requested range")
			}
		}
		return store.WriteBatch(ctx, info, sub.ExSymbol, rows)
	}
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
	if catalog != nil {
		catalog.markDataSourceBackfill(info.Name, status, errText)
	}
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
		if fn == nil || strings.Contains(fn.Name(), "github.com/banbox/banbot/data.RegisterDataSource") ||
			strings.Contains(fn.Name(), "DataSourceCatalog).RegisterDataSource") || strings.Contains(fn.Name(), "github.com/banbox/banbot/data.RegisterFuncDataSource") {
			continue
		}
		return fmt.Sprintf("%s:%d", file, line)
	}
	return ""
}

func markDataSourceBackfill(name string, status DataSourceOpStatus, lastErr string) {
	legacyDataSourceCatalog.markDataSourceBackfill(name, status, lastErr)
}

func markDataSourceSubscription(name string, status DataSourceOpStatus, lastErr string) {
	legacyDataSourceCatalog.markDataSourceSubscription(name, status, lastErr)
}

func updateDataSourceStatusLocked(statuses map[string]*DataSourceStatus, name string, cb func(*DataSourceStatus)) {
	st := statuses[name]
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
