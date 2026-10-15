package data

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"sync"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/utils"
)

// Cleanup plan: compile each stream union once, preflight observation warmup
// before installing feeders, and own stop/join while preserving source readers.
type SubscriptionRequest struct {
	Subscription Subscription
	Consumer     string
	Required     bool
	MaxAgeMS     int64
}

type SubscriptionPlanOptions struct {
	Namespace              string
	AnchorMS, EndMS        int64
	PageRows, PrefetchRows int
	PageBytes              int64
	RequireManagedLive     bool
}

type SubscriptionConsumer struct {
	Name      string
	Required  bool
	MaxAgeMS  int64
	WarmupNum int
}
type PlannedStream struct {
	Subscription              Subscription
	Consumers                 []SubscriptionConsumer
	WarmupStartMS             int64
	EstimatedRows             int64
	EstimateKnown             bool
	SourceVersion, SchemaHash string
}

// SubscriptionPlan is immutable; exported views are independent copies.
type SubscriptionPlan struct {
	catalog  *DataSourceCatalog
	options  SubscriptionPlanOptions
	streams  []PlannedStream
	degraded []SubscriptionDegradation
}

// SubscriptionDegradation identifies the consumers affected by an optional
// source failure. It never appears on the installation's fatal Errors channel.
type SubscriptionDegradation struct {
	Source    string
	Consumers []SubscriptionConsumer
	Err       error
}

func streamRequired(stream PlannedStream) bool {
	for _, consumer := range stream.Consumers {
		if consumer.Required {
			return true
		}
	}
	return false
}

func mergeSubscriptionConsumer(consumers []SubscriptionConsumer, consumer SubscriptionConsumer) []SubscriptionConsumer {
	for i, previous := range consumers {
		if previous.Name != consumer.Name {
			continue
		}
		consumers[i].Required = previous.Required || consumer.Required
		consumers[i].WarmupNum = max(previous.WarmupNum, consumer.WarmupNum)
		if previous.MaxAgeMS == 0 || consumer.MaxAgeMS > 0 && consumer.MaxAgeMS < previous.MaxAgeMS {
			consumers[i].MaxAgeMS = consumer.MaxAgeMS
		}
		return consumers
	}
	return append(consumers, consumer)
}

func (p *SubscriptionPlan) Options() SubscriptionPlanOptions {
	if p == nil {
		return SubscriptionPlanOptions{}
	}
	return p.options
}

// SubscriptionBudgetReport describes logical input pages, not physical heap
// or RSS. Physical query branches, decoder buffers, driver allocations and
// retained warmup/aggregation/factor state require separate measurement.
type SubscriptionBudgetReport struct {
	StreamCount             int      `json:"stream_count"`
	PageRows                int      `json:"page_rows"`
	PrefetchRows            int      `json:"prefetch_rows"`
	ActivePageRowsEstimate  int64    `json:"active_page_rows_estimate"`
	DeclaredWarmupRows      int64    `json:"declared_warmup_rows"`
	PageBytes               int64    `json:"page_bytes"`
	ActivePageBytesEstimate int64    `json:"active_page_bytes_estimate,omitempty"`
	Scope                   string   `json:"scope"`
	Exclusions              []string `json:"exclusions"`
}

func (p *SubscriptionPlan) BudgetReport() SubscriptionBudgetReport {
	report := SubscriptionBudgetReport{Scope: "decoded logical input pages; estimates are not a process memory limit", Exclusions: []string{"physical query branches", "source transport/decoder and database driver buffers", "allocator/map spare capacity", "warmup/aggregation/factor retained state"}}
	if p == nil {
		return report
	}
	report.StreamCount, report.PageRows, report.PrefetchRows, report.PageBytes = len(p.streams), p.options.PageRows, p.options.PrefetchRows, p.options.PageBytes
	streams := int64(report.StreamCount)
	if streams > 0 {
		if int64(report.PageRows) > math.MaxInt64/streams {
			report.ActivePageRowsEstimate = math.MaxInt64
		} else {
			report.ActivePageRowsEstimate = int64(report.PageRows) * streams
		}
		if report.PageBytes > math.MaxInt64/streams {
			report.ActivePageBytesEstimate = math.MaxInt64
		} else {
			report.ActivePageBytesEstimate = report.PageBytes * streams
		}
	}
	for _, stream := range p.streams {
		warm := int64(stream.Subscription.WarmupNum)
		if warm > math.MaxInt64-report.DeclaredWarmupRows {
			report.DeclaredWarmupRows = math.MaxInt64
		} else {
			report.DeclaredWarmupRows += warm
		}
	}
	return report
}
func (p *SubscriptionPlan) Streams() []PlannedStream {
	if p == nil {
		return nil
	}
	streams := slices.Clone(p.streams)
	for i := range streams {
		streams[i].Subscription = cloneSubscription(streams[i].Subscription)
		streams[i].Consumers = slices.Clone(streams[i].Consumers)
	}
	return streams
}
func (p *SubscriptionPlan) Subscriptions() []Subscription {
	if p == nil {
		return nil
	}
	result := make([]Subscription, len(p.streams))
	for i, stream := range p.streams {
		result[i] = cloneSubscription(stream.Subscription)
	}
	return result
}

type SubscriptionSourceMetadata struct{ Version, SchemaHash string }

func (p *SubscriptionPlan) SourceMetadata() map[string]SubscriptionSourceMetadata {
	result := map[string]SubscriptionSourceMetadata{}
	if p != nil {
		for _, stream := range p.streams {
			result[stream.Subscription.Source] = SubscriptionSourceMetadata{stream.SourceVersion, stream.SchemaHash}
		}
	}
	return result
}
func cloneSubscription(sub Subscription) Subscription {
	sub.Fields, sub.SeriesFields = slices.Clone(sub.Fields), slices.Clone(sub.SeriesFields)
	if sub.ExSymbol != nil {
		copySymbol := *sub.ExSymbol
		sub.ExSymbol = &copySymbol
	}
	return sub
}

func sourcePlanIdentity(source DataSource) (version, schema string, errorValue error) {
	if source == nil || source.Info() == nil {
		return "", "", fmt.Errorf("source metadata is required")
	}
	if versioner, ok := source.(DataSourceVersioner); ok {
		version = versioner.Version()
	}
	raw, err := json.Marshal(source.Info())
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(raw)
	return version, hex.EncodeToString(digest[:]), nil
}

func (c *DataSourceCatalog) CompileSubscriptionPlan(ctx context.Context, requests []SubscriptionRequest, options SubscriptionPlanOptions) (*SubscriptionPlan, error) {
	if c == nil {
		return nil, fmt.Errorf("subscription catalog is required")
	}
	if ctx == nil {
		return nil, fmt.Errorf("subscription context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.PageRows < 0 || options.PrefetchRows < 0 || options.PageBytes < 0 || options.EndMS < options.AnchorMS {
		return nil, fmt.Errorf("invalid subscription range or page budget")
	}
	if options.PageRows == 0 {
		options.PageRows = 20000
	}
	seen := make(map[string]*PlannedStream)
	plan := &SubscriptionPlan{catalog: c, options: options}
	for _, request := range requests {
		if request.Consumer == "" || request.MaxAgeMS < 0 {
			return nil, fmt.Errorf("subscription consumer and nonnegative freshness are required")
		}
		items, err := c.NormalizeSubscriptions([]Subscription{request.Subscription})
		if err != nil {
			if request.Required {
				return nil, err
			}
			plan.degraded = append(plan.degraded, SubscriptionDegradation{Source: request.Subscription.Source, Consumers: []SubscriptionConsumer{{Name: request.Consumer, WarmupNum: request.Subscription.WarmupNum, MaxAgeMS: request.MaxAgeMS}}, Err: err})
			continue
		}
		sub := items[0]
		key := sub.Key().String()
		stream := seen[key]
		if stream == nil {
			stream = &PlannedStream{Subscription: cloneSubscription(sub)}
			seen[key] = stream
		} else {
			if !reflect.DeepEqual(stream.Subscription.ExSymbol, sub.ExSymbol) {
				return nil, fmt.Errorf("stream %s has conflicting SID identity", key)
			}
			stream.Subscription.WarmupNum = max(stream.Subscription.WarmupNum, sub.WarmupNum)
			stream.Subscription.Fields = orm.MergeSeriesFields(stream.Subscription.Fields, sub.Fields)
			stream.Subscription.SeriesFields = orm.MergeSeriesFields(stream.Subscription.SeriesFields, sub.SeriesFields)
		}
		consumer := SubscriptionConsumer{Name: request.Consumer, Required: request.Required, MaxAgeMS: request.MaxAgeMS, WarmupNum: sub.WarmupNum}
		stream.Consumers = mergeSubscriptionConsumer(stream.Consumers, consumer)
	}
	if options.PrefetchRows > 0 && len(seen) > 0 {
		if len(seen) > options.PrefetchRows {
			return nil, fmt.Errorf("prefetch budget cannot hold one row per stream")
		}
		options.PageRows = min(options.PageRows, options.PrefetchRows/len(seen))
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	plan.options = options
	for _, key := range keys {
		stream := seen[key]
		start, err := c.SubscriptionWarmupStart(ctx, []*orm.Subscription{&stream.Subscription}, options.AnchorMS)
		if err != nil {
			if streamRequired(*stream) {
				return nil, err
			}
			plan.degraded = append(plan.degraded, SubscriptionDegradation{Source: stream.Subscription.Source, Consumers: slices.Clone(stream.Consumers), Err: err})
			continue
		}
		stream.WarmupStartMS = start
		if stream.Subscription.Frequency == orm.FrequencyBar {
			seconds, _ := utils.TFToSecSafe(stream.Subscription.TimeFrame)
			step := int64(seconds) * 1000
			if start >= 0 && options.EndMS >= start {
				stream.EstimatedRows = (options.EndMS - start) / step
				stream.EstimateKnown = true
			}
		}
		if stream.Subscription.Source != orm.SeriesSourceKline {
			stream.SourceVersion, stream.SchemaHash, err = sourcePlanIdentity(c.GetDataSource(stream.Subscription.Source))
			if err != nil {
				if streamRequired(*stream) {
					return nil, err
				}
				plan.degraded = append(plan.degraded, SubscriptionDegradation{Source: stream.Subscription.Source, Consumers: slices.Clone(stream.Consumers), Err: err})
				continue
			}
		}
		sort.Slice(stream.Consumers, func(i, j int) bool { return stream.Consumers[i].Name < stream.Consumers[j].Name })
		plan.streams = append(plan.streams, *stream)
	}
	return plan, nil
}

func (p *SubscriptionPlan) validate(catalog *DataSourceCatalog) error {
	if p == nil || p.catalog != catalog {
		return fmt.Errorf("subscription plan belongs to another source catalog")
	}
	for _, stream := range p.streams {
		if stream.Subscription.Source == orm.SeriesSourceKline {
			continue
		}
		version, schema, err := sourcePlanIdentity(catalog.GetDataSource(stream.Subscription.Source))
		if err != nil {
			return err
		}
		if version != stream.SourceVersion || schema != stream.SchemaHash {
			return fmt.Errorf("source %s changed after plan compilation", stream.Subscription.Source)
		}
	}
	return nil
}

func (p *SubscriptionPlan) Validate() error {
	if p == nil {
		return fmt.Errorf("subscription plan is required")
	}
	return p.validate(p.catalog)
}

// Bootstrap reads/writes source history using each stream's own observation
// lookback. Kline coverage and aggregation stay with the specialized provider.
func (p *SubscriptionPlan) Bootstrap(ctx context.Context, repo orm.SeriesRepo) error {
	if ctx == nil {
		return fmt.Errorf("subscription context is required")
	}
	if err := p.Validate(); err != nil {
		return err
	}
	ctx = orm.WithSeriesReadByteLimit(ctx, p.options.PageBytes)
	ctx = context.WithValue(ctx, historyPageRowsKey{}, p.options.PageRows)
	for _, stream := range p.streams {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := p.options.EndMS
		if stream.Subscription.Frequency == orm.FrequencyEvent && end < math.MaxInt64 {
			// Replay includes the final event timestamp; history fetch ranges use
			// an exclusive upper bound.
			end++
		}
		if stream.Subscription.Source == orm.SeriesSourceKline || stream.WarmupStartMS >= end {
			continue
		}
		if err := p.catalog.EnsureSeriesSubsRange(ctx, repo, []*orm.Subscription{&stream.Subscription}, stream.WarmupStartMS, end); err != nil {
			return err
		}
	}
	return nil
}

// LiveSourceSubscription explicitly owns producers that outlive SubscribeLive.
// Stop seals intake without waiting; Join runs from the owner after Stop.
type LiveSourceSubscription interface {
	Stop()
	Join() error
}

// LiveSourceErrors is implemented by handles with asynchronous producer errors.
// The installation monitors it and freezes intake before reporting failure.
type LiveSourceErrors interface{ Errors() <-chan error }
type ManagedLiveSource interface {
	// Each call returns an independently owned subscription, or rejects the
	// additional call. Stopping one handle must not stop another subscription.
	SubscribeManaged(context.Context, []*orm.Subscription, DataSink) (LiveSourceSubscription, error)
}

// LiveWarmupSink consumes verified startup history before any live emission.
// Warmup must advance history state without admitting historical targets.
type LiveWarmupSink interface {
	Warmup(*orm.Subscription, []*orm.DataRecord) error
}

// LiveWarmupReadySink validates the complete consumer state after all side
// sources and kline feeders have finished historical startup callbacks.
type LiveWarmupReadySink interface{ WarmupReady(anchorMS int64) error }

func (s *SubscriptionInstallation) warmupReady(anchorMS int64) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if ready, ok := s.sink.(LiveWarmupReadySink); ok {
		return ready.WarmupReady(anchorMS)
	}
	return nil
}

// Sources may change metadata while preparing handles or reading history.
// Optional failed groups remain degraded; required identities must still match.
func (s *SubscriptionInstallation) validateSources(plan *SubscriptionPlan) error {
	for _, stream := range plan.streams {
		name := stream.Subscription.Source
		if name == orm.SeriesSourceKline {
			continue
		}
		group := s.groups[name]
		s.mu.Lock()
		disabled := group.disabled
		s.mu.Unlock()
		if disabled {
			continue
		}
		version, schema, err := sourcePlanIdentity(plan.catalog.GetDataSource(name))
		if err == nil && (version != stream.SourceVersion || schema != stream.SchemaHash) {
			err = fmt.Errorf("source %s changed after plan compilation", name)
		}
		if err != nil {
			if group.required {
				return err
			}
			s.reportGroup(group, err)
		}
	}
	return s.ctx.Err()
}

func (p *SubscriptionPlan) warmupLive(installation *SubscriptionInstallation) error {
	for _, stream := range p.streams {
		sub := stream.Subscription
		if sub.Source == orm.SeriesSourceKline || sub.WarmupNum == 0 {
			continue
		}
		group := installation.groups[sub.Source]
		installation.mu.Lock()
		disabled := group.disabled
		installation.mu.Unlock()
		if disabled {
			continue
		}
		// Live clocks need not be on a bar boundary. Fetch the full closed
		// observations preceding that boundary, not a partial first interval.
		historyEnd := p.options.AnchorMS
		if sub.Frequency == orm.FrequencyBar {
			seconds, _ := utils.TFToSecSafe(sub.TimeFrame)
			step := int64(seconds) * 1000
			stream.WarmupStartMS = p.options.AnchorMS/step*step - int64(sub.WarmupNum)*step
			historyEnd = p.options.AnchorMS / step * step
		}
		var rows []*orm.DataRecord
		readCtx := orm.WithSeriesReadByteLimit(group.ctx, p.options.PageBytes)
		err := ReadSourceHistory(readCtx, p.catalog.GetDataSource(sub.Source), &sub, stream.WarmupStartMS, historyEnd, p.options.PageRows, func(page []*orm.DataRecord) error {
			page = slices.Clone(page)
			for _, row := range page {
				if row == nil || row.Sid != sub.ExSymbol.ID || row.TimeMS < stream.WarmupStartMS || row.TimeMS >= historyEnd || row.EndMS > p.options.AnchorMS || !row.Closed {
					return fmt.Errorf("unavailable, foreign or unclosed historical observation")
				}
			}
			sort.SliceStable(page, func(i, j int) bool { return page[i].TimeMS < page[j].TimeMS })
			if len(page) >= sub.WarmupNum {
				rows = cloneStartupRows(page[len(page)-sub.WarmupNum:])
			} else {
				rows = append(rows, cloneStartupRows(page)...)
				if len(rows) > sub.WarmupNum {
					rows = slices.Clone(rows[len(rows)-sub.WarmupNum:])
				}
			}
			return nil
		})
		if err == nil {
			rows, err = verifiedLiveWarmup(stream, rows, p.options.AnchorMS)
		}
		if err == nil {
			warmer, ok := installation.sink.(LiveWarmupSink)
			if !ok {
				err = fmt.Errorf("source %s requires a historical warmup sink", sub.Source)
			} else {
				err = warmer.Warmup(&sub, cloneStartupRows(rows))
			}
		}
		if err != nil {
			err = fmt.Errorf("source %s warmup: %w", sub.Source, err)
			installation.reportGroup(group, err)
			if group.required {
				return err
			}
			_ = group.join()
		}
	}
	return installation.ctx.Err()
}

func verifiedLiveWarmup(stream PlannedStream, rows []*orm.DataRecord, anchor int64) ([]*orm.DataRecord, error) {
	sub := stream.Subscription
	rows = slices.Clone(rows)
	for _, row := range rows {
		if row == nil || row.Sid != sub.ExSymbol.ID || row.TimeMS < stream.WarmupStartMS || row.TimeMS >= anchor || row.EndMS > anchor || !row.Closed {
			return nil, fmt.Errorf("unavailable, foreign or unclosed historical observation")
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].TimeMS < rows[j].TimeMS })
	for i := 1; i < len(rows); i++ {
		if rows[i].TimeMS == rows[i-1].TimeMS {
			return nil, fmt.Errorf("duplicate historical observation requires reconciliation")
		}
	}
	if len(rows) < sub.WarmupNum {
		return nil, fmt.Errorf("history incomplete: got %d observations, need %d", len(rows), sub.WarmupNum)
	}
	rows = rows[len(rows)-sub.WarmupNum:]
	if sub.Frequency == orm.FrequencyBar {
		seconds, err := utils.TFToSecSafe(sub.TimeFrame)
		if err != nil {
			return nil, err
		}
		step := int64(seconds) * 1000
		for i, row := range rows {
			if row.EndMS != row.TimeMS+step || i > 0 && row.TimeMS != rows[i-1].EndMS {
				return nil, fmt.Errorf("history has incomplete bar coverage")
			}
		}
		if rows[len(rows)-1].EndMS != anchor/step*step {
			return nil, fmt.Errorf("history does not reach the startup boundary")
		}
	}
	return rows, nil
}

type liveSourceGroup struct {
	installation *SubscriptionInstallation
	name         string
	consumers    []SubscriptionConsumer
	streams      map[orm.StreamKey]bool
	required     bool
	ctx          context.Context
	cancel       context.CancelFunc
	handle       LiveSourceSubscription
	disabled     bool // guarded by installation.mu
	joinOnce     sync.Once
	joinErr      error
}

func (g *liveSourceGroup) Emit(sub *orm.Subscription, rows []*orm.DataRecord) error {
	if sub == nil || sub.ExSymbol == nil || !g.streams[sub.Key()] {
		err := fmt.Errorf("source %s emitted an undeclared subscription", g.name)
		g.installation.reportGroup(g, err)
		return err
	}
	for _, row := range rows {
		if row == nil || row.Sid != sub.ExSymbol.ID {
			err := fmt.Errorf("source %s emitted a foreign or nil row", g.name)
			g.installation.reportGroup(g, err)
			return err
		}
	}
	return g.installation.emit(sub, rows, g)
}
func (g *liveSourceGroup) AwaitLiveReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.ctx.Done():
		return g.ctx.Err()
	case <-g.installation.ready:
		if err := ctx.Err(); err != nil {
			return err
		}
		return g.ctx.Err()
	}
}

func (g *liveSourceGroup) join() error {
	g.joinOnce.Do(func() {
		if g.handle != nil {
			g.joinErr = g.handle.Join()
		}
	})
	return g.joinErr
}

type SubscriptionInstallation struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	stopOnce             sync.Once
	mu                   sync.Mutex
	stopped              bool
	active               bool
	firstErr             error
	pending              []subscriptionEmission
	pendingRows          int
	startupBudget        int
	callbacks, producers sync.WaitGroup
	managed              []*liveSourceGroup
	groups               map[string]*liveSourceGroup
	degraded             []SubscriptionDegradation
	stopProvider         func()
	joinProvider         func()
	sink                 DataSink
	errors               chan error
	ready                chan struct{}
}

type subscriptionEmission struct {
	sub    *orm.Subscription
	rows   []*orm.DataRecord
	series []*orm.DataSeries
	group  *liveSourceGroup
}

// LiveSeriesSink retains adjustment and runtime metadata of typed kline rows.
// Side-source adapters continue to use the compact storage DataRecord contract.
type LiveSeriesSink interface {
	EmitSeries(*orm.Subscription, []*orm.DataSeries) error
	WarmupSeries(*orm.Subscription, []*orm.DataSeries) error
}

func (s *SubscriptionInstallation) Errors() <-chan error { return s.errors }

// Activate opens a prepared installation after its owner commits the consumer
// generation. Source failures after this point are failures of that generation.
func (s *SubscriptionInstallation) Activate() error { return s.activate() }

// CommitPrepared linearizes owner publication with source failure reporting.
// The callback must not call installation methods while this lock is held.
func (s *SubscriptionInstallation) CommitPrepared(commit func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstErr != nil {
		return s.firstErr
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.stopped || s.active {
		return fmt.Errorf("fresh prepared subscription required for publication")
	}
	return commit()
}

func (s *SubscriptionInstallation) Degradations() []SubscriptionDegradation {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := slices.Clone(s.degraded)
	for i := range result {
		result[i].Consumers = slices.Clone(result[i].Consumers)
	}
	return result
}

// AwaitLiveReady lets a managed producer wait for whole-plan readiness in its
// owned goroutine. SubscribeManaged itself must return its prepared handle.
func (s *SubscriptionInstallation) AwaitLiveReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.ready:
		return nil
	}
}
func (s *SubscriptionInstallation) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopped = true
		s.pending = nil
		managed := slices.Clone(s.managed)
		s.mu.Unlock()
		s.cancel()
		if prepared, ok := s.sink.(interface{ DiscardPreparedSeries() }); ok {
			prepared.DiscardPreparedSeries()
		}
		for _, source := range managed {
			source.handle.Stop()
		}
		if s.stopProvider != nil {
			s.stopProvider()
		}
	})
}
func (s *SubscriptionInstallation) Join() error {
	if s == nil {
		return nil
	}
	s.producers.Wait()
	for _, source := range s.managed {
		if err := source.join(); err != nil {
			s.reportGroup(source, err)
		}
	}
	s.callbacks.Wait()
	if s.joinProvider != nil {
		s.joinProvider()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}
func (s *SubscriptionInstallation) report(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.mu.Unlock()
	select {
	case s.errors <- err:
	default:
	}
	s.Stop()
}
func (s *SubscriptionInstallation) reportGroup(group *liveSourceGroup, err error) {
	if err == nil {
		return
	}
	if group == nil || group.required {
		s.report(err)
		return
	}
	s.mu.Lock()
	if group.disabled {
		s.mu.Unlock()
		return
	}
	group.disabled = true
	s.degraded = append(s.degraded, SubscriptionDegradation{Source: group.name, Consumers: slices.Clone(group.consumers), Err: err})
	kept := s.pending[:0]
	for _, emission := range s.pending {
		if emission.group == group {
			s.pendingRows -= len(emission.rows)
		} else {
			kept = append(kept, emission)
		}
	}
	s.pending = kept
	handle := group.handle
	s.mu.Unlock()
	group.cancel()
	if handle != nil {
		handle.Stop()
	}
}
func (s *SubscriptionInstallation) Emit(sub *orm.Subscription, rows []*orm.DataRecord) error {
	var group *liveSourceGroup
	if sub != nil {
		group = s.groups[sub.Source]
	}
	return s.emit(sub, rows, group)
}
func (s *SubscriptionInstallation) emit(sub *orm.Subscription, rows []*orm.DataRecord, group *liveSourceGroup) error {
	return s.emitRows(sub, rows, nil, group)
}
func (s *SubscriptionInstallation) emitRows(sub *orm.Subscription, rows []*orm.DataRecord, series []*orm.DataSeries, group *liveSourceGroup) error {
	s.mu.Lock()
	if s.stopped || group != nil && group.disabled {
		s.mu.Unlock()
		return context.Canceled
	}
	if !s.active {
		if s.pendingRows+len(rows) > s.startupBudget {
			s.mu.Unlock()
			err := fmt.Errorf("subscription startup row budget exceeded")
			s.reportGroup(group, err)
			return err
		}
		if sub == nil {
			s.mu.Unlock()
			err := fmt.Errorf("subscription source emitted a nil subscription")
			s.reportGroup(group, err)
			return err
		}
		cp := cloneSubscription(*sub)
		s.pending = append(s.pending, subscriptionEmission{sub: &cp, rows: cloneStartupRows(rows), series: cloneStartupSeries(series), group: group})
		s.pendingRows += len(rows)
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	return s.deliverRows(sub, rows, series, group)
}

func (s *SubscriptionInstallation) deliver(sub *orm.Subscription, rows []*orm.DataRecord, group *liveSourceGroup) error {
	return s.deliverRows(sub, rows, nil, group)
}
func (s *SubscriptionInstallation) deliverRows(sub *orm.Subscription, rows []*orm.DataRecord, series []*orm.DataSeries, group *liveSourceGroup) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return context.Canceled
	}
	if group != nil && group.disabled {
		s.mu.Unlock()
		return nil
	}
	s.callbacks.Add(1)
	s.mu.Unlock()
	defer s.callbacks.Done()
	var err error
	if typed, ok := s.sink.(LiveSeriesSink); ok && series != nil {
		err = typed.EmitSeries(sub, series)
	} else {
		err = s.sink.Emit(sub, rows)
	}
	if err != nil {
		s.reportGroup(group, err)
		return err
	}
	return nil
}

func (s *SubscriptionInstallation) activate() error {
	for {
		s.mu.Lock()
		if err := s.ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.stopped {
			err := s.firstErr
			s.mu.Unlock()
			if err == nil {
				err = context.Canceled
			}
			return err
		}
		if s.active {
			s.mu.Unlock()
			return nil
		}
		pending := s.pending
		s.pending = nil
		s.pendingRows = 0
		if len(pending) == 0 {
			s.active = true
			close(s.ready)
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
		for _, emission := range pending {
			if err := s.deliverRows(emission.sub, emission.rows, emission.series, emission.group); err != nil {
				if emission.group == nil || emission.group.required {
					return err
				}
			}
		}
	}
}

func cloneStartupRows(rows []*orm.DataRecord) []*orm.DataRecord {
	result := make([]*orm.DataRecord, len(rows))
	for i, row := range rows {
		if row != nil {
			copyRow := *row
			copyRow.Values = cloneStartupValue(reflect.ValueOf(row.Values)).Interface().(map[string]any)
			result[i] = &copyRow
		}
	}
	return result
}
func cloneStartupValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type()).Elem()
		result.Set(cloneStartupValue(value.Elem()))
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			result.SetMapIndex(iter.Key(), cloneStartupValue(iter.Value()))
		}
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneStartupValue(value.Index(i)))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneStartupValue(value.Index(i)))
		}
		return result
	case reflect.Ptr:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(cloneStartupValue(value.Elem()))
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if value.Field(i).CanInterface() && result.Field(i).CanSet() {
				result.Field(i).Set(cloneStartupValue(value.Field(i)))
			}
		}
		return result
	default:
		return value
	}
}

func (s *SubscriptionInstallation) addManaged(group *liveSourceGroup, handle LiveSourceSubscription) {
	s.mu.Lock()
	group.handle = handle
	s.managed = append(s.managed, group)
	stopped := s.stopped || group.disabled
	s.mu.Unlock()
	if stopped {
		handle.Stop()
	}
	if reporter, ok := handle.(LiveSourceErrors); ok && reporter.Errors() != nil {
		s.producers.Add(1)
		go func() {
			defer s.producers.Done()
			select {
			case <-group.ctx.Done():
				return
			case err, open := <-reporter.Errors():
				if open && err != nil {
					s.reportGroup(group, err)
				}
			}
		}()
	}
}

// InstallLivePlan prepares every group before starting producers. Managed
// sources report startup success synchronously. Legacy SubscribeLive adapters
// run under a cancellable context and must join their own producers before
// returning, or implement ManagedLiveSource for an explicit lifetime handle.
func (c *DataSourceCatalog) InstallLivePlan(ctx context.Context, plan *SubscriptionPlan, sink DataSink) (*SubscriptionInstallation, error) {
	installation, err := c.prepareLivePlan(ctx, plan, sink, nil, nil)
	if err != nil {
		return nil, err
	}
	if err := plan.warmupLive(installation); err != nil {
		installation.Stop()
		_ = installation.Join()
		return nil, err
	}
	if err := installation.warmupReady(plan.options.AnchorMS); err != nil {
		installation.Stop()
		_ = installation.Join()
		return nil, err
	}
	if err := installation.validateSources(plan); err != nil {
		installation.Stop()
		_ = installation.Join()
		return nil, err
	}
	if err := installation.activate(); err != nil {
		installation.Stop()
		_ = installation.Join()
		return nil, err
	}
	return installation, nil
}

// PrepareLivePlan owns and warms a side-source candidate while retaining its
// bounded startup queue. It emits no live callbacks until Activate. Kline
// feeder replacement requires a provider-specific generation contract.
func (c *DataSourceCatalog) PrepareLivePlan(ctx context.Context, plan *SubscriptionPlan, sink DataSink) (*SubscriptionInstallation, error) {
	if plan == nil {
		return nil, fmt.Errorf("subscription plan is required")
	}
	for _, stream := range plan.streams {
		if stream.Subscription.Source == orm.SeriesSourceKline {
			return nil, fmt.Errorf("prepared replacement requires side sources; kline generation replacement is unavailable")
		}
	}
	installation, err := c.prepareLivePlan(ctx, plan, sink, nil, nil)
	if err == nil {
		err = plan.warmupLive(installation)
	}
	if err == nil {
		err = installation.warmupReady(plan.options.AnchorMS)
	}
	if err == nil {
		err = installation.validateSources(plan)
	}
	if err != nil {
		if installation != nil {
			installation.Stop()
			_ = installation.Join()
		}
		return nil, err
	}
	return installation, nil
}

func (c *DataSourceCatalog) prepareLivePlan(ctx context.Context, plan *SubscriptionPlan, sink DataSink, stopProvider, joinProvider func()) (*SubscriptionInstallation, error) {
	if sink == nil {
		return nil, fmt.Errorf("subscription sink is required")
	}
	if plan == nil || plan.catalog != c {
		return nil, fmt.Errorf("subscription plan belongs to another source catalog")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownedCtx, cancel := context.WithCancel(ctx)
	installation := &SubscriptionInstallation{ctx: ownedCtx, cancel: cancel, sink: sink, errors: make(chan error, 1), ready: make(chan struct{}), groups: make(map[string]*liveSourceGroup), degraded: slices.Clone(plan.degraded)}
	installation.stopProvider, installation.joinProvider = stopProvider, joinProvider
	installation.startupBudget = plan.options.PrefetchRows
	if installation.startupBudget == 0 {
		installation.startupBudget = plan.options.PageRows * max(1, len(plan.streams))
	}
	groups := make(map[string][]*orm.Subscription)
	for _, stream := range plan.streams {
		sub := stream.Subscription
		if sub.Source != orm.SeriesSourceKline {
			cp := sub
			groups[sub.Source] = append(groups[sub.Source], &cp)
			group := installation.groups[sub.Source]
			if group == nil {
				groupCtx, groupCancel := context.WithCancel(ownedCtx)
				group = &liveSourceGroup{installation: installation, name: sub.Source, ctx: groupCtx, cancel: groupCancel, streams: make(map[orm.StreamKey]bool)}
				installation.groups[sub.Source] = group
			}
			group.required = group.required || streamRequired(stream)
			group.streams[sub.Key()] = true
			for _, consumer := range stream.Consumers {
				group.consumers = mergeSubscriptionConsumer(group.consumers, consumer)
			}
		}
	}
	var legacy []string
	for _, stream := range plan.streams {
		name := stream.Subscription.Source
		if name == orm.SeriesSourceKline {
			continue
		}
		version, schema, err := sourcePlanIdentity(c.GetDataSource(name))
		if err == nil && (version != stream.SourceVersion || schema != stream.SchemaHash) {
			err = fmt.Errorf("source %s changed after plan compilation", name)
		}
		if err != nil {
			group := installation.groups[name]
			if group.required {
				cancel()
				return nil, err
			}
			installation.reportGroup(group, err)
		}
	}
	if plan.options.RequireManagedLive {
		for name := range groups {
			if _, ok := c.GetDataSource(name).(ManagedLiveSource); !ok {
				err := fmt.Errorf("source %s requires an explicit managed readiness and join contract", name)
				group := installation.groups[name]
				if group.required {
					cancel()
					return nil, err
				}
				installation.reportGroup(group, err)
			}
		}
	}
	go func() { <-ownedCtx.Done(); installation.Stop() }()
	for _, name := range sortedSourceNames(groups) {
		group := installation.groups[name]
		installation.mu.Lock()
		disabled := group.disabled
		installation.mu.Unlock()
		if disabled {
			continue
		}
		source := c.GetDataSource(name)
		if managed, ok := source.(ManagedLiveSource); ok {
			handle, err := managed.SubscribeManaged(group.ctx, groups[name], group)
			if handle != nil {
				installation.addManaged(group, handle)
			}
			if err == nil && handle == nil {
				err = fmt.Errorf("source %s did not return an owned subscription", name)
			}
			if err != nil {
				err = fmt.Errorf("source %s startup: %w", name, err)
				if !group.required {
					installation.reportGroup(group, err)
					_ = group.join()
					continue
				}
				installation.Stop()
				_ = installation.Join()
				return nil, err
			}
		} else {
			legacy = append(legacy, name)
		}
	}
	installation.producers.Add(len(legacy))
	for _, name := range legacy {
		go func() {
			defer installation.producers.Done()
			group := installation.groups[name]
			err := c.GetDataSource(name).SubscribeLive(group.ctx, groups[name], group)
			if err != nil && group.ctx.Err() == nil {
				installation.reportGroup(group, fmt.Errorf("source %s subscription: %w", name, err))
			}
		}()
	}
	return installation, nil
}

// NewLiveSourceProvider composes event/side sources without a kline socket.
// A plan containing klines still requires the ordinary live feeder factory.
func NewLiveSourceProvider(catalog *DataSourceCatalog) *LiveProvider {
	return &LiveProvider{catalog: catalog}
}

func (p *LiveProvider) InstallSubscriptionPlan(ctx context.Context, plan *SubscriptionPlan, sink DataSink) (*SubscriptionInstallation, error) {
	installation, err := p.PrepareSubscriptionPlan(ctx, plan, sink)
	if err != nil {
		return nil, err
	}
	if err := installation.activate(); err != nil {
		installation.Stop()
		_ = installation.Join()
		return nil, err
	}
	return installation, nil
}

func (p *HistProvider) InstallSubscriptionPlan(ctx context.Context, plan *SubscriptionPlan) error {
	if p == nil {
		return fmt.Errorf("historical provider is required")
	}
	if err := plan.validate(p.catalog); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.SetSeriesPrefetch(plan.options.PageRows, plan.options.PrefetchRows); err != nil {
		return err
	}
	if err := p.SetSeriesPageBytes(plan.options.PageBytes); err != nil {
		return err
	}
	p.compiledSubscriptions = plan
	if err := p.SetSubscriptions(plan.Subscriptions()); err != nil {
		p.compiledSubscriptions = nil
		return err
	}
	return nil
}

func (p *HistProvider) subscriptionWarmupStart(ctx context.Context, sub *orm.Subscription, anchorMS int64) (int64, error) {
	if plan := p.compiledSubscriptions; plan != nil && plan.options.AnchorMS == anchorMS {
		for _, stream := range plan.streams {
			if stream.Subscription.Key() == sub.Key() {
				return stream.WarmupStartMS, nil
			}
		}
	}
	return p.catalog.SubscriptionWarmupStart(ctx, []*orm.Subscription{sub}, anchorMS)
}
