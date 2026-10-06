// Package runner assembles archival replay without exchange or global-runtime
// dependencies. Sinks own account execution; one runner owns factor state.
package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
	"io"
	"os"
	"sort"
)

type Mode string

const (
	Research Mode = "research"
	Weights  Mode = "weights"
	Events   Mode = "events"
	Trade    Mode = "trade"
)

type Chunk struct {
	Path     string
	From, To int64
}
type PriceStream struct{ Source, TimeFrame, Field string }
type Config struct {
	ArtifactPath                                       string
	Definition                                         string
	Expressions                                        *expr.Spec        `json:"expressions,omitempty"`
	ComputationGroup                                   *ComputationGroup `json:"-"`
	ComputationContext                                 ComputationContext
	Execution                                          ExecutionConfig
	Plan                                               *factor.Plan                                          `json:"-"`
	PortfolioBuilder                                   PortfolioBuilder                                      `json:"-"`
	PolicyContext                                      func(context.Context, *factor.PortfolioContext) error `json:"-"`
	PolicySIDMappingVersion                            string                                                `json:",omitempty"`
	Mode                                               Mode
	Chunks                                             []Chunk
	HistoricalInput                                    HistoricalInputFactory                       `json:"-"`
	ObserveBatch                                       func(context.Context, HistoricalBatch) error `json:"-"`
	MaxRecords, MaxPending                             int
	DecisionInterval, LatencyMS, ExpiryMS, LabelWaitMS int64
	DecisionDelayMS                                    int64
	Snapshot                                           factor.SnapshotSpec
	Factor                                             research.MomentumVolConfig
	Combo                                              research.ComboSpec
	Manifest                                           research.ManifestSpec
	StrategyID, AccountID                              string
	InitialNAV                                         float64
	AccountInitialNAV                                  float64
	timeline                                           *replayTimeline
	timelineIndex                                      int
	Prices                                             PriceStream
	FundingSource                                      string
}

// Sink must persist/idempotently coordinate targets before returning success.
// Quotes are visible at now; no future candle ranges are provided.
type Sink interface {
	ProcessSnapshot(context.Context, *factor.TargetPortfolio, map[int32]backtest.Quote, int64) error
}

// BudgetSource supplies reconciled strategy NAV for account-backed modes.
type BudgetSource interface {
	StrategyNAV(context.Context, int64) (float64, error)
}
type StateSource interface {
	StrategyState(context.Context, int64) (backtest.State, error)
}
type QuoteObserver interface {
	ObserveQuote(context.Context, int32, backtest.Quote, int64) error
}
type FundingObserver interface {
	ObserveFunding(context.Context, backtest.Funding, int64) error
}
type Output interface {
	Decision(factor.Frame, *factor.TargetPortfolio, []factor.Diagnostic) error
	Evaluation(research.Report) error
	Executed(*factor.TargetPortfolio, backtest.State, int64) error
}
type Result struct {
	Engine, StrategyID, AccountID                                      string
	TargetsAccepted                                                    int
	Fills                                                              int
	AccountFills                                                       int
	Account                                                            *execution.AccountSnapshot
	Decisions, Executions, Skipped, Incomplete, Unresolved             int
	ManifestID, StrategyHash                                           string
	Book                                                               backtest.State
	Summary                                                            map[string]map[string]research.SeriesSummary
	Manifest                                                           research.ManifestSpec
	NodeCount, MaxRawRecords, MaxPendingEvaluations, MaxRetainedValues int
	NodeUpdates                                                        map[string]uint64
	UnresolvedByHorizon                                                map[string]int `json:",omitempty"`
}
type evaluation struct {
	label             research.LabelSpec
	frame             factor.Frame
	begin, end        map[int32]backtest.Quote
	weights, previous map[int32]float64
	deadline          int64
	previousColumns   map[string]map[int32]factor.Numeric
}

func Run(ctx context.Context, c Config, sink Sink, out Output) (result Result, runErr error) {
	result.Engine, result.StrategyID, result.AccountID = "factor", c.StrategyID, c.AccountID
	c.Snapshot = factor.CloneSnapshotSpec(c.Snapshot)
	c.Snapshot.TrackedQuotesOnly = true
	inputDigest := sha256.New()
	defer func() {
		if c.HistoricalInput != nil && result.Manifest.Currency != "" {
			ref := research.SnapshotReference{ID: c.HistoricalInput.Identity(), AdjustmentVersion: c.Snapshot.AdjustmentVersion, Schemas: c.Snapshot.Schemas, SourceVersions: c.Snapshot.SourceVersions}
			if runErr == nil {
				ref.ContentDigest = hex.EncodeToString(inputDigest.Sum(nil))
			}
			result.Manifest.Snapshots = append(result.Manifest.Snapshots, ref)
			result.ManifestID = ""
			if runErr == nil {
				manifest, err := research.BuildManifest(result.Manifest)
				if err != nil {
					runErr = errors.Join(runErr, err)
				} else {
					result.Manifest, result.ManifestID = manifest.Spec(), manifest.ID()
				}
			}
		}
		if paper, ok := sink.(*AccountSink); ok && paper.Paper != nil {
			result.AccountFills = paper.Paper.Metrics().Fills
			result.Fills = paper.Paper.StrategyFillCount(execution.StrategyID(c.StrategyID))
		}
		artifact := makeRunArtifact(result, runErr)
		if c.ArtifactPath != "" {
			runErr = errors.Join(runErr, WriteRunArtifact(c.ArtifactPath, artifact))
		}
		if writer, ok := out.(RunArtifactOutput); ok {
			outputErr := writer.RunFinished(makeRunArtifact(result, runErr))
			runErr = errors.Join(runErr, outputErr)
			if outputErr != nil && c.ArtifactPath != "" {
				runErr = errors.Join(runErr, WriteRunArtifact(c.ArtifactPath, makeRunArtifact(result, runErr)))
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := ValidateReplayConfig(c, sink == nil); err != nil {
		return result, err
	}
	if c.HistoricalInput != nil {
		c.Chunks = c.HistoricalInput.Ranges()
	}
	if len(c.Chunks) == 0 {
		return result, errors.New("runner: replay input is required")
	}
	if c.Mode == Events && sink == nil {
		paper, cleanup, err := NewPaperSink(ctx, c)
		if err != nil {
			return result, err
		}
		sink = paper
		defer func() { runErr = errors.Join(runErr, cleanup()) }()
	}
	if (c.Mode == Trade || c.Mode == Events) && sink == nil {
		return result, errors.New("runner: events/trade require an account execution sink")
	}
	if c.Mode == Trade || c.Mode == Events {
		if _, ok := sink.(BudgetSource); !ok {
			return result, errors.New("runner: account execution requires reconciled strategy NAV")
		}
		if _, ok := sink.(StateSource); !ok {
			return result, errors.New("runner: account execution requires reconciled strategy state")
		}
	}
	if (c.Mode == Trade || c.Mode == Events) && c.Manifest.Costs.FundingPolicy == "required-stream" {
		if _, ok := sink.(FundingObserver); !ok {
			return result, errors.New("runner: account execution requires funding reconciliation")
		}
	}
	var labelSpec research.LabelSpec
	if len(c.Manifest.Labels) > 0 {
		labelSpec = c.Manifest.Labels[0]
	}
	plan, combo, err := compileDecision(c)
	if err != nil {
		return result, err
	}
	c.Manifest.ExecutionMode = string(c.Mode)
	c.Manifest.LatencyAssumption = fmt.Sprintf("visibility cutoff=grid+%dms; observable-event-after-decision+%dms; no OHLC reconstruction", c.DecisionDelayMS, c.LatencyMS)
	assumptions, err := runAssumptions(c)
	if err != nil {
		return result, err
	}
	c.Manifest.LatencyAssumption += "; replay-config=" + assumptions
	// Archive fingerprints belong to run lineage, outside strategy identity.
	for _, chunk := range c.Chunks {
		if c.HistoricalInput != nil {
			break
		}
		f, err := os.Open(chunk.Path)
		if err != nil {
			return result, err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		closeErr := f.Close()
		if err != nil {
			return result, err
		}
		if closeErr != nil {
			return result, closeErr
		}
		digest := hex.EncodeToString(h.Sum(nil))
		c.Manifest.Snapshots = append(c.Manifest.Snapshots, research.SnapshotReference{ID: digest, ContentDigest: digest, AdjustmentVersion: c.Snapshot.AdjustmentVersion, Schemas: c.Snapshot.Schemas, SourceVersions: c.Snapshot.SourceVersions})
	}
	engine, err := newDecisionEngine(c, plan, combo)
	if err != nil {
		return result, err
	}
	defer engine.close()
	result.ManifestID = engine.manifest.ID()
	result.StrategyHash = engine.manifest.StrategyHash()
	result.Manifest = engine.manifest.Spec()
	var book *backtest.Book
	if c.Mode == Weights {
		book, err = backtest.NewBook(c.InitialNAV)
		if err != nil {
			return result, err
		}
	}
	bookState := func() backtest.State {
		if book != nil {
			return book.State()
		}
		return backtest.State{}
	}
	policy, err := newPolicyRun(c)
	if err != nil {
		return result, err
	}
	if policy != nil && book == nil {
		if _, ok := sink.(PolicySink); !ok {
			return result, errors.New("runner: lifecycle policy requires an allocation-capable sink and explicit position evidence")
		}
	}
	var policyPending *pendingProposal
	var history *research.ICHistory
	if research.IsHistoryMethod(combo.Method) {
		if combo.Label != "" {
			found := false
			for _, label := range c.Manifest.Labels {
				if label.Name == combo.Label {
					labelSpec = label
					found = true
					break
				}
			}
			if !found {
				return result, errors.New("runner: history label not declared")
			}
		}
		history, err = research.NewICHistory(256, labelSpec.Name, combo.Columns)
		if err != nil {
			return result, err
		}
	}
	var acc *research.Accumulator
	if len(c.Manifest.Labels) > 0 {
		names := make([]string, len(c.Manifest.Labels))
		for i, label := range c.Manifest.Labels {
			names[i] = label.Name
		}
		acc, err = research.NewAccumulator(append(plan.Outputs(), "score"), names)
		if err != nil {
			return result, err
		}
	}
	var barrier factor.RoundBarrier
	defer func() { barrier.Stop(); barrier.Join() }()
	var pending *factor.TargetPortfolio
	var executed *factor.TargetPortfolio
	var previousWeights map[int32]float64
	var previousColumns map[string]map[int32]factor.Numeric
	var evaluations []*evaluation
	quotes := map[int32]backtest.Quote{}
	var sequence uint64
	lastTime := int64(0)
	finish := func(now int64) error {
		for i := 0; i < len(evaluations); {
			e := evaluations[i]
			if e.deadline > now {
				i++
				continue
			}
			labels := make([]research.Label, 0, len(c.Snapshot.Universe.Evaluation))
			for _, sid := range c.Snapshot.Universe.Evaluation {
				begin := e.begin[sid]
				end := e.end[sid]
				bAt := begin.AtMS
				if bAt <= e.frame.DecisionTime {
					bAt = e.frame.DecisionTime + c.LatencyMS
				}
				bn, en := factor.Numeric{Validity: factor.Missing}, factor.Numeric{Validity: factor.Missing}
				if begin.Price > 0 {
					bn = factor.Numeric{Value: begin.Price, Validity: factor.Valid}
				}
				if end.Price > 0 {
					en = factor.Numeric{Value: end.Price, Validity: factor.Valid}
				}
				l, err := research.ReturnLabel(e.label, sid, e.frame.DecisionTime, bAt, bAt+e.label.Horizon, max(bAt+e.label.Horizon, now), bn, en)
				if err != nil {
					return err
				}
				labels = append(labels, l)
			}
			report, err := research.Evaluate(e.frame, c.Snapshot.Universe, labels, research.EvaluationSpec{AsOf: now, PrimaryLabel: e.label.Name, CostRate: c.Manifest.Costs.FeeRate + c.Manifest.Costs.SlippageRate, CurrentWeights: e.weights, PreviousWeights: e.previous, PreviousColumns: e.previousColumns})
			if err != nil {
				return err
			}
			if err = acc.Add(report); err != nil {
				return err
			}
			if history != nil && e.label.Name == labelSpec.Name {
				for _, name := range combo.Columns {
					lm := report.Columns[name].Labels[labelSpec.Name]
					if lm.Pairs >= 2 {
						if err = history.Add(now, research.ICSample{Column: name, Label: labelSpec.Name, DecisionTime: e.frame.DecisionTime, MatureAt: now, AvailableAt: now, IC: lm.IC, RankIC: lm.RankIC, Samples: lm.Pairs}); err != nil {
							return err
						}
					}
				}
			}
			if out != nil {
				if err = out.Evaluation(report); err != nil {
					return err
				}
			}
			copy(evaluations[i:], evaluations[i+1:])
			evaluations[len(evaluations)-1] = nil
			evaluations = evaluations[:len(evaluations)-1]
		}
		return nil
	}
	for _, chunk := range c.Chunks {
		var input HistoricalInput
		if c.HistoricalInput != nil {
			input, err = c.HistoricalInput.Open(ctx, c, chunk)
		} else {
			input, err = openArchiveInput(ctx, c, chunk)
		}
		if err != nil {
			return result, err
		}
		defer func() { runErr = errors.Join(runErr, input.Close()) }()
		driver := newReplayInput(input, c, chunk)
		for {
			batch, nextErr := driver.Next(ctx)
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				return result, nextErr
			}
			now := batch.AtMS
			if c.HistoricalInput != nil {
				for _, record := range batch.Records {
					digest, hashErr := factor.ContentHash(record)
					if hashErr != nil {
						return result, hashErr
					}
					_, _ = io.WriteString(inputDigest, digest)
				}
			}
			warming := now-c.DecisionDelayMS < chunk.From
			result.MaxRawRecords = max(result.MaxRawRecords, input.MaxRetainedRecords())
			if c.timeline != nil {
				if err = c.timeline.enter(ctx, c.timelineIndex, now); err != nil {
					return result, err
				}
			}
			if err = ctx.Err(); err != nil {
				return result, err
			}
			lastTime = now
			// Price availability and explicit funding settlements precede decisions at
			// equal timestamps. Such prices cannot execute that new decision.
			for _, r := range batch.Records {
				if r.Series.Source == c.Prices.Source && r.Series.TimeFrame == c.Prices.TimeFrame {
					n := factor.Number(r.Series.Values, c.Prices.Field)
					if n.Validity != factor.Valid || n.Value <= 0 {
						continue
					}
					q := backtest.Quote{AtMS: r.EventTime, AvailableAt: now, Price: n.Value}
					q = withSpread(q, r.Series.Values)
					if old, ok := quotes[r.Series.Sid]; ok && old.AtMS > q.AtMS {
						continue
					}
					quotes[r.Series.Sid] = q
					if book != nil && !warming {
						if err = book.Mark(r.Series.Sid, q, now); err != nil {
							return result, err
						}
					}
					if observer, ok := sink.(QuoteObserver); ok && !warming {
						if err = observer.ObserveQuote(ctx, r.Series.Sid, q, now); err != nil {
							return result, err
						}
					}
					for _, e := range evaluations {
						if q.AtMS >= e.frame.DecisionTime+c.LatencyMS && q.AtMS <= e.frame.DecisionTime+c.ExpiryMS {
							if _, ok := e.begin[r.Series.Sid]; !ok {
								e.begin[r.Series.Sid] = q
								e.deadline = max(e.deadline, q.AtMS+e.label.Horizon+c.LabelWaitMS)
							}
						}
						begin, ok := e.begin[r.Series.Sid]
						if ok && q.AtMS == begin.AtMS+e.label.Horizon {
							e.end[r.Series.Sid] = q
						}
					}
				}
			}
			for _, r := range batch.Records {
				if r.Series.Source == c.FundingSource && c.Manifest.Costs.FundingPolicy == "required-stream" {
					n := factor.Number(r.Series.Values, "rate")
					if n.Validity != factor.Valid {
						return result, errors.New("runner: invalid funding rate")
					}
					f := backtest.Funding{ID: fmt.Sprintf("%s:%d:%d", r.Series.Source, r.Series.Sid, r.EventTime), SID: r.Series.Sid, AtMS: r.EventTime, AvailableAt: r.AvailableAt, Rate: n.Value}
					if observer, ok := sink.(FundingObserver); ok && !warming {
						if err = observer.ObserveFunding(ctx, f, now); err != nil {
							return result, err
						}
					}
					if book != nil && !warming {
						if err = book.ApplyFunding(f, now); err != nil {
							return result, err
						}
					}
				}
			}
			if c.ObserveBatch != nil {
				if err = c.ObserveBatch(ctx, batch); err != nil {
					return result, err
				}
			}
			if policyPending != nil {
				if now >= policyPending.spec.ExpireAt {
					policyPending = nil
					result.Skipped++
				} else {
					policyPending, err = policy.refresh(ctx, c, policyPending, sink, book, quotes, now)
					if err != nil {
						return result, err
					}
					ready, readyErr := proposalReady(policyPending, policy.previous, quotes, now, c.Snapshot.Universe.Tracked)
					if readyErr != nil {
						return result, readyErr
					}
					if ready {
						var receipt execution.PolicyReceipt
						if book != nil {
							receipt, err = book.AcceptProposal(policyPending.proposal, policyPending.version, policyPending.cursor, quotes, now, c.Manifest.Costs.FeeRate, c.Manifest.Costs.SlippageRate)
						} else {
							receipt, err = sink.(PolicySink).AcceptProposal(ctx, policyPending.proposal, policyPending.version, policyPending.cursor, copyQuotes(quotes), now)
						}
						if receipt.Accepted {
							if acceptErr := policy.accepted(policyPending); acceptErr != nil {
								return result, acceptErr
							}
							if policyPending.proposal.Target != nil {
								result.Executions++
								result.TargetsAccepted++
								state := bookState()
								if source, ok := sink.(StateSource); ok {
									state, err = source.StrategyState(ctx, now)
									if err != nil {
										return result, err
									}
								}
								if outputErr := emitAllocationAccepted(out, policyPending.proposal.Target, state, now); outputErr != nil {
									return result, outputErr
								}
							}
							policyPending = nil
						}
						if errors.Is(err, execution.ErrPolicyEvidenceChanged) {
							err = nil
						}
						if err != nil {
							return result, err
						}
						if receipt.SendError != nil {
							return result, receipt.SendError
						}
					}
				}
			}
			if pending != nil {
				sp := pending.Spec()
				if now >= sp.ExpireAt {
					pending = nil
					result.Skipped++
				} else if now >= sp.ExecutableAt {
					ready := true
					effective, err := pending.EffectiveTargets(executed)
					if err != nil {
						return result, err
					}
					targetsToExecute := effective
					if sp.Mode == factor.Patch {
						targetsToExecute = pending.Targets()
					}
					for sid := range targetsToExecute {
						q, ok := quotes[sid]
						if !ok || q.AtMS < sp.ExecutableAt || q.AvailableAt > now {
							ready = false
						}
					}
					if ready {
						if sink != nil {
							if err = sink.ProcessSnapshot(ctx, pending, copyQuotes(quotes), now); err != nil {
								return result, err
							}
						}
						if c.Mode == Weights {
							if err = book.Execute(pending, quotes, now, c.Manifest.Costs.FeeRate, c.Manifest.Costs.SlippageRate); err != nil {
								return result, err
							}
						}
						result.Executions++
						result.TargetsAccepted++
						executed, err = factor.NewTargetPortfolio(sp, effective)
						if err != nil {
							return result, err
						}
						if out != nil {
							state := bookState()
							if source, ok := sink.(StateSource); ok {
								state, err = source.StrategyState(ctx, now)
								if err != nil {
									return result, err
								}
							}
							if err = emitTargetAccepted(out, pending, state, now); err != nil {
								return result, err
							}
						}
						pending = nil
					}
				}
			}
			if err = finish(now); err != nil {
				return result, err
			}
			grid := now - c.DecisionDelayMS
			gridOffset := chunk.From
			if policy != nil {
				gridOffset = 0
			}
			if grid <= 0 || (grid-gridOffset)%c.DecisionInterval != 0 {
				if c.timeline != nil {
					c.timeline.leave()
				}
				continue
			}
			spec := c.Snapshot
			spec.GridTime = grid
			spec.DecisionTime = now
			if spec.ReplayTime != 0 {
				spec.ReplayTime = now
			}
			req := requirements(plan, spec.Universe, grid)
			requested := map[factor.StreamKey]bool{}
			for _, r := range req {
				requested[factor.StreamKey{SID: r.SID, Source: r.Source, TimeFrame: r.TimeFrame}] = true
			}
			token, err := barrier.Begin(plan.Hash(), spec, req, now+c.ExpiryMS)
			if err != nil {
				return result, err
			}
			visible, err := input.Visible(ctx, grid, now, spec.ReplayTime)
			if err != nil {
				return result, err
			}
			for _, r := range visible {
				if !requested[factor.StreamKey{SID: r.Series.Sid, Source: r.Series.Source, TimeFrame: r.Series.TimeFrame}] {
					continue
				}
				if err = barrier.Observe(token, r, now); err != nil {
					return result, err
				}
			}
			snapshot, err := barrier.Freeze(token, now)
			if errors.Is(err, factor.ErrSnapshotIncomplete) {
				if !warming {
					result.Incomplete++
					if policy != nil {
						evidence, evidenceErr := policyEvidence(ctx, sink, book, now)
						if evidenceErr != nil {
							return result, evidenceErr
						}
						sequence = max(sequence+1, evidence.PlanSequence+1)
						nav := c.InitialNAV
						if book != nil {
							nav = bookState().NAV
						}
						if source, ok := sink.(BudgetSource); ok {
							nav, evidenceErr = source.StrategyNAV(ctx, now)
							if evidenceErr != nil {
								return result, evidenceErr
							}
						}
						monitor, monitorErr := policyMonitoringFrame(plan.Hash(), grid, now, evidence)
						if monitorErr != nil {
							return result, monitorErr
						}
						sp := engine.portfolioSpec(monitor, spec.Universe, sequence, nav, now+c.LatencyMS, now+c.ExpiryMS)
						candidate, candidateErr := policy.propose(ctx, c, monitor, spec.Universe, nil, sp, grid, evidence, quotes, true)
						if candidateErr != nil {
							return result, candidateErr
						}
						if err := emitAllocationDecision(out, monitor, candidate.proposal.Target, append(candidate.proposal.Reasons, factor.Diagnostic{Code: "policy-monitor-incomplete", Detail: "only expiry and hard exits checked; no ordinary ranking step"})); err != nil {
							return result, err
						}
						if !candidate.noop {
							policyPending = candidate
						}
					}
				}
				if c.timeline != nil {
					c.timeline.leave()
				}
				continue
			}
			if err != nil {
				return result, err
			}
			frame, err := engine.evaluate(ctx, snapshot)
			if err != nil {
				return result, err
			}
			result.MaxRetainedValues = max(result.MaxRetainedValues, engine.session.RetainedValues())
			if warming {
				if c.timeline != nil {
					c.timeline.leave()
				}
				continue
			}
			frame, diags, err := engine.combine(frame, spec.Universe, history)
			if err != nil {
				return result, err
			}
			sequence++
			nav := c.InitialNAV
			if book != nil {
				nav = bookState().NAV
			}
			if source, ok := sink.(BudgetSource); ok && (c.Mode == Trade || c.Mode == Events) {
				nav, err = source.StrategyNAV(ctx, now)
				if err != nil {
					return result, err
				}
			}
			var evidence factor.PortfolioEvidence
			if policy != nil {
				evidence, err = policyEvidence(ctx, sink, book, now)
				if err != nil {
					return result, err
				}
				sequence = max(sequence, evidence.PlanSequence+1)
			}
			p, pdiag, err := engine.buildPortfolio(frame, spec.Universe, sequence, nav, now+c.LatencyMS, now+c.ExpiryMS)
			if err != nil {
				return result, err
			}
			diags = append(diags, pdiag...)
			result.Decisions++
			var policyWeights map[int32]float64
			if policy != nil {
				portfolioSpec := engine.portfolioSpec(frame, spec.Universe, sequence, nav, now+c.LatencyMS, now+c.ExpiryMS)
				proposal, proposalErr := policy.propose(ctx, c, frame, spec.Universe, p, portfolioSpec, grid, evidence, quotes, false)
				if proposalErr != nil {
					return result, proposalErr
				}
				diags = append(diags, proposal.proposal.Reasons...)
				if outputErr := emitAllocationDecision(out, frame, proposal.proposal.Target, diags); outputErr != nil {
					return result, outputErr
				}
				if proposal.proposal.Target == nil {
					policyWeights = previousWeights
				} else {
					effective, effectiveErr := proposal.proposal.Target.EffectiveAllocations(policy.previous)
					if effectiveErr != nil {
						return result, effectiveErr
					}
					valued, valueErr := factor.NewPortfolioTarget(proposal.proposal.Target.Spec(), effective)
					if valueErr != nil {
						return result, valueErr
					}
					policyWeights = allocationWeights(valued, quotes)
				}
				if policyPending != nil {
					result.Skipped++
				}
				policyPending = proposal
				if proposal.noop {
					policyPending = nil
					result.Skipped++
				}
			} else if out != nil {
				if err = out.Decision(factor.CloneFrame(frame), p, diags); err != nil {
					return result, err
				}
			}
			if policy == nil && p != nil {
				if pending != nil {
					result.Skipped++
				}
				pending = p
			} else if policy == nil {
				result.Skipped++
			}
			if acc != nil {
				if len(evaluations)+len(c.Manifest.Labels) > c.MaxPending {
					return result, errors.New("runner: pending label bound exceeded")
				}
				var weights map[int32]float64
				if p != nil {
					weights = p.Targets()
				}
				if policy != nil {
					weights = policyWeights
				}
				for _, label := range c.Manifest.Labels {
					e := &evaluation{label: label, frame: frame, begin: map[int32]backtest.Quote{}, end: map[int32]backtest.Quote{}, previous: previousWeights, weights: weights, deadline: now + c.ExpiryMS + label.Horizon + c.LabelWaitMS, previousColumns: previousColumns}
					evaluations = append(evaluations, e)
				}
				if weights != nil {
					previousWeights = weights
				}
				previousColumns = factor.CloneFrame(frame).Values
				result.MaxPendingEvaluations = max(result.MaxPendingEvaluations, len(evaluations))
			}
			if c.timeline != nil {
				c.timeline.leave()
			}
		}
		if book != nil {
			book.EndChunk()
		}
		if err = input.Close(); err != nil {
			return result, err
		}
	}
	result.Unresolved = len(evaluations)
	if len(evaluations) > 0 {
		result.UnresolvedByHorizon = map[string]int{}
		for _, e := range evaluations {
			result.UnresolvedByHorizon[e.label.Name]++
		}
	}
	if pending != nil {
		result.Skipped++
	}
	if policyPending != nil {
		result.Skipped++
	}
	result.Book = bookState()
	if source, ok := sink.(StateSource); ok {
		result.Book, err = source.StrategyState(ctx, lastTime)
		if err != nil {
			return result, err
		}
	}
	if account, ok := sink.(*AccountSink); ok {
		snapshot, snapshotErr := account.Account.Snapshot(ctx)
		if snapshotErr != nil {
			return result, snapshotErr
		}
		result.Account = &snapshot
	}
	if acc != nil {
		result.Summary = acc.Summary()
	}
	result.NodeCount = plan.NodeCount()
	result.NodeUpdates = engine.session.Updates()
	return result, nil
}
func withSpread(q backtest.Quote, values map[string]any) backtest.Quote {
	bid, ask := factor.Number(values, "bid"), factor.Number(values, "ask")
	if bid.Validity == factor.Valid && ask.Validity == factor.Valid {
		q.Bid, q.Ask = bid.Value, ask.Value
	}
	return q
}
func runAssumptions(c Config) (string, error) {
	e := c.Execution
	e.StorePath = ""
	e.SenderLeaseDir = ""
	e.HistoryPath = ""
	ranges := make([][2]int64, len(c.Chunks))
	for i, ch := range c.Chunks {
		ranges[i] = [2]int64{ch.From, ch.To}
	}
	body, err := json.Marshal(struct {
		Prices                PriceStream
		FundingSource         string
		InitialNAV            float64
		ExpiryMS, LabelWaitMS int64
		ReceptionReplay       bool
		Ranges                [][2]int64
		Execution             ExecutionConfig
	}{c.Prices, c.FundingSource, c.InitialNAV, c.ExpiryMS, c.LabelWaitMS, c.Snapshot.ReplayTime != 0, ranges, e})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}
func requirements(plan *factor.Plan, u factor.Universe, at int64) []factor.Requirement {
	ids := append(append([]int32{}, u.Reference...), u.Investable...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	seen := map[int32]bool{}
	out := []factor.Requirement{}
	for _, sid := range ids {
		if seen[sid] {
			continue
		}
		seen[sid] = true
		for _, in := range plan.Inputs() {
			out = append(out, factor.Requirement{SID: sid, Source: in.Source, TimeFrame: in.TimeFrame, EventTime: at, AsOfLatest: in.AsOfLatest, MaxAge: in.MaxAge})
		}
	}
	return out
}
func copyQuotes(src map[int32]backtest.Quote) map[int32]backtest.Quote {
	dst := make(map[int32]backtest.Quote, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
