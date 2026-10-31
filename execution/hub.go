package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// HubAccountRegistration is supplied by a robot, never loaded from hub YAML.
// Configuration may contain credentials and must not be logged or echoed.
type HubAccountRegistration struct {
	Account       AccountKey
	Env           string
	Configuration json.RawMessage
}

// HubAccountBinding transfers a fully initialized account to the hub. Shutdown
// must stop and join its owner/workers before closing its store and transport.
// It is called once, including when hub shutdown races successful registration.
type HubAccountBinding struct {
	Account  *SharedAccount
	Shutdown func() error
	// RegisterAdditional validates the immutable host baseline before admitting
	// contributors from another robot. Opaque configurations stay host-owned.
	RegisterAdditional func(context.Context, HubAccountRegistration) error
}

type HubAccountFactory func(context.Context, HubAccountRegistration) (HubAccountBinding, error)

type hubAccount struct {
	hash    [32]byte
	env     string
	done    chan struct{}
	binding HubAccountBinding
	err     error
}

// Hub owns account lifetimes independently of robot connections. Its API only
// accepts account-level commands; SDK calls and ledger transactions stay local.
type Hub struct {
	publicFeeds   map[HubPendingFeed]bool
	feedAdmission PendingFeedAdmissionValidator
	mu            sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	factory       HubAccountFactory
	accounts      map[AccountKey]*hubAccount
	recovery      *hubRecoveryRegistry
	work          sync.WaitGroup
	closed        bool
	closeOnce     sync.Once
	closeErr      error
}

func NewHub(factory HubAccountFactory) (*Hub, error) {
	if factory == nil {
		return nil, errors.New("execution: hub account factory required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{ctx: ctx, cancel: cancel, factory: factory, accounts: map[AccountKey]*hubAccount{}}, nil
}

func hubRegistrationHash(request HubAccountRegistration) ([32]byte, error) {
	if err := request.Account.Validate(); err != nil {
		return [32]byte{}, err
	}
	if request.Env != "prod" && request.Env != "dry_run" {
		return [32]byte{}, errors.New("execution: hub only accepts prod and dry_run accounts")
	}
	// Normalize object ordering and whitespace without rounding numeric values.
	var configuration any
	decoder := json.NewDecoder(bytes.NewReader(request.Configuration))
	decoder.UseNumber()
	if !json.Valid(request.Configuration) || decoder.Decode(&configuration) != nil || configuration == nil {
		return [32]byte{}, errors.New("execution: hub account configuration must be valid non-null JSON")
	}
	body, err := json.Marshal(struct {
		Env           string
		Configuration any
	}{request.Env, configuration})
	if err != nil {
		return [32]byte{}, errors.New("execution: invalid hub account configuration")
	}
	return sha256.Sum256(body), nil
}

func (h *Hub) Register(ctx context.Context, request HubAccountRegistration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	hash, err := hubRegistrationHash(request)
	if err != nil {
		return err
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrOwnerStopped
	}
	if existing := h.accounts[request.Account]; existing != nil {
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.ctx.Done():
			return ErrOwnerStopped
		case <-existing.done:
			if existing.err != nil || existing.hash == hash {
				return existing.err
			}
			if existing.env != request.Env || existing.binding.RegisterAdditional == nil {
				return errors.New("execution: hub account registration conflicts with its existing configuration")
			}
			return h.use(ctx, request.Account, func(_ *SharedAccountBorrow) error {
				return existing.binding.RegisterAdditional(ctx, request)
			})
		}
	}
	// Persist only the account identity, never the registration's credentials.
	// A crash during initialization must leave an observable recovery obligation.
	if err := h.rememberAccountLocked(request); err != nil {
		h.mu.Unlock()
		return err
	}
	entry := &hubAccount{hash: hash, env: request.Env, done: make(chan struct{})}
	h.accounts[request.Account] = entry
	h.work.Add(1)
	h.mu.Unlock()
	defer h.work.Done()

	// Admission owns initialization. A dropped HTTP connection must not tear down
	// an accepted account, but stopping the hub cancels its initialization.
	request.Configuration = bytes.Clone(request.Configuration)
	binding, err := h.factory(h.ctx, request)
	if err == nil && (binding.Account == nil || binding.Shutdown == nil) {
		err = errors.New("execution: hub factory returned an incomplete account binding")
	}
	if err == nil && binding.Account.AccountKey() != request.Account {
		err = ErrOwnerToken
	}
	if err != nil && binding.Shutdown != nil {
		err = errors.Join(err, binding.Shutdown())
		binding = HubAccountBinding{}
	} else if err != nil && binding.Account != nil {
		// An invalid factory cannot transfer ownership without its shutdown
		// function. At least close the returned service instead of leaking it.
		err = errors.Join(err, binding.Account.Close())
		binding = HubAccountBinding{}
	}
	h.mu.Lock()
	entry.binding, entry.err = binding, err
	if err != nil {
		delete(h.accounts, request.Account)
	}
	closed := h.closed
	close(entry.done)
	h.mu.Unlock()
	if closed {
		return ErrOwnerStopped
	}
	return err
}

func (h *Hub) use(ctx context.Context, key AccountKey, call func(*SharedAccountBorrow) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrOwnerStopped
	}
	entry := h.accounts[key]
	if entry == nil {
		h.mu.Unlock()
		return errors.New("execution: hub account is not registered")
	}
	h.work.Add(1)
	h.mu.Unlock()
	defer h.work.Done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.ctx.Done():
		return ErrOwnerStopped
	case <-entry.done:
	}
	if entry.err != nil {
		return entry.err
	}
	borrow := entry.binding.Account.Borrow()
	defer borrow.Release()
	stop := context.AfterFunc(h.ctx, borrow.Stop)
	defer stop()
	return call(borrow)
}

func (h *Hub) Snapshot(ctx context.Context, key AccountKey) (AccountSnapshot, error) {
	var result AccountSnapshot
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		result, err = account.Snapshot(ctx)
		return err
	})
	return result, err
}

type hubTargetRevision struct {
	Hash    [32]byte
	Request StrategyRebalance
}

func (h *Hub) ClaimStrategyWriter(ctx context.Context, key AccountKey, claim StrategyWriterClaim) (StrategyWriterLease, error) {
	var lease StrategyWriterLease
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		lease, err = account.ClaimStrategyWriter(ctx, claim)
		return err
	})
	return lease, err
}

func (h *Hub) RebalanceStrategy(ctx context.Context, key AccountKey, request StrategyRebalance, _ int64) error {
	if request.ProposalHash != "" && !validProposalHash(request.ProposalHash) || len(request.ProposalState) > MaxPolicyStateBytes || len(request.ProposalState) > 0 && (!json.Valid(request.ProposalState) || request.ProposalHash == "") {
		return errors.New("execution: invalid hub target proposal state")
	}
	if !canonicalID(string(request.Strategy)) || !canonicalID(request.PlanID) {
		return errors.New("execution: invalid hub target identity")
	}
	if request.StrategyEpoch == 0 || request.StrategySequence == 0 {
		return errors.New("execution: hub targets require strategy writer epoch and sequence")
	}
	request.Requests = append([]InstrumentRebalance(nil), request.Requests...)
	for n := range request.Requests {
		request.Requests[n].Quote = VisibleQuote{}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(body)
	return h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.WithState(func(service *SharedAccount) error {
			ctx, cancel := joinedOperationContext(account.Context(), ctx)
			defer cancel()
			name := "hub-target:" + request.PlanID
			stored, err := service.store.StrategyCheckpoint(ctx, request.Strategy, name)
			var revision hubTargetRevision
			switch {
			case err == nil:
				if err := json.Unmarshal(stored, &revision); err != nil {
					return err
				}
				if revision.Hash != hash {
					return errors.New("execution: hub target identity reused with a different command")
				}
			case errors.Is(err, sql.ErrNoRows):
				now := time.Now().UnixMilli()
				if request.DecisionMS > now || request.ExpiresMS <= now {
					return errors.New("execution: hub target decision is future or expired")
				}
				for n := range request.Requests {
					input := &request.Requests[n]
					input.Quote, err = service.ReadQuote(ctx, input.Instrument.ID, now)
					if err != nil {
						return err
					}
				}
				request.DecisionMS = time.Now().UnixMilli()
				revision = hubTargetRevision{Hash: hash, Request: request}
			default:
				return err
			}
			body, err := json.Marshal(revision)
			if err != nil {
				return err
			}
			// Freeze owner-observed quotes with the command identity in the same
			// transaction as the plan, so lost ACK retries never change its hash.
			checkpoint := hubTargetCheckpoint(request.Strategy, name, body, revision.Request.ProposalState)
			prepared, err := service.PrepareStrategiesWithCheckpoints([]StrategyRebalance{revision.Request}, ctx, []StrategyCheckpoint{checkpoint})
			if err != nil {
				return err
			}
			return service.SendPrepared(prepared, time.Now().UnixMilli(), ctx)
		})
	})
}

func (h *Hub) ProjectionEvents(ctx context.Context, key AccountKey, name string, limit int) ([]CommittedEvent, error) {
	var result []CommittedEvent
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		result, err = account.ProjectionEvents(ctx, name, limit)
		return err
	})
	return result, err
}

func (h *Hub) AdvanceProjection(ctx context.Context, key AccountKey, name string, through int64) error {
	return h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.AdvanceProjection(ctx, name, through)
	})
}

func (h *Hub) ObservePendingFeed(ctx context.Context, key AccountKey, observation PendingFeedObservation) (bool, error) {
	var accepted bool
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		accepted, err = account.ObservePendingFeed(ctx, observation)
		return err
	})
	return accepted, err
}

func (h *Hub) ApplyPendingCommand(ctx context.Context, key AccountKey, command PendingCommand) (int64, error) {
	if command.StrategyEpoch == 0 || command.StrategySequence == 0 {
		return 0, errors.New("execution: hub pending commands require strategy writer epoch and sequence")
	}
	var revision int64
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		revision, err = h.applyPendingWithFeedAdmission(ctx, account, command)
		return err
	})
	return revision, err
}

func (h *Hub) PendingOrders(ctx context.Context, key AccountKey) ([]PendingDefinition, error) {
	var book []PendingDefinition
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		book, err = account.PendingOrders(ctx)
		return err
	})
	return book, err
}

// ApplyTimeSeriesExit resolves the selector and shared quantity budget at the
// account owner, not by issuing independent per-lot RPC reductions.
func (h *Hub) ApplyTimeSeriesExit(ctx context.Context, key AccountKey, command TimeSeriesExitCommand) ([]TimeSeriesExitResult, error) {
	if command.StrategyEpoch == 0 || command.StrategySequence == 0 {
		return nil, errors.New("execution: hub exits require strategy writer epoch and sequence")
	}
	var result []TimeSeriesExitResult
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		result, err = account.ApplyTimeSeriesExit(ctx, command, time.Now().UnixMilli())
		return err
	})
	return result, err
}

func (h *Hub) Close() error {
	h.closeOnce.Do(func() {
		h.mu.Lock()
		h.closed = true
		h.cancel()
		h.mu.Unlock()
		h.work.Wait()
		for _, entry := range h.accounts {
			if entry.binding.Shutdown != nil {
				h.closeErr = errors.Join(h.closeErr, entry.binding.Shutdown())
			}
		}
	})
	return h.closeErr
}
