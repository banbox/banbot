package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// AccountKey identifies a venue session/account/settlement boundary, never a
// Runtime. VenueSessionIdentity must be a stable, non-secret environment ID.
type AccountKey struct {
	VenueSessionIdentity string
	Account              string
	SettlementDomain     string
}

func (k AccountKey) Validate() error {
	for _, value := range []string{k.VenueSessionIdentity, k.Account, k.SettlementDomain} {
		if value == "" || strings.TrimSpace(value) != value {
			return fmt.Errorf("execution: account identity requires three non-empty canonical fields")
		}
	}
	return nil
}

var (
	ErrOwnerStopped = errors.New("execution: account owner stopped")
	ErrReleased     = errors.New("execution: account handle released")
	ErrOwnerToken   = errors.New("execution: account identity or generation mismatch")
	nextGeneration  atomic.Uint64
)

type OwnerToken struct {
	Key        AccountKey
	Generation uint64
}

// AccountRegistry is owned by a Process. Its zero value is ready for use.
// It deliberately has no reopen/takeover API: in-process generation checks
// cannot fence another process's network writes.
type AccountRegistry struct {
	mu      sync.Mutex
	owners  map[AccountKey]*accountOwner
	stopped bool
}

type accountOwner struct {
	token   OwnerToken
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	stopped bool
	serial  sync.Mutex
	work    sync.WaitGroup
}

// AccountHandle borrows an owner. Release never stops the shared owner.
type AccountHandle struct {
	owner    *accountOwner
	released atomic.Bool
}

func (r *AccountRegistry) Acquire(key AccountKey) (*AccountHandle, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrOwnerStopped
	}
	if r.owners == nil {
		r.owners = make(map[AccountKey]*accountOwner)
	}
	owner := r.owners[key]
	if owner == nil {
		ctx, cancel := context.WithCancel(context.Background())
		owner = &accountOwner{token: OwnerToken{key, nextGeneration.Add(1)}, ctx: ctx, cancel: cancel}
		r.owners[key] = owner
	}
	return &AccountHandle{owner: owner}, nil
}

func (h *AccountHandle) Token() OwnerToken {
	if h == nil || h.owner == nil {
		return OwnerToken{}
	}
	return h.owner.token
}

func (h *AccountHandle) Release() {
	if h != nil {
		h.released.Store(true)
	}
}

// DoLocal serializes local owner work and joins its callback at shutdown.
// This is NOT a network-write API. Submit/cancel/native triggers require the
// persistent outbox/attempt gate and adapter introduced by the execution stage.
// Callbacks must observe ctx cancellation and must not call registry.Join.
func (h *AccountHandle) DoLocal(token OwnerToken, call func(context.Context) error) error {
	if h == nil || h.owner == nil || h.released.Load() {
		return ErrReleased
	}
	owner := h.owner
	owner.mu.Lock()
	if token != owner.token {
		owner.mu.Unlock()
		return ErrOwnerToken
	}
	if owner.stopped {
		owner.mu.Unlock()
		return ErrOwnerStopped
	}
	if call == nil {
		owner.mu.Unlock()
		return errors.New("execution: nil owner callback")
	}
	owner.work.Add(1)
	owner.mu.Unlock()
	defer owner.work.Done()
	owner.serial.Lock()
	defer owner.serial.Unlock()
	owner.mu.Lock()
	stopped := owner.stopped
	owner.mu.Unlock()
	if stopped {
		return ErrOwnerStopped
	}
	if h.released.Load() {
		return ErrReleased
	}
	return call(owner.ctx)
}

// Stop closes admission and cancels accepted callbacks without waiting.
func (r *AccountRegistry) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	for _, owner := range r.owners {
		owner.mu.Lock()
		owner.stopped = true
		owner.cancel()
		owner.mu.Unlock()
	}
}

// Join waits for accepted work, including work queued behind a slow callback.
// Stop must precede Join; Close is the ordinary owner-side shutdown boundary.
func (r *AccountRegistry) Join() {
	r.mu.Lock()
	owners := make([]*accountOwner, 0, len(r.owners))
	for _, owner := range r.owners {
		owners = append(owners, owner)
	}
	r.mu.Unlock()
	for _, owner := range owners {
		owner.work.Wait()
	}
}

func (r *AccountRegistry) Close() {
	r.Stop()
	r.Join()
}
