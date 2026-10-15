package runtime

import (
	"errors"
	"fmt"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/execution"
)

// BorrowAccount assembles an account service at the process boundary. Engines
// release their borrow; Process remains responsible for the final stop/join.
func (p *Process) BorrowAccount(key execution.AccountKey, opts execution.SharedExecutionOptions) (*execution.SharedAccountBorrow, error) {
	if p == nil {
		return nil, errors.New("runtime: process is required")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := p.beginRuntimeConstruction(); err != nil {
		return nil, err
	}
	defer p.finishRuntimeConstruction()
	borrow, err := p.acquireSharedAccount(key, opts)
	if err == nil {
		p.runtimeMu.Lock()
		p.hasAccountOwners = true
		if !p.registered {
			p.registered = true
			registerActiveProcess(p)
		}
		p.runtimeMu.Unlock()
	}
	return borrow, err
}

func (p *Process) acquireSharedAccount(key execution.AccountKey, opts biz.SharedExecutionOptions) (*biz.SharedAccountBorrow, error) {
	p.sharedAccountMu.Lock()
	defer p.sharedAccountMu.Unlock()
	if p.legacySenders[execution.SenderIdentity(key)] != nil {
		return nil, fmt.Errorf("runtime: physical account already has a legacy TS sender")
	}
	for existingKey := range p.sharedAccounts {
		if existingKey != key && execution.SenderIdentity(existingKey) == execution.SenderIdentity(key) {
			return nil, fmt.Errorf("runtime: physical account cannot own independent settlement senders")
		}
	}
	if existing := p.sharedAccounts[key]; existing != nil {
		if !existing.Matches(opts) {
			return nil, fmt.Errorf("runtime: same account cannot bind another store, lease directory or adapter")
		}
		return existing.Borrow(), nil
	}
	keeper, err := p.accountOwners.Acquire(key)
	if err != nil {
		return nil, err
	}
	service, err := biz.NewSharedAccount(keeper, opts)
	if err != nil {
		keeper.Release()
		return nil, err
	}
	if p.sharedAccounts == nil {
		p.sharedAccounts = make(map[execution.AccountKey]*biz.SharedAccount)
	}
	p.sharedAccounts[key] = service
	return service.Borrow(), nil
}

func (p *Process) closeSharedAccounts() error {
	p.sharedAccountMu.Lock()
	services := p.sharedAccounts
	legacy := p.legacySenders
	p.sharedAccounts = nil
	p.legacySenders = nil
	p.sharedAccountMu.Unlock()
	var result error
	for _, service := range services {
		result = errors.Join(result, service.Close())
	}
	for _, binding := range legacy {
		result = errors.Join(result, binding.sender.Close())
	}
	return result
}

// CloseError is retained across repeated Close calls, including adapter, store
// and sender lease failures. Call Close before reading the completed result.
func (p *Process) CloseError() error {
	if p == nil {
		return nil
	}
	p.runtimeMu.Lock()
	defer p.runtimeMu.Unlock()
	return p.closeErr
}

// SharedExecution returns this runtime's borrowed facade. Service identity is
// shared by AccountKey; releasing one Runtime does not close that service.
func (r *Runtime) SharedExecution() *biz.SharedAccountBorrow { return r.sharedExecution }
