package biz

import "github.com/banbox/banbot/execution"

// The account service belongs to execution. These aliases keep TS consumers
// source-compatible while order managers retain strategy views only.
type SharedExecutionOptions = execution.SharedExecutionOptions
type SharedAccount = execution.SharedAccount
type SharedAccountBorrow = execution.SharedAccountBorrow

func NewSharedAccount(owner *execution.AccountHandle, opts SharedExecutionOptions) (*SharedAccount, error) {
	return execution.NewSharedAccount(owner, opts)
}
