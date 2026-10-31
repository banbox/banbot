package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const hubMaxRequestBytes = 4 << 20

type hubRPCRequest struct {
	Account      AccountKey
	Registration *HubAccountRegistration
	Rebalance    *StrategyRebalance
	NowMS        int64
	Projection   string
	Limit        int
	Through      int64
	Feed         *PendingFeedObservation
	Pending      *PendingCommand
	TSProjection *TimeSeriesProjectionRequest
	TSExit       *TimeSeriesExitCommand
	Policy       *PolicyAcceptance
	Strategy     StrategyID
	PolicyResume *HubPolicyResume
	WriterClaim  *StrategyWriterClaim
	Instrument   *Instrument
	InstrumentID string
	Option       bool
	Contributor  *HubContributorRegistration
	PolicyCancel *HubPolicyEntriesCancel
	TargetResume *HubTargetResume
	Side         OrderSide
	WaitSeconds  int
}

type hubRPCResponse struct {
	CurrentWriter                *HubWriterLease           `json:",omitempty"`
	Snapshot                     *AccountSnapshot          `json:",omitempty"`
	Events                       []CommittedEvent          `json:",omitempty"`
	Accepted                     bool                      `json:",omitempty"`
	Revision                     int64                     `json:",omitempty"`
	Pending                      []PendingDefinition       `json:",omitempty"`
	TSProjection                 *TimeSeriesProjectionPage `json:",omitempty"`
	TSExits                      []TimeSeriesExitResult    `json:",omitempty"`
	Policy                       *PolicyReceipt            `json:",omitempty"`
	PolicyEvidence               *PolicyEvidence           `json:",omitempty"`
	PolicyFound                  bool                      `json:",omitempty"`
	PolicySendFailed             bool                      `json:",omitempty"`
	EvidenceChanged              bool                      `json:",omitempty"`
	WriterLease                  *StrategyWriterLease      `json:",omitempty"`
	Recovery                     []HubRecoveryStatus       `json:",omitempty"`
	Risk                         *PortfolioRisk            `json:",omitempty"`
	Totals                       *StrategyTotals           `json:",omitempty"`
	Quote                        *VisibleQuote             `json:",omitempty"`
	LimitPrice                   float64                   `json:",omitempty"`
	Settled                      bool                      `json:",omitempty"`
	CancellationRecoveryRequired bool                      `json:",omitempty"`
	TargetRecovery               *HubTargetRecovery        `json:",omitempty"`
	TargetSendFailed             bool                      `json:",omitempty"`
}

// HubServer uses a private local socket, not the spider's public subscription
// broadcast. Possession of this same-user socket authorizes account operations.
type HubServer struct {
	hub       *Hub
	server    *http.Server
	listener  *net.UnixListener
	path      string
	identity  os.FileInfo
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func ListenHubUnix(path string, hub *Hub) (*HubServer, error) {
	if hub == nil || !filepath.IsAbs(path) {
		return nil, errors.New("execution: hub requires an absolute private socket path")
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !parent.IsDir() || parent.Mode().Perm()&0022 != 0 {
		return nil, errors.New("execution: hub socket directory must not be group/world writable")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err // Never remove an existing socket to take ownership.
	}
	listener.SetUnlinkOnClose(false)
	identity, err := os.Lstat(path)
	if err == nil {
		err = os.Chmod(path, 0600)
	}
	if err != nil {
		_ = listener.Close()
		if current, statErr := os.Lstat(path); statErr == nil && identity != nil && os.SameFile(identity, current) {
			_ = os.Remove(path)
		}
		return nil, err
	}
	result := &HubServer{hub: hub, listener: listener, path: path, identity: identity, done: make(chan struct{})}
	result.server = &http.Server{Handler: http.HandlerFunc(result.serveHTTP), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		defer close(result.done)
		_ = result.server.Serve(listener)
	}()
	return result, nil
}

func (s *HubServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, hubMaxRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request hubRPCRequest
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid hub request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid hub request", http.StatusBadRequest)
		return
	}
	var result hubRPCResponse
	var err error
	switch r.URL.Path {
	case "/v1/writer-lease":
		var lease HubWriterLease
		lease, err = s.hub.WriterLease(r.Context(), request.Account, request.Strategy)
		result.CurrentWriter = &lease
	case "/v1/register":
		if request.Registration == nil {
			err = errors.New("missing registration")
		} else {
			err = s.hub.Register(r.Context(), *request.Registration)
		}
	case "/v1/register-contributor":
		if request.Contributor == nil {
			err = errors.New("missing contributor registration")
		} else {
			err = s.hub.RegisterContributor(r.Context(), request.Account, *request.Contributor)
		}
	case "/v1/snapshot":
		var snapshot AccountSnapshot
		snapshot, err = s.hub.Snapshot(r.Context(), request.Account)
		result.Snapshot = &snapshot
	case "/v1/recovery_status":
		result.Recovery, err = s.hub.RecoveryStatuses(r.Context())
	case "/v1/account-risk":
		var risk PortfolioRisk
		risk, err = s.hub.AccountRisk(r.Context(), request.Account)
		result.Risk = &risk
	case "/v1/strategy-totals":
		var totals StrategyTotals
		totals, err = s.hub.StrategyTotals(r.Context(), request.Account, request.Strategy)
		result.Totals = &totals
	case "/v1/visible-quote":
		var quote VisibleQuote
		quote, err = s.hub.VisibleQuote(r.Context(), request.Account, request.InstrumentID)
		result.Quote = &quote
	case "/v1/default-limit-price":
		result.LimitPrice, err = s.hub.DefaultLimitPrice(r.Context(), request.Account, request.InstrumentID, request.Side, request.WaitSeconds)
	case "/v1/instrument-settled":
		if request.Instrument == nil {
			err = errors.New("missing settlement instrument")
		} else {
			result.Settled, err = s.hub.InstrumentSettled(r.Context(), request.Account, *request.Instrument, request.Option)
		}
	case "/v1/rebalance":
		if request.Rebalance == nil {
			err = errors.New("missing rebalance")
		} else {
			err = s.hub.RebalanceStrategy(r.Context(), request.Account, *request.Rebalance, request.NowMS)
		}
	case "/v1/target-resume":
		if request.TargetResume == nil {
			err = errors.New("missing strategy target resume identity")
		} else {
			var recovery HubTargetRecovery
			recovery, err = s.hub.ResumeStrategyTarget(r.Context(), request.Account, *request.TargetResume)
			result.TargetRecovery = &recovery
		}
	case "/v1/events":
		result.Events, err = s.hub.ProjectionEvents(r.Context(), request.Account, request.Projection, request.Limit)
	case "/v1/advance":
		err = s.hub.AdvanceProjection(r.Context(), request.Account, request.Projection, request.Through)
	case "/v1/feed":
		if request.Feed == nil {
			err = errors.New("missing feed")
		} else {
			result.Accepted, err = s.hub.ObservePendingFeed(r.Context(), request.Account, *request.Feed)
		}
	case "/v1/pending-command":
		if request.Pending == nil {
			err = errors.New("missing pending command")
		} else {
			result.Revision, err = s.hub.ApplyPendingCommand(r.Context(), request.Account, *request.Pending)
		}
	case "/v1/pending-orders":
		result.Pending, err = s.hub.PendingOrders(r.Context(), request.Account)
	case "/v1/ts-projection":
		if request.TSProjection == nil {
			err = errors.New("missing time-series projection")
		} else {
			var page TimeSeriesProjectionPage
			page, err = s.hub.TimeSeriesProjection(r.Context(), request.Account, *request.TSProjection)
			result.TSProjection = &page
		}
	case "/v1/ts-exit":
		if request.TSExit == nil {
			err = errors.New("missing time-series exit command")
		} else {
			result.TSExits, err = s.hub.ApplyTimeSeriesExit(r.Context(), request.Account, *request.TSExit)
		}
	case "/v1/claim-writer":
		if request.WriterClaim == nil {
			err = errors.New("missing strategy writer claim")
		} else {
			var lease StrategyWriterLease
			lease, err = s.hub.ClaimStrategyWriter(r.Context(), request.Account, *request.WriterClaim)
			result.WriterLease = &lease
		}
	case "/v1/policy-accept":
		if request.Policy == nil {
			err = errors.New("missing policy acceptance")
		} else {
			var receipt PolicyReceipt
			receipt, err = s.hub.AcceptPolicyBatch(r.Context(), request.Account, *request.Policy)
			result.Policy, result.PolicySendFailed = &receipt, receipt.SendError != nil
		}
	case "/v1/policy-evidence":
		var evidence PolicyEvidence
		evidence, err = s.hub.PolicyEvidence(r.Context(), request.Account, request.Strategy)
		result.PolicyEvidence = &evidence
	case "/v1/policy-cancel-entries":
		if request.PolicyCancel == nil {
			err = errors.New("missing policy entries cancellation")
		} else {
			err = s.hub.CancelPolicyEntries(r.Context(), request.Account, *request.PolicyCancel)
		}
	case "/v1/policy-resume":
		if request.PolicyResume == nil {
			err = errors.New("missing policy resume identity")
		} else {
			var receipt PolicyReceipt
			receipt, result.PolicyFound, err = s.hub.ResumePolicy(r.Context(), request.Account, *request.PolicyResume)
			result.Policy, result.PolicySendFailed = &receipt, receipt.SendError != nil
		}
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		if result.Policy != nil && result.Policy.Accepted {
			result.PolicySendFailed = true
			_ = json.NewEncoder(w).Encode(result)
			return
		}
		if errors.Is(err, ErrPolicyCancellationRecoveryRequired) {
			result.CancellationRecoveryRequired = true
			_ = json.NewEncoder(w).Encode(result)
			return
		}
		if result.TargetRecovery != nil && result.TargetRecovery.Found {
			result.TargetSendFailed = true
			_ = json.NewEncoder(w).Encode(result)
			return
		}
		if errors.Is(err, ErrPolicyEvidenceChanged) {
			result.EvidenceChanged = true
			_ = json.NewEncoder(w).Encode(result)
			return
		}
		// Factory/SDK errors may embed credentials. Never expose their text.
		http.Error(w, "hub operation failed; inspect account recovery state before retrying", http.StatusConflict)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}

func (s *HubServer) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.server.Close()
		<-s.done
		s.closeErr = errors.Join(s.closeErr, s.hub.Close())
		if current, err := os.Lstat(s.path); err == nil && os.SameFile(s.identity, current) {
			s.closeErr = errors.Join(s.closeErr, os.Remove(s.path))
		}
	})
	return s.closeErr
}

type HubClient struct {
	client    *http.Client
	transport *http.Transport
}

func NewHubClient(socketPath string) *HubClient {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
	return &HubClient{client: &http.Client{Transport: transport, Timeout: 30 * time.Second}, transport: transport}
}

// Close only releases client connections. Accepted hub accounts remain active.
func (c *HubClient) Close() { c.transport.CloseIdleConnections() }

func (c *HubClient) call(ctx context.Context, method string, request hubRPCRequest) (hubRPCResponse, error) {
	var result hubRPCResponse
	body, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	if len(body) > hubMaxRequestBytes {
		return result, errors.New("execution: hub request exceeds maximum size")
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://hub/v1/"+method, bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	r.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, errors.New("execution: hub request failed; outcome may require recovery")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, errors.New("execution: hub operation failed; inspect account recovery state before retrying")
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(&result)
	if err == nil && result.CancellationRecoveryRequired {
		err = ErrPolicyCancellationRecoveryRequired
	}
	if err == nil && result.TargetSendFailed {
		err = errors.New("execution: accepted hub target send requires account recovery")
	}
	return result, err
}

func (c *HubClient) Register(ctx context.Context, request HubAccountRegistration) error {
	_, err := c.call(ctx, "register", hubRPCRequest{Registration: &request})
	return err
}

func (c *HubClient) RegisterContributor(ctx context.Context, account AccountKey, request HubContributorRegistration) error {
	_, err := c.call(ctx, "register-contributor", hubRPCRequest{Account: account, Contributor: &request})
	return err
}

func (c *HubClient) Snapshot(ctx context.Context, account AccountKey) (AccountSnapshot, error) {
	result, err := c.call(ctx, "snapshot", hubRPCRequest{Account: account})
	if err != nil {
		return AccountSnapshot{}, err
	}
	if result.Snapshot == nil {
		return AccountSnapshot{}, errors.New("execution: hub returned no snapshot")
	}
	return *result.Snapshot, nil
}

func (c *HubClient) RebalanceStrategy(ctx context.Context, account AccountKey, request StrategyRebalance, nowMS int64) error {
	_, err := c.call(ctx, "rebalance", hubRPCRequest{Account: account, Rebalance: &request, NowMS: nowMS})
	return err
}

func (c *HubClient) ResumeStrategyTarget(ctx context.Context, account AccountKey, request HubTargetResume) (HubTargetRecovery, error) {
	result, err := c.call(ctx, "target-resume", hubRPCRequest{Account: account, TargetResume: &request})
	if err != nil {
		return HubTargetRecovery{}, err
	}
	if result.TargetRecovery == nil {
		return HubTargetRecovery{}, errors.New("execution: hub returned no target recovery")
	}
	return *result.TargetRecovery, nil
}

func (c *HubClient) ProjectionEvents(ctx context.Context, account AccountKey, name string, limit int) ([]CommittedEvent, error) {
	result, err := c.call(ctx, "events", hubRPCRequest{Account: account, Projection: name, Limit: limit})
	return result.Events, err
}

func (c *HubClient) AdvanceProjection(ctx context.Context, account AccountKey, name string, through int64) error {
	_, err := c.call(ctx, "advance", hubRPCRequest{Account: account, Projection: name, Through: through})
	return err
}

func (c *HubClient) ObservePendingFeed(ctx context.Context, account AccountKey, observation PendingFeedObservation) (bool, error) {
	result, err := c.call(ctx, "feed", hubRPCRequest{Account: account, Feed: &observation})
	return result.Accepted, err
}

func (c *HubClient) ApplyPendingCommand(ctx context.Context, account AccountKey, command PendingCommand) (int64, error) {
	result, err := c.call(ctx, "pending-command", hubRPCRequest{Account: account, Pending: &command})
	return result.Revision, err
}

func (c *HubClient) PendingOrders(ctx context.Context, account AccountKey) ([]PendingDefinition, error) {
	result, err := c.call(ctx, "pending-orders", hubRPCRequest{Account: account})
	return result.Pending, err
}

func (c *HubClient) ApplyTimeSeriesExit(ctx context.Context, account AccountKey, command TimeSeriesExitCommand) ([]TimeSeriesExitResult, error) {
	result, err := c.call(ctx, "ts-exit", hubRPCRequest{Account: account, TSExit: &command})
	return result.TSExits, err
}

func (c *HubClient) ClaimStrategyWriter(ctx context.Context, account AccountKey, request StrategyWriterClaim) (StrategyWriterLease, error) {
	result, err := c.call(ctx, "claim-writer", hubRPCRequest{Account: account, WriterClaim: &request})
	if err != nil {
		return StrategyWriterLease{}, err
	}
	if result.WriterLease == nil {
		return StrategyWriterLease{}, errors.New("execution: hub returned no writer lease")
	}
	return *result.WriterLease, nil
}

func (c *HubClient) RecoveryStatuses(ctx context.Context) ([]HubRecoveryStatus, error) {
	result, err := c.call(ctx, "recovery_status", hubRPCRequest{})
	return result.Recovery, err
}

func (c *HubClient) AccountRisk(ctx context.Context, account AccountKey) (PortfolioRisk, error) {
	result, err := c.call(ctx, "account-risk", hubRPCRequest{Account: account})
	if err != nil {
		return PortfolioRisk{}, err
	}
	if result.Risk == nil {
		return PortfolioRisk{}, errors.New("execution: hub returned no account risk")
	}
	return *result.Risk, nil
}

func (c *HubClient) StrategyTotals(ctx context.Context, account AccountKey, strategy StrategyID) (StrategyTotals, error) {
	result, err := c.call(ctx, "strategy-totals", hubRPCRequest{Account: account, Strategy: strategy})
	if err != nil {
		return StrategyTotals{}, err
	}
	if result.Totals == nil {
		return StrategyTotals{}, errors.New("execution: hub returned no strategy totals")
	}
	return *result.Totals, nil
}

func (c *HubClient) VisibleQuote(ctx context.Context, account AccountKey, instrument string) (VisibleQuote, error) {
	result, err := c.call(ctx, "visible-quote", hubRPCRequest{Account: account, InstrumentID: instrument})
	if err != nil {
		return VisibleQuote{}, err
	}
	if result.Quote == nil {
		return VisibleQuote{}, errors.New("execution: hub returned no visible quote")
	}
	return *result.Quote, nil
}

func (c *HubClient) DefaultLimitPrice(ctx context.Context, account AccountKey, instrument string, side OrderSide, seconds int) (float64, error) {
	result, err := c.call(ctx, "default-limit-price", hubRPCRequest{Account: account, InstrumentID: instrument, Side: side, WaitSeconds: seconds})
	return result.LimitPrice, err
}

func (c *HubClient) InstrumentSettled(ctx context.Context, account AccountKey, instrument Instrument, option bool) (bool, error) {
	result, err := c.call(ctx, "instrument-settled", hubRPCRequest{Account: account, Instrument: &instrument, Option: option})
	return result.Settled, err
}

func decodeHubPolicyReceipt(result hubRPCResponse, err error) (PolicyReceipt, error) {
	var receipt PolicyReceipt
	if result.Policy != nil {
		receipt = *result.Policy
	}
	if err != nil {
		return receipt, err
	}
	if result.EvidenceChanged {
		return receipt, ErrPolicyEvidenceChanged
	}
	if result.Policy == nil {
		return receipt, errors.New("execution: hub returned no policy receipt")
	}
	if result.PolicySendFailed {
		receipt.SendError = errors.New("execution: accepted policy send requires account recovery; do not create another acceptance identity")
	}
	return receipt, nil
}

func (c *HubClient) AcceptPolicyBatch(ctx context.Context, account AccountKey, request PolicyAcceptance) (PolicyReceipt, error) {
	result, err := c.call(ctx, "policy-accept", hubRPCRequest{Account: account, Policy: &request})
	return decodeHubPolicyReceipt(result, err)
}

func (c *HubClient) CancelPolicyEntries(ctx context.Context, account AccountKey, request HubPolicyEntriesCancel) error {
	result, err := c.call(ctx, "policy-cancel-entries", hubRPCRequest{Account: account, PolicyCancel: &request})
	if err == nil && result.CancellationRecoveryRequired {
		return ErrPolicyCancellationRecoveryRequired
	}
	return err
}

func (c *HubClient) PolicyEvidence(ctx context.Context, account AccountKey, strategy StrategyID) (PolicyEvidence, error) {
	result, err := c.call(ctx, "policy-evidence", hubRPCRequest{Account: account, Strategy: strategy})
	if err != nil {
		return PolicyEvidence{}, err
	}
	if result.PolicyEvidence == nil {
		return PolicyEvidence{}, errors.New("execution: hub returned no policy evidence")
	}
	return *result.PolicyEvidence, nil
}

func (c *HubClient) ResumePolicy(ctx context.Context, account AccountKey, request HubPolicyResume) (PolicyReceipt, bool, error) {
	result, err := c.call(ctx, "policy-resume", hubRPCRequest{Account: account, PolicyResume: &request})
	receipt, err := decodeHubPolicyReceipt(result, err)
	return receipt, result.PolicyFound, err
}

func (c *HubClient) WriterLease(ctx context.Context, account AccountKey, strategy StrategyID) (HubWriterLease, error) {
	result, err := c.call(ctx, "writer-lease", hubRPCRequest{Account: account, Strategy: strategy})
	if err != nil {
		return HubWriterLease{}, err
	}
	if result.CurrentWriter == nil {
		return HubWriterLease{}, errors.New("execution: hub returned no current writer")
	}
	return *result.CurrentWriter, nil
}
