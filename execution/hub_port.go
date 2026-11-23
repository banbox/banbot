package execution

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
)

// HubPort is the robot-side command/query surface of a Hub account: exactly
// what biz.HubOrderMgr and biz.HubAccountReader use. *HubClient (Unix socket)
// and *LocalHubPort (same process) implement it with identical error
// classification, so a robot behaves the same over either.
type HubPort interface {
	Snapshot(ctx context.Context, account AccountKey) (AccountSnapshot, error)
	ApplyPendingCommand(ctx context.Context, account AccountKey, command PendingCommand) (int64, error)
	ApplyTimeSeriesExit(ctx context.Context, account AccountKey, command TimeSeriesExitCommand) ([]TimeSeriesExitResult, error)
	BindTimeSeriesTakeover(ctx context.Context, account AccountKey, binding TimeSeriesTakeoverBinding) error
	PendingOrders(ctx context.Context, account AccountKey) ([]PendingDefinition, error)
	VisibleQuote(ctx context.Context, account AccountKey, instrument string) (VisibleQuote, error)
	DefaultLimitPrice(ctx context.Context, account AccountKey, instrument string, side OrderSide, seconds int) (float64, error)
	ClaimStrategyWriter(ctx context.Context, account AccountKey, request StrategyWriterClaim) (StrategyWriterLease, error)
	WriterLease(ctx context.Context, account AccountKey, strategy StrategyID) (HubWriterLease, error)
	SetLossStopPolicy(ctx context.Context, account AccountKey, policy LossStopPolicy) error
	LossStopStatus(ctx context.Context, account AccountKey) (LossStopStatus, error)
	SetAdmissionPolicy(ctx context.Context, account AccountKey, registrant string, policy AdmissionPolicy) error
	AdmissionStatus(ctx context.Context, account AccountKey) (AdmissionStatus, error)
	AdvanceProjection(ctx context.Context, account AccountKey, name string, through int64) error
	TimeSeriesProjection(ctx context.Context, account AccountKey, request TimeSeriesProjectionRequest) (TimeSeriesProjectionPage, error)
}

var (
	_ HubPort = (*HubClient)(nil)
	_ HubPort = (*LocalHubPort)(nil)
)

// LocalHubPort serves a Hub hosted in this process through the SAME wire
// contract as the Unix socket: every call is JSON-encoded, answered by the
// HubServer request handler and JSON-decoded by HubClient. Error mapping
// (venue minimum, allowlisted rejections, recovery flags, generic failure
// without error text), request size limits, and copy semantics (no pointer or
// slice shared with the owner) are therefore identical by construction. Only
// the socket and HTTP transport are skipped. Close releases nothing; the Hub
// stays owned by its host.
type LocalHubPort struct {
	*HubClient
	server *HubServer
}

// NewLocalHubPort returns nil for a nil hub.
func NewLocalHubPort(hub *Hub) *LocalHubPort {
	if hub == nil {
		return nil
	}
	server := &HubServer{hub: hub}
	transport := &http.Transport{}
	client := &HubClient{transport: transport, client: &http.Client{Transport: localHubTransport{server}, Timeout: hubClientTimeout}}
	return &LocalHubPort{HubClient: client, server: server}
}

// SetErrorObserver is the in-process counterpart of HubServer.SetErrorObserver.
func (p *LocalHubPort) SetErrorObserver(observe func(path string, err error)) {
	p.server.SetErrorObserver(observe)
}

type localHubTransport struct{ server *HubServer }

func (t localHubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	w := &localHubWriter{header: http.Header{}, status: http.StatusOK}
	t.server.serveHTTP(w, r)
	return &http.Response{
		StatusCode: w.status, Status: strconv.Itoa(w.status), Header: w.header, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Body: io.NopCloser(bytes.NewReader(w.body.Bytes())), ContentLength: int64(w.body.Len()), Request: r,
	}, nil
}

type localHubWriter struct {
	header http.Header
	status int
	wrote  bool
	body   bytes.Buffer
}

func (w *localHubWriter) Header() http.Header { return w.header }
func (w *localHubWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status, w.wrote = code, true
	}
}
func (w *localHubWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.body.Write(p)
}
