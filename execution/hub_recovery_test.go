package execution

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHubRestartRecordsUnprotectedAccountWithoutPersistingCredentials(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "accounts.json")
	var calls, closed atomic.Int32
	factory := func(_ context.Context, request HubAccountRegistration) (HubAccountBinding, error) {
		calls.Add(1)
		return hubTestBinding(t, request.Account, &closed), nil
	}
	h, err := NewHubWithRecovery(factory, path)
	if err != nil {
		t.Fatal(err)
	}
	request := hubTestRegistration()
	if err := h.Register(ctx, request); err != nil {
		t.Fatal(err)
	}
	states, err := h.RecoveryStatuses(ctx)
	if err != nil || len(states) != 1 || states[0].State != HubAccountActive {
		t.Fatalf("active account: %+v %v", states, err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(data), "never-echo-this") || strings.Contains(string(data), "capital") || strings.Contains(string(data), "api_key") {
		t.Fatalf("registry leaked registration: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("registry permissions: %v %v", info, err)
	}
	h, err = NewHubWithRecovery(factory, path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	states, err = h.RecoveryStatuses(ctx)
	if err != nil || len(states) != 1 || states[0].State != HubWaitingAuthentication || states[0].Account != request.Account || calls.Load() != 1 {
		t.Fatalf("offline robot recovery silently active: %+v %v calls=%d", states, err, calls.Load())
	}
	if _, err := h.Snapshot(ctx, request.Account); err == nil {
		t.Fatal("unauthenticated recovery exposed an active account")
	}
	if err := h.Register(ctx, request); err != nil {
		t.Fatal(err)
	}
	states, err = h.RecoveryStatuses(ctx)
	if err != nil || states[0].State != HubAccountActive || calls.Load() != 2 {
		t.Fatalf("reauthentication failed: %+v %v", states, err)
	}
}

func TestHubRecoveryRetainsFailedInitializationAndFencesEnvironment(t *testing.T) {
	root := t.TempDir()
	_ = os.Chmod(root, 0700)
	path := filepath.Join(root, "accounts.json")
	var calls int
	factory := func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		calls++
		return HubAccountBinding{}, errors.New("private credential error")
	}
	h, err := NewHubWithRecovery(factory, path)
	if err != nil {
		t.Fatal(err)
	}
	request := hubTestRegistration()
	if err := h.Register(context.Background(), request); err == nil {
		t.Fatal("failed initialization accepted")
	}
	_ = h.Close()
	h, err = NewHubWithRecovery(factory, path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	rows, err := h.RecoveryStatuses(context.Background())
	if err != nil || len(rows) != 1 || rows[0].State != HubWaitingAuthentication {
		t.Fatalf("failed initialization recovery lost: %+v %v", rows, err)
	}
	request.Env = "prod"
	if err := h.Register(context.Background(), request); err == nil || calls != 1 {
		t.Fatal("recovered identity changed environment or reached factory")
	}
}

func TestHubRecoveryRejectsUnsafeOrCorruptRegistry(t *testing.T) {
	root := t.TempDir()
	_ = os.Chmod(root, 0700)
	factory := func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		t.Fatal("invalid registry opened private account")
		return HubAccountBinding{}, nil
	}
	path := filepath.Join(root, "accounts.json")
	for _, data := range []string{"not-json", "null", `[{"Account":{},"Env":"prod"}]`} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		h, err := NewHubWithRecovery(factory, path)
		if err == nil {
			_ = h.Close()
			t.Fatalf("accepted corrupt registry %s", data)
		}
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if h, err := NewHubWithRecovery(factory, path); err == nil {
		_ = h.Close()
		t.Fatal("accepted readable registry")
	}
	_ = os.Remove(path)
	if err := os.Symlink(filepath.Join(root, "missing"), path); err != nil {
		t.Fatal(err)
	}
	if h, err := NewHubWithRecovery(factory, path); err == nil {
		_ = h.Close()
		t.Fatal("accepted symlink registry")
	}
}

func TestHubRecoveryPersistenceFailurePreventsAccountStartup(t *testing.T) {
	root := t.TempDir()
	_ = os.Chmod(root, 0700)
	path := filepath.Join(root, "accounts.json")
	h, err := NewHubWithRecovery(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		t.Fatal("factory ran before recovery obligation was durable")
		return HubAccountBinding{}, nil
	}, path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := h.Register(context.Background(), hubTestRegistration()); err == nil {
		t.Fatal("accepted account without durable recovery registry")
	}
	states, err := h.RecoveryStatuses(context.Background())
	if err != nil || len(states) != 0 {
		t.Fatalf("failed persistence admitted account: %+v %v", states, err)
	}
}

func TestHubRecoveryStatusOverRPC(t *testing.T) {
	root := t.TempDir()
	_ = os.Chmod(root, 0700)
	path := filepath.Join(root, "accounts.json")
	var closed atomic.Int32
	factory := func(_ context.Context, request HubAccountRegistration) (HubAccountBinding, error) {
		return hubTestBinding(t, request.Account, &closed), nil
	}
	h, err := NewHubWithRecovery(factory, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Register(context.Background(), hubTestRegistration()); err != nil {
		t.Fatal(err)
	}
	_ = h.Close()
	h, err = NewHubWithRecovery(factory, path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	socket := hubTestSocket(t)
	server, err := ListenHubUnix(socket, h)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := NewHubClient(socket)
	defer client.Close()
	rows, err := client.RecoveryStatuses(context.Background())
	if err != nil || len(rows) != 1 || rows[0].State != HubWaitingAuthentication {
		t.Fatalf("RPC hid offline protection gap: %+v %v", rows, err)
	}
}
