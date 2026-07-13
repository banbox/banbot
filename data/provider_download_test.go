package data

import "testing"

func TestHistProviderDownloadGate(t *testing.T) {
	provider := NewHistProvider(nil, nil, nil, false, nil)
	provider.SetAllowDownload(false)
	if err := provider.downIfNeed(); err != nil {
		t.Fatalf("disabled download should not touch database or exchange: %v", err)
	}

	provider.SetAllowDownload(true)
	if !provider.allowDownload {
		t.Fatal("expected download preparation to enable provider downloads")
	}
}
