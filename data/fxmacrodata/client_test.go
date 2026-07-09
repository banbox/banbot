package fxmacrodata

import "testing"

func TestBuildURL(t *testing.T) {
	client := NewClient("test-key")
	got := client.buildURL("/announcements/usd/inflation", nil)
	want := "https://api.fxmacrodata.com/v1/announcements/usd/inflation?api_key=test-key"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
