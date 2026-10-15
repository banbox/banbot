package exg

import (
	"strings"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type archiveURLStub struct {
	banexg.BanExchange
	url string
}

func (s *archiveURLStub) BuildArchiveURL(string, string, string, string) (string, *errs.Error) {
	return s.url, nil
}

func TestAccountDownloadRequiresCapability(t *testing.T) {
	exchange := &symbolMarketStub{}

	if res, err := StartAccountDownload(exchange, "orders", "account", 1, 2, 3); err == nil || res != nil || err.Code != errs.CodeNotSupport || !strings.Contains(err.Error(), "account download") {
		t.Fatalf("start without capability = %v/%v, want CodeNotSupport", res, err)
	}
	if res, err := GetAccountDownload(exchange, "orders", "account", "download", 3); err == nil || res != nil || err.Code != errs.CodeNotSupport || !strings.Contains(err.Error(), "account download") {
		t.Fatalf("get without capability = %v/%v, want CodeNotSupport", res, err)
	}
}

func TestBuildArchiveURLRequiresCapability(t *testing.T) {
	if url, err := BuildArchiveURLForExchange(&symbolMarketStub{}, "spot", "trades", "BTCUSDT", "2025-01-01"); err == nil || url != "" || err.Code != errs.CodeNotSupport || !strings.Contains(err.Error(), "archive") {
		t.Fatalf("archive without capability = %q/%v, want CodeNotSupport", url, err)
	}
}

func TestGetArchiveURLCapabilityUnwrapsBotExchange(t *testing.T) {
	underlying := &archiveURLStub{url: "https://example.test/archive"}
	capability := GetArchiveURLCapability(&BotExchange{BanExchange: underlying})
	if capability == nil {
		t.Fatal("wrapped archive capability is nil")
	}
	url, err := capability.BuildArchiveURL("spot", "trades", "BTCUSDT", "2025-01-01")
	if err != nil || url != underlying.url {
		t.Fatalf("wrapped archive capability = %q/%v, want %q", url, err, underlying.url)
	}
}
