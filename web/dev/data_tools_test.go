package dev

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/legacygate"
	"github.com/gofiber/fiber/v2"
)

func TestRunDataToolsUsesLegacyGate(t *testing.T) {
	unlock := legacygate.Lock()
	done := make(chan error, 1)
	go func() {
		done <- RunDataTools(&DataToolsArgs{Action: "invalid"})
	}()

	select {
	case <-done:
		t.Fatal("RunDataTools entered while the legacy gate was held")
	case <-time.After(50 * time.Millisecond):
	}

	unlock()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("RunDataTools accepted an invalid action")
		}
	case <-time.After(time.Second):
		t.Fatal("RunDataTools did not enter after the legacy gate was released")
	}
}

func TestHandleDataToolsReleasesAdmissionAfterExchangeFailure(t *testing.T) {
	oldManager := dataToolsMgr
	dataToolsMgr = &DataToolsManager{}
	t.Cleanup(func() { dataToolsMgr = oldManager })

	app := fiber.New()
	app.Post("/data_tools", handleDataTools)
	req := httptest.NewRequest("POST", "/data_tools", strings.NewReader(`{"action":"download","exchange":"missing-exchange","market":"spot","pairs":["BTC/USDT"],"periods":["1m"],"startMs":1,"force":true}`))
	req.Header.Set("Content-Type", "application/json")
	if _, err := app.Test(req); err != nil {
		t.Fatalf("data tools request failed: %v", err)
	}

	if err := dataToolsMgr.StartTask(); err != nil {
		t.Fatalf("data tools admission remained claimed after synchronous failure: %v", err)
	}
	dataToolsMgr.EndTask()
}
