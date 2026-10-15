package dev

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestHandleDataToolsReleasesAdmissionAfterExchangeFailure(t *testing.T) {
	app := fiber.New()
	server := newDevServer(DevDeps{})
	app.Post("/data_tools", server.handleDataTools)
	req := httptest.NewRequest("POST", "/data_tools", strings.NewReader(`{"action":"download","exchange":"missing-exchange","market":"spot","pairs":["BTC/USDT"],"periods":["1m"],"startMs":1,"force":true}`))
	req.Header.Set("Content-Type", "application/json")
	if _, err := app.Test(req); err != nil {
		t.Fatalf("data tools request failed: %v", err)
	}

	if err := server.dataTools.StartTask(); err != nil {
		t.Fatalf("data tools admission remained claimed after synchronous failure: %v", err)
	}
	server.dataTools.EndTask()
}
