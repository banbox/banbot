package base

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestCsvFilePathRejectsTraversal(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../secret.csv", "/tmp/secret.csv", "nested/secret.csv", `nested\secret.csv`, "secret.txt"} {
		t.Run(name, func(t *testing.T) {
			if _, err := csvFilePath(t.TempDir(), name); err == nil {
				t.Fatalf("csvFilePath accepted %q", name)
			}
		})
	}

	dir := t.TempDir()
	path, err := csvFilePath(dir, "prices.CSV")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(path, "/prices.CSV") {
		t.Fatalf("path = %q", path)
	}
}

func TestCsvDataRejectsTraversal(t *testing.T) {
	app := fiber.New()
	RegApiCsvAt(app, t.TempDir())
	body, err := json.Marshal(CsvDataArgs{Name: "../secret.csv", StartMS: 1, EndMS: 2, TFSecs: 1})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/csv/data", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusBadRequest)
	}
}

func TestCsvUploadRejectsBackslashPath(t *testing.T) {
	app := fiber.New()
	RegApiCsvAt(app, t.TempDir())
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", `..\secret.csv`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("time,value\n1,2\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/csv/upload", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusBadRequest)
	}
}
