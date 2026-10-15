package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cloudwego/eino/schema"
)

// Exercise the HTTP contract of both Eino-backed model implementations without
// connecting to a provider or requiring credentials.
func TestChatModelDependencyCompatibility(t *testing.T) {
	for _, apiType := range []string{"openai", "glm-plan"} {
		t.Run(apiType, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				if r.Header.Get("Authorization") != "Bearer test-key" {
					t.Error("API key was not forwarded")
				}
				var body struct {
					Model       string           `json:"model"`
					Messages    []schema.Message `json:"messages"`
					Temperature float64          `json:"temperature"`
					TestField   string           `json:"test_field"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if body.Model != "test-model" || body.TestField != "preserved" {
					t.Errorf("model or extra payload lost: %+v", body)
				}
				wantContent := "hello"
				if apiType == "glm-plan" {
					wantContent += " /nothink"
				}
				if len(body.Messages) != 1 || body.Messages[0].Role != schema.User || body.Messages[0].Content != wantContent {
					t.Errorf("unexpected messages: %+v", body.Messages)
				}
				if apiType == "openai" && (body.Temperature < 0.19 || body.Temperature > 0.21) {
					t.Errorf("temperature lost: %v", body.Temperature)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"test-response","object":"chat.completion","model":"test-model","choices":[{"index":0,"message":{"role":"assistant","content":"hello back"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
			}))
			defer server.Close()

			temperature := 0.2
			chat, err := createChatModelFromConfig(&LLMModelConfig{
				APIType: apiType, Name: "test-model", APIKey: "test-key",
				BaseURL: server.URL + "/v1", Temperature: &temperature,
				Payload: map[string]interface{}{"test_field": "preserved"},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response, err := chat.Generate(ctx, []*schema.Message{schema.UserMessage("hello")})
			if err != nil {
				t.Fatal(err)
			}
			if response.Role != schema.Assistant || response.Content != "hello back" {
				t.Fatalf("unexpected response: %+v", response)
			}
			if response.ResponseMeta == nil || response.ResponseMeta.Usage == nil ||
				response.ResponseMeta.Usage.TotalTokens != 5 || response.ResponseMeta.FinishReason != "stop" {
				t.Fatalf("response metadata lost: %+v", response.ResponseMeta)
			}
		})
	}
}
