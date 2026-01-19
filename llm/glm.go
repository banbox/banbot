package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

type glmImplOptions struct {
	ReasoningEffort string
	ToolStream      bool
}

func WithReasoningEffort(e string) model.Option {
	return model.WrapImplSpecificOptFn(func(o *glmImplOptions) {
		o.ReasoningEffort = e
	})
}

func WithToolStream(enable bool) model.Option {
	return model.WrapImplSpecificOptFn(func(o *glmImplOptions) {
		o.ToolStream = enable
	})
}

type GLMChatModel struct {
	baseURL    string
	apiKey     string
	model      string
	httpClient *http.Client
	boundTools []*schema.ToolInfo
	payload    map[string]interface{}
}

func NewGLMChatModel(apiKey, baseURL, model string) *GLMChatModel {
	if apiKey == "" {
		for _, k := range []string{"ZAI_API_KEY", "ZAI_CODING_API_KEY", "GLM_API_KEY"} {
			if v := strings.TrimSpace(os.Getenv(k)); v != "" {
				apiKey = v
				break
			}
		}
	}
	if baseURL == "" {
		baseURL = strings.TrimSpace(os.Getenv("ZAI_API_BASE_URL"))
	}
	if baseURL == "" {
		baseURL = "https://open.bigmodel.cn/api/coding/paas/v4"
	}
	baseURL = strings.TrimRight(baseURL, "/")
	if model == "" {
		model = "GLM-4.6"
	}
	return &GLMChatModel{
		baseURL: baseURL,
		apiKey:  apiKey,
		model:   model,
		httpClient: &http.Client{
			Timeout: 300 * time.Second,
		},
		payload: make(map[string]interface{}),
	}
}

func (g *GLMChatModel) chatURL() string { return g.baseURL + "/chat/completions" }

func (g *GLMChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	if g == nil {
		return nil, fmt.Errorf("nil GLMChatModel")
	}
	if g.apiKey == "" {
		return nil, fmt.Errorf("missing Z.AI API key")
	}

	common := model.GetCommonOptions(&model.Options{Model: &g.model}, opts...)
	impl := model.GetImplSpecificOptions(&glmImplOptions{}, opts...)

	mdl := g.model
	if common.Model != nil && *common.Model != "" {
		mdl = *common.Model
	}
	if len(common.Tools) == 0 && len(g.boundTools) > 0 {
		common.Tools = g.boundTools
	}

	msgs := make([]map[string]string, 0, len(input))
	for _, m := range input {
		if m == nil {
			continue
		}
		role := string(m.Role)
		if role == "" {
			role = "user"
		}
		msgs = append(msgs, map[string]string{
			"role":    role,
			"content": m.Content,
		})
	}

	if impl.ReasoningEffort == "" {
		for i := len(msgs) - 1; i >= 0; i-- {
			if msgs[i]["role"] == "user" {
				if !strings.HasSuffix(msgs[i]["content"], " /nothink") {
					msgs[i]["content"] = msgs[i]["content"] + " /nothink"
				}
				break
			}
		}
	}

	req := map[string]any{
		"messages": msgs,
		"model":    mdl,
		"stream":   false,
	}
	if common.MaxTokens != nil && *common.MaxTokens > 0 {
		req["max_completion_tokens"] = *common.MaxTokens
	}
	if common.Temperature != nil {
		req["temperature"] = *common.Temperature
	}
	if common.TopP != nil {
		req["top_p"] = *common.TopP
	}
	if impl.ReasoningEffort != "" {
		req["reasoning_effort"] = impl.ReasoningEffort
	}
	if len(common.Tools) > 0 {
		if glmTools := toGLMTools(common.Tools); len(glmTools) > 0 {
			req["tools"] = glmTools
		}
		if common.ToolChoice != nil {
			if choice := convertToolChoice(*common.ToolChoice); choice != "" {
				req["tool_choice"] = choice
			}
		}
	}
	if len(common.Stop) > 0 {
		req["stop"] = common.Stop
	}

	for k, v := range g.payload {
		req[k] = v
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, g.chatURL(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)

	resp, err := g.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("unauthorized: check Z.AI API key")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("glm api error: status %d", resp.StatusCode)
	}

	var parsed map[string]any
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}

	content := ""
	var finishReason string
	var usage *schema.TokenUsage

	if choices, ok := parsed["choices"].([]any); ok && len(choices) > 0 {
		if ch, ok := choices[0].(map[string]any); ok {
			if msg, ok := ch["message"].(map[string]any); ok {
				if c, ok := msg["content"].(string); ok {
					content = c
				}
			}
			if content == "" {
				if c, ok := ch["content"].(string); ok {
					content = c
				}
			}
			if fr, ok := ch["finish_reason"].(string); ok {
				finishReason = fr
			}
		}
	}
	if content == "" {
		if c, ok := parsed["content"].(string); ok {
			content = c
		}
	}
	if content == "" {
		return nil, fmt.Errorf("empty content in response")
	}

	if usageData, ok := parsed["usage"].(map[string]any); ok {
		usage = &schema.TokenUsage{}
		if pt, ok := usageData["prompt_tokens"].(float64); ok {
			usage.PromptTokens = int(pt)
		}
		if ct, ok := usageData["completion_tokens"].(float64); ok {
			usage.CompletionTokens = int(ct)
		}
		if tt, ok := usageData["total_tokens"].(float64); ok {
			usage.TotalTokens = int(tt)
		}
		if ptd, ok := usageData["prompt_tokens_details"].(map[string]any); ok {
			if cached, ok := ptd["cached_tokens"].(float64); ok {
				usage.PromptTokenDetails.CachedTokens = int(cached)
			}
		}
	}

	var responseMeta *schema.ResponseMeta
	if finishReason != "" || usage != nil {
		responseMeta = &schema.ResponseMeta{
			FinishReason: finishReason,
			Usage:        usage,
		}
	}

	extra := make(map[string]any)
	if id, ok := parsed["id"].(string); ok && id != "" {
		extra["id"] = id
	}
	if requestID, ok := parsed["request_id"].(string); ok && requestID != "" {
		extra["request_id"] = requestID
	}
	if created, ok := parsed["created"].(float64); ok {
		extra["created"] = int64(created)
	}
	if model, ok := parsed["model"].(string); ok && model != "" {
		extra["model"] = model
	}

	out := &schema.Message{
		Role:         schema.Assistant,
		Content:      content,
		ResponseMeta: responseMeta,
	}
	if len(extra) > 0 {
		out.Extra = extra
	}
	return out, nil
}

func (g *GLMChatModel) Stream(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	if g == nil {
		return nil, fmt.Errorf("nil GLMChatModel")
	}
	if g.apiKey == "" {
		return nil, fmt.Errorf("missing Z.AI API key")
	}

	common := model.GetCommonOptions(&model.Options{Model: &g.model}, opts...)
	impl := model.GetImplSpecificOptions(&glmImplOptions{}, opts...)

	mdl := g.model
	if common.Model != nil && *common.Model != "" {
		mdl = *common.Model
	}
	if len(common.Tools) == 0 && len(g.boundTools) > 0 {
		common.Tools = g.boundTools
	}

	msgs := make([]map[string]string, 0, len(input))
	for _, m := range input {
		if m == nil {
			continue
		}
		role := string(m.Role)
		if role == "" {
			role = "user"
		}
		msgs = append(msgs, map[string]string{
			"role":    role,
			"content": m.Content,
		})
	}

	if impl.ReasoningEffort == "" {
		for i := len(msgs) - 1; i >= 0; i-- {
			if msgs[i]["role"] == "user" {
				if !strings.HasSuffix(msgs[i]["content"], " /nothink") {
					msgs[i]["content"] = msgs[i]["content"] + " /nothink"
				}
				break
			}
		}
	}

	req := map[string]any{
		"messages": msgs,
		"model":    mdl,
		"stream":   true,
		"stream_options": map[string]any{
			"include_usage": true,
		},
	}
	if common.MaxTokens != nil && *common.MaxTokens > 0 {
		req["max_completion_tokens"] = *common.MaxTokens
	}
	if common.Temperature != nil {
		req["temperature"] = *common.Temperature
	}
	if impl.ReasoningEffort != "" {
		req["reasoning_effort"] = impl.ReasoningEffort
	}
	if len(common.Tools) > 0 {
		if glmTools := toGLMTools(common.Tools); len(glmTools) > 0 {
			req["tools"] = glmTools
		}
		if common.ToolChoice != nil {
			if choice := convertToolChoice(*common.ToolChoice); choice != "" {
				req["tool_choice"] = choice
			}
		}
		if impl.ToolStream {
			req["tool_stream"] = true
		}
	}
	if len(common.Stop) > 0 {
		req["stop"] = common.Stop
	}

	for k, v := range g.payload {
		req[k] = v
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, g.chatURL(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := g.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusUnauthorized {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unauthorized: check Z.AI API key")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("glm api error: status %d", resp.StatusCode)
	}

	reader, writer := schema.Pipe[*schema.Message](32)

	go func() {
		defer func() {
			_ = resp.Body.Close()
			writer.Close()
		}()

		scanner := bufio.NewScanner(resp.Body)
		buf := make([]byte, 0, 1024*64)
		scanner.Buffer(buf, 1024*1024)

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			payload := strings.TrimPrefix(line, "data: ")
			if payload == "[DONE]" {
				return
			}
			var m map[string]any
			if err = json.Unmarshal([]byte(payload), &m); err != nil {
				continue
			}
			choices, ok := m["choices"].([]any)
			if !ok || len(choices) == 0 {
				continue
			}
			ch, _ := choices[0].(map[string]any)
			delta, _ := ch["delta"].(map[string]any)
			if delta == nil {
				continue
			}
			content, _ := delta["content"].(string)
			if content == "" && impl.ReasoningEffort == "" {
				if rc, ok := delta["reasoning_content"].(string); ok && rc != "" {
					content = strings.TrimSpace(rc)
				}
			}
			if content != "" {
				_ = writer.Send(&schema.Message{Role: schema.Assistant, Content: content}, nil)
			}
		}

		if err := scanner.Err(); err != nil {
			writer.Send(nil, err)
			writer.Close()
		}
	}()

	return reader, nil
}

func (g *GLMChatModel) WithTools(tools []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	if g == nil {
		return nil, fmt.Errorf("nil GLMChatModel")
	}
	ng := *g
	if len(tools) > 0 {
		ng.boundTools = append([]*schema.ToolInfo(nil), tools...)
	} else {
		ng.boundTools = nil
	}
	return &ng, nil
}

func convertToolChoice(tc schema.ToolChoice) string {
	switch tc {
	case "allowed":
		return "auto"
	case "forced":
		return "required"
	case "forbidden":
		return "none"
	default:
		return ""
	}
}

func toGLMTools(tools []*schema.ToolInfo) []map[string]any {
	if len(tools) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(tools))
	for _, t := range tools {
		if t == nil {
			continue
		}
		fn := map[string]any{
			"name":        t.Name,
			"description": t.Desc,
		}
		if t.ParamsOneOf != nil {
			if js, err := t.ParamsOneOf.ToJSONSchema(); err == nil && js != nil {
				fn["parameters"] = js
			}
		}
		out = append(out, map[string]any{
			"type":     "function",
			"function": fn,
		})
	}
	return out
}
