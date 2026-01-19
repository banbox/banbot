package llm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banexg/log"
	openaicomp "github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"go.uber.org/zap"
)

const (
	// DefaultConcurrencyLimit is the default concurrency limit for each model.
	DefaultConcurrencyLimit = 10
	// MaxModelFailures is the maximum consecutive failures before disabling a model.
	MaxModelFailures = 5
)

// ModelConfigProvider provides access to model configuration by name.
type ModelConfigProvider interface {
	GetLLMModel(name string) (*LLMModelConfig, error)
}

// ModelNamesProvider provides a default ordered model list.
type ModelNamesProvider interface {
	GetLLMModelNames() []string
}

// LLMModel represents a single LLM model with concurrency control.
type LLMModel struct {
	Config         *LLMModelConfig
	ChatModel      model.ToolCallingChatModel
	key            string
	concurrencySem chan struct{}
	contiFails     int
	disabled       bool
	mu             sync.Mutex
	successCount   int64
	failureCount   int64
	totalWaitTime  int64
	totalDuration  int64
}

// ModelStats tracks success and failure counts for a specific model.
type ModelStats struct {
	ModelName     string  `json:"model_name"`
	SuccessCount  int64   `json:"success_count"`
	FailureCount  int64   `json:"failure_count"`
	WaitTimeRatio float64 `json:"wait_time_ratio"`
}

// Global model cache with mutex for thread-safe access.
var (
	modelCache   = make(map[string]*LLMModel)
	modelCacheMu sync.RWMutex
)

const (
	KeyTimeoutRate = "timeoutRate"
)

// GetLLMModel retrieves or creates an LLM model by name with caching.
func GetLLMModel(cfg ModelConfigProvider, name string) (*LLMModel, error) {
	modelCacheMu.RLock()
	if cached, ok := modelCache[name]; ok {
		modelCacheMu.RUnlock()
		cached.mu.Lock()
		disabled := cached.disabled
		cached.mu.Unlock()
		if disabled {
			return nil, fmt.Errorf("model '%s' is disabled due to consecutive failures", name)
		}
		return cached, nil
	}
	modelCacheMu.RUnlock()

	modelCacheMu.Lock()
	defer modelCacheMu.Unlock()

	if cached, ok := modelCache[name]; ok {
		return cached, nil
	}

	modelCfg, err := cfg.GetLLMModel(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get model config for '%s': %w", name, err)
	}
	if modelCfg.Concurrency == 0 {
		modelCfg.Concurrency = DefaultConcurrencyLimit
	}

	chatModel, err := createChatModelFromConfig(modelCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create chat model for '%s': %w", name, err)
	}

	llmModel := &LLMModel{
		Config:         modelCfg,
		ChatModel:      chatModel,
		key:            name,
		concurrencySem: make(chan struct{}, modelCfg.Concurrency),
	}
	modelCache[name] = llmModel

	log.Debug("Created and cached LLM model",
		zap.String("model_name", name),
		zap.String("model", modelCfg.Name),
		zap.Int("concurrency", modelCfg.Concurrency))

	return llmModel, nil
}

func createChatModelFromConfig(cfg *LLMModelConfig) (model.ToolCallingChatModel, error) {
	if cfg.APIType == "glm-plan" {
		glm := NewGLMChatModel(cfg.APIKey, cfg.BaseURL, cfg.Name)
		if len(cfg.Payload) > 0 {
			glm.payload = cfg.Payload
		}
		return glm, nil
	}

	chatConfig := &openaicomp.ChatModelConfig{
		Model:       cfg.Name,
		APIKey:      cfg.APIKey,
		BaseURL:     cfg.BaseURL,
		ExtraFields: cfg.Payload,
	}
	if cfg.Temperature != nil {
		temp := float32(*cfg.Temperature)
		chatConfig.Temperature = &temp
	}

	chatModel, err := openaicomp.NewChatModel(context.Background(), chatConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenAI chat model: %w", err)
	}
	return chatModel, nil
}

// Generate calls the LLM model with concurrency control and timeout.
func (m *LLMModel) Generate(ctx context.Context, messages []*schema.Message) (*schema.Message, error) {
	waitStart := time.Now()

	select {
	case m.concurrencySem <- struct{}{}:
		defer func() { <-m.concurrencySem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	waitDuration := time.Since(waitStart).Milliseconds()
	m.mu.Lock()
	m.totalWaitTime += waitDuration
	m.mu.Unlock()

	timeoutRate2, _ := ctx.Value(KeyTimeoutRate).(float64)
	if m.Config.Timeout > 0 {
		timeoutRate := m.Config.TimeoutRate
		if timeoutRate == 0 {
			timeoutRate = 1.0
		}
		timeout := float64(m.Config.Timeout) * timeoutRate
		if timeoutRate2 > 0 {
			timeout *= timeoutRate2
		}
		actualTimeout := time.Duration(timeout) * time.Second

		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, actualTimeout)
		defer cancel()
	}

	callStart := time.Now()
	resp, err := m.ChatModel.Generate(ctx, messages)
	callDuration := time.Since(callStart).Milliseconds()
	m.mu.Lock()
	m.totalDuration += callDuration
	m.mu.Unlock()

	return resp, err
}

// RecordStatus records a success/failure for a model and disables it if threshold is reached.
func (m *LLMModel) RecordStatus(success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if success {
		m.contiFails = 0
		m.successCount++
		return
	}
	m.contiFails++
	m.failureCount++
	if m.contiFails >= MaxModelFailures {
		m.disabled = true
		log.Warn("Model disabled due to consecutive failures",
			zap.String("model", m.Config.Name),
			zap.Int("failure_count", m.contiFails))
	}
}

// IsDisabled checks if a model is disabled.
func (m *LLMModel) IsDisabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.disabled
}

// GetStats returns the statistics for this model.
func (m *LLMModel) GetStats() (successCount, failureCount, totalWaitTime, totalDuration int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.successCount, m.failureCount, m.totalWaitTime, m.totalDuration
}

// ResetStats resets the statistics counters for this model.
func (m *LLMModel) ResetStats() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successCount = 0
	m.failureCount = 0
	m.totalWaitTime = 0
	m.totalDuration = 0
}

// ModelSelectionStrategy defines how models are selected.
type ModelSelectionStrategy string

const (
	StrategySequential ModelSelectionStrategy = "sequential"
	StrategyRandom     ModelSelectionStrategy = "random"
)

// LLMCallMeta captures per-attempt context for hooks.
type LLMCallMeta struct {
	ModelName    string
	ModelConfig  *LLMModelConfig
	SystemPrompt string
	UserPrompt   string
	Messages     []*schema.Message
	Attempt      int
	Start        time.Time
	Duration     time.Duration
}

// LLMCallOptions contains options for LLM calls.
type LLMCallOptions struct {
	UserID    int
	JobID     int
	Node      string
	OnAttempt func(meta *LLMCallMeta)
	OnSuccess func(meta *LLMCallMeta, response *schema.Message)
	OnFailure func(meta *LLMCallMeta, err error)
}

// LLMManager manages multiple LLM models with dynamic model ordering.
type LLMManager struct {
	models     []*LLMModel
	modelIndex map[string]*LLMModel
	getModels  func() []string
	cfg        ModelConfigProvider
	mu         sync.RWMutex
}

// NewLLMManager creates a new LLM manager with a model provider.
func NewLLMManager(cfg ModelConfigProvider, getModels func() []string) (*LLMManager, error) {
	if getModels == nil {
		nameProvider, ok := cfg.(ModelNamesProvider)
		if !ok {
			return nil, fmt.Errorf("getModels is nil")
		}
		getModels = nameProvider.GetLLMModelNames
	}

	manager := &LLMManager{
		models:     nil,
		modelIndex: make(map[string]*LLMModel),
		getModels:  getModels,
		cfg:        cfg,
	}

	return manager, nil
}

// Call invokes LLM with automatic failover based on getModels order.
func (m *LLMManager) Call(ctx context.Context, systemPrompt, userPrompt string, opts *LLMCallOptions) (string, error) {
	modelNames := m.getModels()
	if len(modelNames) == 0 {
		return "", fmt.Errorf("no models available")
	}

	var messages []*schema.Message
	if systemPrompt != "" {
		messages = append(messages, &schema.Message{Role: schema.System, Content: systemPrompt})
	}
	messages = append(messages, &schema.Message{Role: schema.User, Content: userPrompt})

	var lastErr error
	var resolved bool
	var attempted bool
	for idx, modelName := range modelNames {
		if strings.TrimSpace(modelName) == "" {
			continue
		}
		llmModel, err := m.getOrInitModel(modelName)
		if err != nil {
			log.Warn("Failed to resolve model",
				zap.String("model", modelName),
				zap.Error(err))
			lastErr = err
			continue
		}
		resolved = true
		if llmModel.IsDisabled() {
			log.Debug("Skipping disabled model", zap.String("model", modelName))
			continue
		}
		attempted = true

		meta := &LLMCallMeta{
			ModelName:    modelName,
			ModelConfig:  llmModel.Config,
			SystemPrompt: systemPrompt,
			UserPrompt:   userPrompt,
			Messages:     messages,
			Attempt:      idx + 1,
			Start:        time.Now(),
		}
		if opts != nil && opts.OnAttempt != nil {
			opts.OnAttempt(meta)
		}

		resp, err := llmModel.Generate(ctx, messages)
		meta.Duration = time.Since(meta.Start)
		if err != nil {
			log.Warn("LLM call failed, trying next model",
				zap.String("model", modelName),
				zap.Error(err))
			llmModel.RecordStatus(false)
			if opts != nil && opts.OnFailure != nil {
				opts.OnFailure(meta, err)
			}
			lastErr = err
			continue
		}

		llmModel.RecordStatus(true)
		content := m.extractContent(resp.Content)
		if opts != nil && opts.OnSuccess != nil {
			opts.OnSuccess(meta, resp)
		}

		return content, nil
	}

	if lastErr != nil {
		return "", fmt.Errorf("all models failed: %v", lastErr)
	}
	if !resolved {
		return "", fmt.Errorf("no models available")
	}
	if !attempted {
		return "", fmt.Errorf("all models are disabled")
	}
	return "", fmt.Errorf("all models failed")
}

func (m *LLMManager) getOrInitModel(modelName string) (*LLMModel, error) {
	m.mu.RLock()
	if cached, ok := m.modelIndex[modelName]; ok {
		m.mu.RUnlock()
		return cached, nil
	}
	m.mu.RUnlock()

	llmModel, err := GetLLMModel(m.cfg, modelName)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if cached, ok := m.modelIndex[modelName]; ok {
		m.mu.Unlock()
		return cached, nil
	}
	m.modelIndex[modelName] = llmModel
	m.models = append(m.models, llmModel)
	m.mu.Unlock()

	return llmModel, nil
}

func (m *LLMManager) extractContent(content string) string {
	tagLen := 8
	thinkEnd := strings.LastIndex(content, "</think>")
	if thinkEnd < 0 {
		thinkEnd = strings.LastIndex(content, "</prepare>")
		tagLen = 10
	}
	if thinkEnd > 0 {
		content = content[thinkEnd+tagLen:]
	}
	return content
}

// GetModels returns the list of models managed by this manager.
func (m *LLMManager) GetModels() []*LLMModel {
	m.mu.RLock()
	models := make([]*LLMModel, len(m.models))
	copy(models, m.models)
	m.mu.RUnlock()
	return models
}

// GetAllModelStats returns statistics for all models managed by this manager.
func (m *LLMManager) GetAllModelStats() []ModelStats {
	models := m.GetModels()
	stats := make([]ModelStats, 0, len(models))
	for _, model := range models {
		successCount, failureCount, totalWaitTime, totalDuration := model.GetStats()
		var waitTimeRatio float64
		totalTime := totalWaitTime + totalDuration
		if totalTime > 0 {
			waitTimeRatio = float64(totalWaitTime) / float64(totalTime)
		}
		stats = append(stats, ModelStats{
			ModelName:     model.key,
			SuccessCount:  successCount,
			FailureCount:  failureCount,
			WaitTimeRatio: waitTimeRatio,
		})
	}
	return stats
}

// ResetAllModelStats resets statistics for all models managed by this manager.
func (m *LLMManager) ResetAllModelStats() {
	models := m.GetModels()
	for _, model := range models {
		model.ResetStats()
	}
}
