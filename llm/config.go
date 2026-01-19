package llm

import (
	"fmt"
	"os"
	"regexp"
)

// LLMModelConfig represents a single LLM model configuration.
type LLMModelConfig struct {
	Name            string                 `yaml:"name,omitempty" mapstructure:"name"`
	Extend          interface{}            `yaml:"extend,omitempty" mapstructure:"extend"` // string or []string
	APIType         string                 `yaml:"api_type,omitempty" mapstructure:"api_type"`
	APIKey          string                 `yaml:"api_key,omitempty" mapstructure:"api_key"`
	BaseURL         string                 `yaml:"base_url,omitempty" mapstructure:"base_url"`
	Timeout         int                    `yaml:"timeout,omitempty" mapstructure:"timeout"`
	TimeoutRate     float64                `yaml:"timeout_rate,omitempty" mapstructure:"timeout_rate"`
	Temperature     *float64               `yaml:"temperature,omitempty" mapstructure:"temperature"`
	Concurrency     int                    `yaml:"concurrency,omitempty" mapstructure:"concurrency"`
	PromptPrice     float64                `yaml:"prompt_price,omitempty" mapstructure:"prompt_price"`
	CompletionPrice float64                `yaml:"completion_price,omitempty" mapstructure:"completion_price"`
	CachePrice      float64                `yaml:"cache_price,omitempty" mapstructure:"cache_price"`
	Payload         map[string]interface{} `yaml:"payload,omitempty" mapstructure:"payload"`
}

// ResolveModels resolves the 'extend' field in LLM model configurations.
func ResolveModels(models map[string]*LLMModelConfig) error {
	if models == nil {
		return nil
	}
	resolved := make(map[string]bool)
	for name := range models {
		if err := resolveModel(models, name, resolved, []string{}); err != nil {
			return err
		}
	}
	return nil
}

// GetModelConfig returns LLM model configuration by name.
func GetModelConfig(models map[string]*LLMModelConfig, name string) (*LLMModelConfig, error) {
	model, ok := models[name]
	if !ok {
		return nil, fmt.Errorf("LLM model '%s' not found", name)
	}
	return model, nil
}

// CalculateModelCost calculates cost for a model based on token usage.
// Prices are per 1M tokens in config, so we divide by 1,000,000.
func CalculateModelCost(models map[string]*LLMModelConfig, modelName string, inputTokens, outputTokens int) (float64, error) {
	model, err := GetModelConfig(models, modelName)
	if err != nil {
		return 0, err
	}
	inputCost := float64(inputTokens) / 1000000.0 * model.PromptPrice
	outputCost := float64(outputTokens) / 1000000.0 * model.CompletionPrice
	return inputCost + outputCost, nil
}

// GetModelPricing returns pricing information for a model.
func GetModelPricing(models map[string]*LLMModelConfig, modelName string) (promptPrice, completionPrice, cachePrice float64, err error) {
	model, err := GetModelConfig(models, modelName)
	if err != nil {
		return 0, 0, 0, err
	}
	return model.PromptPrice, model.CompletionPrice, model.CachePrice, nil
}

var envVarPattern = regexp.MustCompile(`\$\{([^}:]+)(?::([^}]*))?\}`)

// SubstituteEnvVars replaces ${VAR_NAME} or ${VAR_NAME:default} with environment variable values.
func SubstituteEnvVars(content string) string {
	return envVarPattern.ReplaceAllStringFunc(content, func(match string) string {
		parts := envVarPattern.FindStringSubmatch(match)
		varName := parts[1]
		defaultValue := ""
		if len(parts) > 2 {
			defaultValue = parts[2]
		}
		if value := os.Getenv(varName); value != "" {
			return value
		}
		return defaultValue
	})
}

// SubstituteEnvValue applies SubstituteEnvVars recursively to a value tree.
func SubstituteEnvValue(val interface{}) interface{} {
	switch v := val.(type) {
	case string:
		return SubstituteEnvVars(v)
	case map[string]interface{}:
		for k, vv := range v {
			v[k] = SubstituteEnvValue(vv)
		}
		return v
	case map[interface{}]interface{}:
		for k, vv := range v {
			v[k] = SubstituteEnvValue(vv)
		}
		return v
	case []interface{}:
		for i, vv := range v {
			v[i] = SubstituteEnvValue(vv)
		}
		return v
	case []string:
		for i, vv := range v {
			v[i] = SubstituteEnvVars(vv)
		}
		return v
	default:
		return val
	}
}

func resolveModel(models map[string]*LLMModelConfig, name string, resolved map[string]bool, chain []string) error {
	if resolved[name] {
		return nil
	}
	for _, c := range chain {
		if c == name {
			return fmt.Errorf("circular dependency detected in model inheritance: %v", append(chain, name))
		}
	}
	model := models[name]
	if model == nil {
		return fmt.Errorf("model '%s' not found", name)
	}
	if model.Extend == nil {
		resolved[name] = true
		return nil
	}

	var parentNames []string
	switch v := model.Extend.(type) {
	case string:
		parentNames = []string{v}
	case []interface{}:
		for _, p := range v {
			if s, ok := p.(string); ok {
				parentNames = append(parentNames, s)
			}
		}
	case []string:
		parentNames = v
	default:
		return fmt.Errorf("invalid extend type for model '%s'", name)
	}

	newChain := append(chain, name)
	for _, parentName := range parentNames {
		if err := resolveModel(models, parentName, resolved, newChain); err != nil {
			return err
		}
	}

	merged := &LLMModelConfig{}
	for _, parentName := range parentNames {
		parent := models[parentName]
		if parent == nil {
			return fmt.Errorf("parent model '%s' not found for model '%s'", parentName, name)
		}
		mergeModelConfig(merged, parent)
	}
	mergeModelConfig(merged, model)

	models[name] = merged
	resolved[name] = true
	return nil
}

// mergeModelConfig merges source into target (non-zero values from source override target).
func mergeModelConfig(target, source *LLMModelConfig) {
	if source.Name != "" {
		target.Name = source.Name
	}
	if source.APIType != "" {
		target.APIType = source.APIType
	}
	if source.APIKey != "" {
		target.APIKey = source.APIKey
	}
	if source.BaseURL != "" {
		target.BaseURL = source.BaseURL
	}
	if source.Timeout != 0 {
		target.Timeout = source.Timeout
	}
	if source.TimeoutRate != 0 {
		target.TimeoutRate = source.TimeoutRate
	}
	if source.Temperature != nil {
		target.Temperature = source.Temperature
	}
	if source.Concurrency != 0 {
		target.Concurrency = source.Concurrency
	}
	if source.PromptPrice != 0 {
		target.PromptPrice = source.PromptPrice
	}
	if source.CompletionPrice != 0 {
		target.CompletionPrice = source.CompletionPrice
	}
	if source.CachePrice != 0 {
		target.CachePrice = source.CachePrice
	}
	if source.Payload != nil {
		if target.Payload == nil {
			target.Payload = make(map[string]interface{})
		}
		for k, v := range source.Payload {
			target.Payload[k] = v
		}
	}
}
