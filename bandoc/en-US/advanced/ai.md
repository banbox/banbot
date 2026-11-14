# AI and external models

## ML/DL through gRPC

Banbot exposes a gRPC data service:

```shell
bot tool data-server --config strategy.yml
```

Define features in the Go strategy project, fetch them from a Python or other client and train a model. Deploy a prediction service, then have the Go strategy prepare features, call the model and generate trading intents. Your application supplies features, training, model serving and trading logic. See the example strategy project's `rpc_ai` directory for a reference.

Keep field definitions, warmup, missing values and data availability rules consistent between historical training and online inference. See [Custom time-series data](../guide/custom_data.md).

## LLM management library

The `llm` package provides configuration inheritance, environment substitution, concurrency limits, timeouts, statistics and model fallback. Model settings live under `llm_models`; see [Configuration](../guide/configuration.md).

Ordinary backtest and live entry points do not automatically call LLMs. Adding configuration alone does not create predictions or trading signals. A strategy application or tool must call `llm.NewLLMManager(configProvider, getModels)` and then `manager.Call(ctx, systemPrompt, userPrompt, options)`. `*config.Config` implements model configuration access; the caller supplies model ordering.

The model cache is process-wide and does not fully isolate same-name model configurations across tasks. External requests have latency and may fail. The caller controls timeouts, retries and delivery of results to strategies; do not assume external model responses repeat identically in a backtest rerun.
