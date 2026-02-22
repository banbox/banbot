You may want to use AI tools such as machine learning/deep learning during trading, and banbot provides grpc support for this.

The common process of combining AI is as follows:
1. Define the features on the golang side and start banbot to provide grpc services through `data_server`
2. Request feature data on the python side and save it as training/test data.
3. Train ML/DL and other models on the python side.
4. Deploy the model on the python side to provide grpc prediction services.
5. Implement the trading strategy on the golang side, prepare the feature request model to obtain the prediction results for trading decisions.

banbot has provided a complete golang side [code example](https://github.com/banbox/banstrats/tree/main/rpc_ai) for the above process.

## LLM API Integration
In addition to traditional ML/DL workflows, banbot also has built-in LLM integration capabilities, allowing you to call large language model APIs in strategies or tools for tasks such as text analysis, report generation, and other auxiliary work.

The LLM module provides unified model configuration and management capabilities (multi-model failover, concurrency control, statistics tracking, automatic disabling, etc.), making it easy to use stably in live trading environments.
