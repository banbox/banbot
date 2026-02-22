您可能希望在交易过程中使用机器学习/深度学习等AI工具，banbot对此提供了grpc支持。

结合ai的常见流程如下：
1. golang端定义好特征，启动banbot通过`data_server`提供grpc服务
2. python端请求得到特征数据，保存为训练/测试数据。
3. python端训练ML/DL等模型。
4. python端部署模型提供grpc预测服务。
5. golang端实现交易策略，准备特征请求模型得到预测结果，用于交易决策。

banbot已为上面流程提供了完整的golang端[代码示例](https://github.com/banbox/banstrats/tree/main/rpc_ai)。

## LLM大模型接口
除传统ML/DL工作流外，banbot也内置LLM集成能力，允许在策略或工具中调用大模型接口完成文本分析、报告生成等辅助工作。

LLM模块提供了统一的模型配置与管理能力（多模型故障转移、并发控制、统计追踪、自动禁用等），便于在实盘环境中稳定使用。
