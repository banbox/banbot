# 策略定义与注册

当前时序策略使用 `TradeStrat`、`StratJob` 和实例 `State`，因子/截面使用 `factor/runner` 的 Go definition 或启动编译的 expressions。Go 策略需要编入当前可执行程序；表达式不要求同名 Go definition。两引擎原始数据保留 `DataSeries.Values` 的类型/NULL，自定义字段不收窄为 OHLCV。

见[自定义策略](../bandoc/zh-CN/guide/strat_custom.md)、[因子指南](../bandoc/zh-CN/guide/factor.md)及[审查记录](../doc/strategy_engine_refactor.md)。以下保留历史动态加载方案调研，不表示当前策略装配已经使用其中某个 loader，也不能据历史比例做性能承诺。

## 历史调研：策略热加载的几种方案
由于策略经常需要增删或更新，需要和机器人分开分发。go不支持二进制分发包，目前调研到下面几种动态加载方案。  
由于本项目采用go最主要目的是改善性能，而策略和系统涉及到很多次交互，是最大的性能瓶颈，故使用goloader方案。  
### goloader
重用了golang编译器的输出，不需要自己写编译器，就能支持所有golang的语言特征。  
只进行符号重定位，和提供runtime信息，可以卸载加载的代码，复用了runtime，支持windows，linux，macos
和原生go性能相同，是所有方案中性能最好的。
### go原生plugin  
可参考使用[tidb文档](https://github.com/pingcap/tidb/blob/master/docs/design/2018-12-10-plugin-framework.md)  
[官方文档](https://pkg.go.dev/plugin)  
[一文搞懂Go语言的plugin](https://tonybai.com/2021/07/19/understand-go-plugin/)  
[Go Plugin，一个凉了快半截的特性？](https://www.easemob.com/news/6987)  

【优点】  
* 性能接近原生go，非常快，大概有原生70%的性能  

【缺点】  
* 只支持linux，mac，不支持windows
* 要求插件和主程序打包版本完全一致
* 要求所有依赖包版本完全一致
* 有一定的入侵风险
### WebAssembly
将插件部分打包为wasi，然后主程序嵌入一个wasm运行时，由运行时加载插件并运行。  
go没有原生支持wasm，所以目前有两种方法：  
1. 使用Cgo链接一个C或Rust的运行时
2. 使用wazero作为运行时  

[wasmer 16.7 star](https://github.com/wasmerio/wasmer)  
使用CGo+LLVM编译器，性能接近原生；  
[wasm-micro-runtime 4.2 star](https://github.com/bytecodealliance/wasm-micro-runtime)  
使用CGo嵌入到Go程序中  
[wasmtime 13.5 start](https://github.com/bytecodealliance/wasmtime)
使用CGo嵌入到Go程序中，性能比wasmer-LLVM慢一些  
[wasm3 6.5 star](https://github.com/wasm3/wasm3)  
Go绑定不支持Windows，
[WasmEdge 7.2 star](https://github.com/WasmEdge/WasmEdge)  
使用CGo嵌入到Go主程序  
[extism 2.8 star](https://github.com/extism/extism)  
使用wazero集成到go主程序  
[wazero 4.2 star](https://github.com/tetratelabs/wazero)  
纯go实现的，使用TinyGo，优于CGo嵌入  

### go-tengo脚本
[Github](https://github.com/d5/tengo.git)  
这是基于纯go写的动态解释性脚本语言，支持预编译。性能接近原生python，比原生go慢50倍。  
未测试与gp-plugin-grpc的性能对比，应该tengo稍快。

### go-plugin-grpc  
[Github](https://github.com/hashicorp/go-plugin)  
这是基于grpc的插件方案，支持所有主要语言。  

【优点】  
* 支持语言多
* 双向通信调用
* 进程隔离，无安全风险  

【缺点】
* 性能应该是最差的，尤其在频繁通信的时候。
