# 各个go包的依赖关系及公开方法
[文档](https://www.banbot.site/en-US/api/)

# 代码规范
* 可使用AI辅助开发并贡献代码，但提pr前必须逐行审查代码，删除不必要、未使用、不规范的文档或代码。
* 不接受AI生成的大段文档，此代码库中文档应保持精炼；可将示例配置维护到doc/config.yml，将其他文档维护到bandoc（也可发邮件到anyongjin163@163.com）
* 注意多语言支持，短文本应使用`GetLangMsg`，较长内容应使用`ReadLangFile`；
* 要合并的提交应保持干净，可使用`git cherry-pick`和`git reset --soft`从最新主分支代码挑选必要的提交，不要包含"Merge xxx"
* 对于锁的复杂引用应使用deadlock，简单的确定的使用不应使用deadlock，（高频调用deadlock会内存泄漏）

# 常见问题
### 如何进行函数性能测试？（不含IO）
回测时添加`-cpu-profile`参数，启用性能测试，输出`cpu.profile`到进程当前工作目录。命令退出时停止采样并关闭文件。然后执行下面命令可以查看结果
```shell
go tool pprof -http :6060 cpu.profile
```
### 如何进行函数性能测试？（包含IO）
回测时添加`-cpu-profile`参数，启用性能测试，然后执行下面命令可以查看结果
```shell
go tool pprof --http=:6061 http://localhost:6060/debug/fgprof?seconds=10
```
### 如何分析内存泄露？(初级)
命令行启动添加`-mem-profile`参数，将会在6060端口提供pprof分析接口。执行下面命令查看当前内存占用最多：
```shell
go tool pprof http://localhost:6060/debug/pprof/heap
> top
```
然后执行`list [method]`即可显示具体某个方法的内存行热点。

上面`go tool pprof`命令同时会生成一个heap文件，等待一段时间再次执行得到新heap文件。

然后执行`go tool pprof -base old_path new_path`可分析两个heap文件差异，使用`top`和`list`继续分析即可。

### 复杂项目分析内存泄露？
上面的方法只能显示申请内存最多的位置，但复杂项目中泄露对象可能被非常多位置跟踪，任意一处保留引用即导致内存泄露，可使用[goref](https://github.com/cloudwego/goref)排查未释放的引用。

先启动进程，查询进程ID，然后执行：
```shell
grf attach [PID]
go tool pprof -http=:5079 ./grf.out
```

### 如何发布go模块新版本？
先更新 `core/data.go` 的 `Version`；若修改了 `web/ui`，还需编译、打包前端并更新 `UIVersion`。未修改前端时沿用已有 `UIVersion` 及对应 release 的 `dist.zip`。发布源码不得依赖本地目录 `replace`：先发布配套依赖，再固定远程版本，验证构建、测试和文档后提交，为该提交创建并推送准确的版本标签：
```shell
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin HEAD
git push origin v1.0.0
```
beta 标签在 GitHub 上创建 prerelease；发布说明应列出配置兼容性、验证范围及前端资源版本。本地存在 `go.work` 时，发布验证应设置 `GOWORK=off`，并用 `go list -m -json` 确认依赖来自远程模块缓存，而非相邻开发目录。
### 如何引用本地go模块？
1. 被引用模块执行`go mod init`添加`go.mod`文件，修改`module`后的模块名
2. 在当前项目的`go.mod`中保留相应`require`，添加`replace 模块名 => 本地路径`。本地联调无需发布或创建标签。
3. 发布后若要使用远端版本，再执行`go get 模块名@version`并移除本地`replace`。

### 如何修改测试web UI？
在`web/ui`目录安装依赖后执行`npm run dev`，浏览器访问`http://localhost:5173`，即可实时预览ui变动。开发服务器不需要更换`adapter-static`；该适配器用于生成打包进 Go 程序的静态页面。
后端接口默认访问`http://localhost:8000`。

### 如何编写新的运行时组件？
通过 `config.LoadRunSpec` 加载统一配置，由 `entry` 装配任务级 `runtime.Runtime`，向组件传递明确的依赖。任务状态、时钟、存储和退出回调归该 Runtime 所有，避免新组件读取兼容包级变量。任意时序字段统一通过 `orm.DataSeries.Values` 传递。详见 [运行时上下文](runtime_context.md)、[时序数据](series_usage.md) 和 [包级架构审查](strategy_engine_refactor.md)。

### json
It is not recommended to replace `encoding/json` with [sonic](https://github.com/bytedance/sonic/issues/574). The binary file will increase by 15M (on Windows)
