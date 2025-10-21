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
回测时添加`-cpu-profile`参数，启用性能测试，输出`cpu.profile`到回测目录下。然后执行下面命令可以查看结果
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
```shell
git tag v1.0.0
git push origin --tags
```
### 如何引用本地go模块？
1. 被引用模块执行`go mod init`添加`go.mod`文件，修改`module`后的模块名
2. 执行上面`git tag`添加新版本
3. 在当前项目（和依赖当前项目的入口项目）添加依赖：`go get 模块名@version`
4. 在当前项目（和依赖当前项目的入口项目）的`go.mod`中添加`replace`指令，改为本地绝对或相对路径

### 如何修改测试web UI？
您只需将`web/ui/svelte.config.js`中的`adapter-static`改为`adapter-auto`，然后执行`npm run dev`，浏览器访问`http://localhost:5173`，即可实时预览ui变动。
后端接口默认访问`http://localhost:8000`。

### json
It is not recommended to replace `encoding/json` with [sonic](https://github.com/bytedance/sonic/issues/574). The binary file will increase by 15M (on Windows)
