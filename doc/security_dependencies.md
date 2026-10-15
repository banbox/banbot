# v0.6.0-beta.5：稳定依赖升级与漏洞修复（2026-10-05）

排查基于 GitHub Dependabot 当日返回的全部 108 条未关闭告警，而不是仅依据推送提示。按告警中每个包的漏洞版本范围逐一比对修改后的 `go.mod` 和两份 npm 锁文件，108 条均已避开受影响版本。此结果表示本地依赖已修复；GitHub 页面需要这些变更进入默认分支并重新扫描后才会更新。

| 范围 | GitHub 告警数 | 本地修复后仍命中的原告警 |
| --- | ---: | ---: |
| Go 后端 | 38 | 0 |
| Web UI | 65 | 0 |
| 文档站 | 5 | 0 |
| 合计 | 108 | 0 |

原告警等级为 critical 12、high 39、moderate 49、low 8。同一个依赖可能对应多条漏洞，npm audit 的受影响包数量与 GitHub 的告警数量不能直接比较。

## 后端

| 依赖 | 原版本 | 修复版本 |
| --- | --- | --- |
| `github.com/jackc/pgx/v5` | 5.7.6 | 5.11.0 |
| `github.com/gofiber/fiber/v2` | 2.52.9 | 2.52.15 |
| `google.golang.org/grpc` | 1.76.0 | 1.83.2 |
| `github.com/xuri/excelize/v2` | 2.10.0 | 2.11.0 |
| `github.com/getkin/kin-openapi` | 0.118.0 | 依赖链已移除 |
| `github.com/buger/jsonparser` | 1.1.1 | 1.1.2 |
| `golang.org/x/crypto` | 0.43.0 | 0.57.0 |
| `golang.org/x/net` | 0.46.0 | 0.59.0 |
| `golang.org/x/image` | 0.32.0 | 0.46.0 |
| `golang.org/x/text` | 0.30.0 | 0.42.0 |
| `github.com/valyala/fasthttp` | 1.64.0 | 1.75.0 |
| `github.com/klauspost/compress` | 1.18.0 | 1.20.1 |

Go 官方漏洞库扫描补充发现了原 GitHub 清单尚未包含的问题，因此部分版本高于 GitHub 的最低修复版本，例如 gRPC 服务端崩溃、文本处理死循环和图像解码资源耗尽。必要的间接依赖随上游要求同步升级，没有新增业务依赖或修改交易、时序数据、QuestDB WAL 逻辑。

最低 Go 版本提高到 **1.26.0**，`toolchain` 推荐 **go1.26.8**。两个 Docker 构建基础镜像同步到 `golang:1.26.8`。`toolchain` 是默认工具链选择建议，构建环境仍应确保使用受支持且已修补的 Go 版本。

`govulncheck v1.8.0` 使用 Go 1.26.8 和 2026-10-01 的官方漏洞库，最终结果为 **0 条可达漏洞、0 条已导入包漏洞**。另有一条模块级提示 [GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932)：`golang.org/x/crypto/openpgp` 已停止维护，上游没有修复版本。本项目及扫描到的依赖没有导入该包，因此没有删除其他仍使用的 `x/crypto` 包，也没有屏蔽该提示。

## 稳定版本选择

在安全修复基础上，检查所有直接依赖的稳定发布版本，升级 Eino 0.9.21 / OpenAI 模型组件 0.1.13、pgx 5.11.0、SQLite 1.60.1、Fiber 2.52.15、validator 10.30.5、Telegram 1.27.0、mapstructure 2.5.0、tablewriter 1.1.5、zap 1.28.0 等，并更新实际依赖链的兼容补丁。已经是最新稳定版本的 banexg、banta、decimal 等保留。

gRPC 使用已修复的稳定版 **1.83.2**。尝试当日最新稳定版 1.84.0 后，官方漏洞扫描实测重新命中可达漏洞 [GO-2026-6443](https://pkg.go.dev/vuln/GO-2026-6443)；其修复也已回移到 1.83.2。因此保留安全补丁版本，不采用有漏洞的 1.84.0 或 1.85 开发版。

前端将 CodeMirror、Tailwind 4.3.3、DaisyUI 5.7.47、TypeScript 5.9.3、ESLint 9.39.5、Prettier 3.9.9、Svelte 插件和适配器等升级到现有主版本内的稳定版本。Kit 2、Vite 6 和现有插件主版本保持匹配，不执行未经适配的大版本迁移。Klinecharts 10.0.3 稳定版实测产生 36 条新增 API 类型错误，因此精确锁定已兼容的 `10.0.0-alpha5`；后续图表升级需要独立 API 迁移。

Eino 跨 0.x minor 的升级通过新增离线 HTTP 回归测试验证 OpenAI / GLM 的 endpoint、鉴权、消息内容、扩展 payload 和响应 token 元数据。新版模型组件不再依赖 kin-openapi，`go mod tidy` 已将其及不再需要的 OpenAPI 依赖从模块与校验文件中移除。pgx 5.11 新增 `Rows.TypeMap()`，三个测试替身补齐接口；现有 WAL 与表替换测试的行为保持一致。

## Web UI 与文档站

UI 保留直接依赖的现有主版本，主要升级 SvelteKit 2.70.3、Svelte 5.57.1、Vite 6.4.3、Paraglide 2.25.4 和 lodash 4.18.1。更新间接依赖 devalue、js-yaml、nanoid、kysely、uuid 等；仅对 SvelteKit 的 `cookie` 使用限定范围的 `^0.7.2` override，并验证了现有 API 兼容性及非法 cookie 名称拒绝行为。两处页面增加三处类型注解，适配新版 Paraglide 的 locale 和本地化字符串类型。

文档站保留 VitePress 稳定版 1.6.4，以 `vite: ^6.4.3` override 替换有漏洞的 Vite 5 依赖链，并更新 esbuild、PostCSS 和 nanoid。其 Vue 插件声明支持 Vite 6；已验证生产构建和开发服务器路由。

| 本地审计 | 修改前受影响包 | 修改后 |
| --- | ---: | ---: |
| UI `npm audit` | 36 | 0 |
| 文档站 `npm audit` | 5 | 0 |

## 验证与边界

后端验证使用当前已跟踪源码的独立副本，禁用本机 `go.work`，避免邻目录 banexg/banta 和被忽略的 `tmp` 实验程序影响依赖解析。以下命令应在干净检出中运行；默认外部集成测试仍按照项目约定跳过。

```sh
GOWORK=off go mod verify
GOWORK=off go mod tidy -diff
GOWORK=off go test ./... -count=1 -timeout=10m
GOWORK=off go vet ./...
GOWORK=off go build ./...
GOWORK=off go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
```

上述后端验证均通过，另外通过了 Linux amd64 / CGO_DISABLED 构建，Windows 编译程序输出 `banbot v0.6.0-beta.5`。本机未跟踪的 `go.work` 也已同步最低 Go 版本和工具链建议；独立检出的全量测试不依赖这些本机设置。

UI 和文档站均执行安装、审计和生产构建。UI 额外在现有 GitHub CI 使用的 Node 22 分支（22.23.3）执行 `npm ci`、构建、11 个既有测试和审计，全部通过。UI 类型检查由原有 17 条错误减少为 16 条，没有新增错误；Svelte 对原有状态初值捕获增加了 13 条警告，总计 15 条。UI 全量 lint 被原有格式和 ESLint 问题阻塞。这些结果不能表述为类型检查或 lint 通过。

最终 UI 已打包为本地 `tmp/release-v0.6.0-beta.5/dist.zip`，包含 206 个文件；压缩包通过完整性检查，解压内容逐文件与 Node 22 构建结果一致。其 SHA-256 为 `7ce1edb6e1f24aef8971628858c8ca2b08a748c06ad220668e494a96d9fe616d`。构建包作为 GitHub 预发布附件提供，不作为源码提交。

`core.Version` 和 `core.UIVersion` 均更新为 **v0.6.0-beta.5**，通过 git tag 标识发布提交，UI 构建包使用 release 附件名 `dist.zip`，与自动下载路径一致。现有 Dockerfile 从远端 banstrats 构建，因此部署镜像还需要使用包含这些修复的 banbot 发布版本；仅发布依赖修复不会更新已部署的镜像。本机未运行 Docker 镜像构建或真实交易所/数据库集成验证。

## 证据来源

- [项目 Dependabot 告警](https://github.com/banbox/banbot/security/dependabot)
- [Go 官方漏洞数据库](https://vuln.go.dev/)与 [Go 工具链选择规则](https://go.dev/doc/toolchain)
- [gRPC 修复版本的依赖声明](https://proxy.golang.org/google.golang.org/grpc/@v/v1.83.2.mod)
- [x/crypto 修复版本的 Go 版本要求](https://proxy.golang.org/golang.org/x/crypto/@v/v0.56.0.mod)
- [VitePress 上游文档](https://deepwiki.com/vuejs/vitepress)及 npm 发布包的依赖、peerDependencies 声明

原始告警快照、逐条版本比对结果和验证日志保存在本机临时目录 `banbot-security` 和 `banbot-release-beta5`；日志不进入仓库。
