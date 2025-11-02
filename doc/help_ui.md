# BanBot UI 前端项目概述

## 项目整体概述
BanBot UI 是一个为量化交易机器人 BanBot 设计的前端项目。它提供了策略开发、回测、数据管理以及实盘机器人监控等功能。项目基于 SvelteKit 框架，使用 DaisyUI 和 TailwindCSS 进行样式设计，并采用 Vite 作为构建工具。

## 技术架构与实现方案
- **框架**: SvelteKit - 现代全栈Web框架
- **样式**: TailwindCSS + DaisyUI - 原子化CSS框架和组件库
- **构建工具**: Vite - 快速构建工具
- **图表**: KlineCharts - 专业的K线图表库
- **代码编辑**: CodeMirror - 集成代码编辑器
- **国际化**: Inlang + Paraglide - 多语言支持(中英文)
- **状态管理**: Svelte stores + svelte-persisted-store - 响应式状态管理及持久化
- **数据请求**: ofetch - 轻量级的HTTP请求库

## 核心文件索引

### 应用程序入口
- **src/app.html**: 主HTML模板文件，定义网页基本结构。
- **src/routes/+layout.svelte**: 全局布局，处理全局消息提醒和模态框。
- **src/routes/(dev)/+layout.svelte**: 开发环境布局，包含左侧主菜单和顶部导航。
- **src/routes/dash/+layout.svelte**: 实盘监控环境布局，包含机器人账户管理。
- **src/style.css**: 全局样式文件，引入TailwindCSS和DaisyUI。

### 核心服务层
- **src/lib/netio.ts**: API通信核心,封装了 `getApi`、`postApi`、`getAccApi`、`postAccApi` 等函数,支持开发模式和实盘模式API调用,基于ofetch。
- **src/hooks.server.ts**: 服务端钩子,处理国际化路由。
- **src/hooks.ts**: 客户端钩子,处理国际化路由。

### 状态管理
- **src/lib/stores/site.ts**: 全站状态管理,包括API主机、加载状态、构建状态、iframe检测等。
- **src/lib/stores/alerts.ts**: 全局警告消息管理。
- **src/lib/stores/modals.ts**: 全局模态框（确认框、提示框）管理。
- **src/lib/dash/store.ts**: 实盘监控仪表盘状态,管理机器人账户列表、当前活动账户、持久化存储。

### 通用组件库
- **src/lib/Icon.svelte**: 全局图标组件。
- **src/lib/Alert.svelte**: 全局警告提示组件。
- **src/lib/Snippets.svelte**: 通用代码片段组件.
- **src/lib/SuggestTags.svelte**: 带建议的标签输入组件。
- **src/lib/treeview/**: 文件树视图组件。

### 工具函数库
- **src/lib/common.ts**: 通用数据和函数。
- **src/lib/dateutil.ts**: 日期和时间处理函数,基于dayjs。
- **src/lib/index.ts**: 库导出入口。

### 国际化
- **src/lib/paraglide/**: Paraglide国际化生成文件。
- **messages/**: 多语言消息文件 (zh-CN.json, en-US.json)。

### 配置文件
- **package.json**: 项目依赖和脚本配置。
- **svelte.config.js**: SvelteKit配置，使用 `adapter-static` 进行静态站点生成。
- **vite.config.ts**: Vite构建配置，集成TailwindCSS和Paraglide。
- **tailwind.config.cjs**: TailwindCSS和DaisyUI配置。
- **tsconfig.json**: TypeScript配置。

## 功能模块组件

#### K线图表系统 (KLine)
- **src/lib/kline/Chart.svelte**: K线图主组件，封装了KlineCharts库。
- **src/lib/kline/MenuBar.svelte**: 图表菜单栏，提供周期、指标、设置等功能。
- **src/lib/kline/DrawBar.svelte**: 图表绘制工具栏。
- **src/lib/kline/mydatafeed.ts**: K线图数据源适配器，负责从后端获取数据。
- **src/lib/kline/chart.ts**: 图表配置和工具函数。
- **src/lib/kline/coms.ts**: K线图公共组件和工具。
- **src/lib/kline/indicators/**: 技术指标相关定义。
- **src/lib/kline/overlays/**: 图表覆盖层（绘图工具）定义。
- **src/lib/kline/figures/**: 自定义图形组件。
- **src/lib/kline/Modal*.svelte**: K线图相关的各种设置模态框。

#### 开发与回测 (Dev)
- **src/lib/dev/CodeMirror.svelte**: 用于策略和配置编辑的代码编辑器,支持Go和YAML语法高亮。
- **src/lib/dev/Monaco.svelte**: Monaco编辑器组件,提供更强大的代码编辑功能。
- **src/lib/dev/AllConfig.svelte**: 展示所有配置项的抽屉组件。
- **src/lib/dev/DrawerDataTools.svelte**: 数据工具抽屉，提供数据下载、导入、导出等功能。
- **src/lib/dev/RangeSlider.svelte**: 范围选择滑块组件。
- **src/lib/dev/websocket.ts**: 用于接收后端实时消息的WebSocket客户端。
- **src/lib/dev/ban_hints.ts**: BanBot代码提示和自动完成配置。
- **src/lib/treeview/**: 用于展示策略文件目录的树形组件。

#### 实盘监控 (Dashboard)
- **src/lib/dash/AddBot.svelte**: 添加（登录）新的机器人实例的组件。
- **src/lib/dash/store.ts**: Dashboard的核心状态管理，包括机器人账户列表和当前活动账户。
- **src/lib/order/**: 用于展示订单和持仓详情的组件。

## 路由页面

#### 开发环境路由 `(dev)`
- **src/routes/(dev)/+page.svelte**: 开发模式主页，包含快捷入口和文档链接，自动检测实盘模式并跳转。
- **src/routes/(dev)/strategy/+page.svelte**: 策略开发页面，包含文件树和代码编辑器。
- **src/routes/(dev)/backtest/new/+page.svelte**: 新建回测任务页面。
- **src/routes/(dev)/backtest/+page.svelte**: 回测历史列表页面。
- **src/routes/(dev)/backtest/item/+page.svelte**: 单个回测报告详情页。
- **src/routes/(dev)/optimize/+page.svelte**: 超参数优化页面。
- **src/routes/(dev)/data/+page.svelte**: 本地数据管理页面。
- **src/routes/(dev)/data/item/+page.svelte**: 单个品种的数据详情页。
- **src/routes/(dev)/setting/+page.svelte**: 全局配置、编译、语言等设置页面。
- **src/routes/(dev)/trade/+page.svelte**: 实盘交易指引页面。

#### 实盘监控路由 `dash`
- **src/routes/dash/+page@.svelte**: 机器人列表和登录入口。
- **src/routes/dash/board/+page.svelte**: 机器人状态总览仪表盘。
- **src/routes/dash/strat_job/+page.svelte**: 策略任务管理页面。
- **src/routes/dash/kline/+page.svelte**: 实盘K线图页面。
- **src/routes/dash/perf/+page.svelte**: 账户收益表现统计页面。
- **src/routes/dash/order/+page.svelte**: 订单和持仓管理页面。
- **src/routes/dash/rebate/+page.svelte**: 账户资金流水页面。
- **src/routes/dash/setting/+page.svelte**: 查看机器人当前运行配置。
- **src/routes/dash/tool/+page.svelte**: 交易所数据工具页面。
- **src/routes/dash/log/+page.svelte**: 机器人实时日志页面。

#### 独立页面
- **src/routes/kline/+page.svelte**: 一个独立的、可用于外部嵌入的K线图页面。

## 前端UI风格特征总结

### 1. 框架技术与基础风格
- **技术栈**: 采用 SvelteKit + TypeScript，组件化开发，利用 Svelte 5 的符文（Runes）进行状态管理。
- **UI风格**: 基于 DaisyUI 和 TailwindCSS，整体风格简洁、功能化。大量使用 `card`, `btn`, `input`, `select`, `table` 等 DaisyUI 组件，保持了高度的视觉一致性。
- **布局**: 多采用卡片式（Card）布局来组织信息模块，页面结构清晰。使用 Flexbox 和 Grid 进行灵活布局。

### 2. 结构与交互
- **导航**: 主要功能通过固定的侧边栏菜单进行导航，分为`(dev)`和`dash`两大模块，逻辑清晰。
- **交互组件**: 广泛使用模态框（Modal）和抽屉（Drawer）进行复杂交互，如设置、添加机器人、查看详情等，避免页面跳转。
- **状态提示**: 通过全局的 `Alert` 系统提供实时的操作反馈（成功、失败、警告）。加载状态通过全局 `loading` 指示器展示。

### 3. 数据与展示
- **数据可视化**: 核心是 `KlineCharts` 图表，提供了丰富的绘图工具和技术指标支持。统计数据多以表格（Table）和统计卡片（Stats）的形式展示。
- **代码展示**: 使用 `CodeMirror` 组件展示和编辑策略代码及配置文件，支持语法高亮。
- **国际化**: 所有面向用户的文本都通过 `inlang/paraglide` 进行国际化处理，支持中英文切换。

### 4. 代码风格与规范
- **组件化**: 功能高度组件化，可复用性强，如 `Icon`, `Modal`, `FormField` 等。
- **类型安全**: 全面使用TypeScript，接口和类型定义清晰（如 `types.d.ts`）。
- **代码分离**: 业务逻辑、UI组件和状态管理分离。API请求集中在 `netio.ts`，状态存储在 `stores` 目录，UI组件在 `lib` 和 `routes` 中。

### 整体风格简要描述：
以“功能优先、简洁高效”为核心，UI风格统一、一致性强。交互逻辑清晰，通过模态框和抽屉简化操作流程。代码结构模块化，易于维护和扩展。整体呈现出一种为专业交易者设计的、注重实用性和效率的工具型应用风格。
