This is the documentation repository for banbot. This project uses [vitepress](https://vitepress.dev/) to generate the documentation site.

You can download this repository and open it with cursor, then use @codebase in chat to quickly ask questions about all the documents.  
You can also use this repository as a knowledge base and interact with it through AI tools like [cherry studio](https://github.com/CherryHQ/cherry-studio).

website: https://docs.banbot.site

Both language trees include [factor/cross-sectional workflows](en-US/guide/factor.md),
[中文多因子指南](zh-CN/guide/factor.md) and the corresponding API pages.
When editing engine behavior, update concepts/configuration/strategy/backtest/live/CLI
and sidebar/API navigation together. From the repository root, run:

```shell
npm ci --prefix bandoc --no-audit --no-fund
npm run build --prefix bandoc
python scripts/check_documentation_links.py
```

The link check supports VitePress language roots, extensionless pages and public assets;
it does not fetch external links or validate fragment anchors. Live examples must meet the documented Hub/provider and current-session verification
requirements; configuration examples do not certify real-venue readiness.

## Documentation scope

The user guides describe supported workflows, configuration, public strategy/data APIs,
report interpretation, and operational limits. Internal module inventories, ownership
implementation details and contributor procedures belong in `doc/project/`.
Update both language trees when those contracts change; revise existing examples and
navigation alongside new features. Shared-account operations and notifications have
separate guides:

- [Shared accounts and Hub](en-US/guide/execution.md) / [共享账户与 Hub](zh-CN/guide/execution.md)
- [Notifications](en-US/guide/notifications.md) / [消息通知](zh-CN/guide/notifications.md)

用户文档以 `doc/project/` 和当前源码为依据，保留用户需要的操作、示例、API 与限制；
内部模块清单和开发维护流程留在项目文档。涉及用户行为变化时，同步修订中英文正文、
示例和导航，直接描述当前实现。

## Compilation and Deployment
To compile the bot documentation:
```shell
npm run build
```
Then package the output directory `.vitepress\dist` and upload it to the server:

这是banbot的文档仓库，本项目使用[vitepress](https://vitepress.dev/)生成文档站点。

您可下载此仓库，然后使用cursor打开，在chat中使用@codebase快速对全部文档进行问答。  
您也可将此仓库作为知识库，通过[cherry studio](https://github.com/CherryHQ/cherry-studio)等AI工具问答。

站点：https://docs.banbot.site

## 编译和部署
编译机器人文档：
```shell
npm run build
```
然后打包输出目录`.vitepress\dist`上传到服务器：
