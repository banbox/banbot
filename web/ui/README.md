# BanBot UI
这是交易机器人banbot的前端部分；K线使用Klinecharts；基于sveltekit架构；ui使用daisyUI+tailwind

## 开发笔记
** 国际化 **  
使用inlang的国际化[插件](https://inlang.com/m/dxnzrydw/paraglide-sveltekit-i18n/getting-started)
1. 在 project.inlang/settings.json 中添加语言标签，并在 messages 文件夹下添加语言文件。
2. 在需要使用语言标签的地方，使用:
```typescript
import * as m from '$lib/paraglide/messages.js'
m.hello()
m['hello']()
```

**注册新指标**  
在 src/lib/coms.ts 中添加新指标的字段和默认参数。

**编译为静态资源**  
sveltekit支持编译静态资源，只需将svelte.config.js中的`adapter-auto`改为`adapter-static`，然后执行`npm run build`，即可在dist目录下生成静态资源。

**云端指标**  
本项目原生支持云端指标加载和显示，后端需提供`/kline/all_inds`和`/kline/calc_ind`接口，具体参数请参考`src/lib/indicators/cloudInds.ts`

## TODO
* 覆盖物从localstorage中恢复
* textBox多行文本边界计算不正确
* 滚动条样式未全局生效

# sv

Everything you need to build a Svelte project, powered by [`sv`](https://github.com/sveltejs/cli).

## Creating a project

If you're seeing this, you've probably already done this step. Congrats!

```bash
# create a new project in the current directory
npx sv create

# create a new project in my-app
npx sv create my-app
```

## Developing

Once you've created a project and installed dependencies with `npm install` (or `pnpm install` or `yarn`), start a development server:

```bash
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

## Building

To create a production version of your app:

```bash
npm run build
```

You can preview the production build with `npm run preview`.

> To deploy your app, you may need to install an [adapter](https://svelte.dev/docs/kit/adapters) for your target environment.