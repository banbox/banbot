import { defineConfig } from 'vitepress'
import {getApiMenus} from './share'

export default defineConfig({
    label: '简体中文',
    lang: 'zh-CN',
    description: "一个高性能、易用、多品种、多策略、多周期、多账户的事件驱动交易机器人。",
    themeConfig: {
        // https://vitepress.dev/reference/default-theme-config
        nav: [
            { text: '主页', link: '/zh-CN' },
            { text: '文档', link: '/zh-CN/guide/start' },
            { text: 'API', link: '/zh-CN/api' }
        ],

        sidebar: {
            '/zh-CN/': [
                {
                    text: '开始',
                    items: [
                        { text: '介绍', link: '/zh-CN/guide/start' },
                        { text: '快速开始', link: '/zh-CN/guide/quick_local' },
                        { text: 'Banbot基础', link: '/zh-CN/guide/basic' },
                        { text: '命令行', link: '/zh-CN/guide/bot_usage' },
                        { text: '配置', link: '/zh-CN/guide/configuration' },
                        { text: '自定义策略', link: '/zh-CN/guide/strat_custom' },
                        { text: '回测', link: '/zh-CN/guide/backtest' },
                        { text: '品种管理器', link: '/zh-CN/guide/pair_filters' },
                        { text: '实时交易', link: '/zh-CN/guide/live_trading' },
                        { text: '常见问题', link: '/zh-CN/guide/faq' }
                    ]
                },{
                    text: '进阶',
                    items: [
                        { text: '初始化新项目', link: '/zh-CN/guide/init_project' },
                        {text: '自定义CMD', link: '/zh-CN/advanced/custom_cmd'},
                        {text: 'K线工具', link: '/zh-CN/advanced/kline_tools'},
                        { text: '超参数优化', link: '/zh-CN/guide/hyperopt' },
                        { text: '滚动优化回测', link: '/zh-CN/guide/roll_btopt' },
                        {text: 'AI', link: '/zh-CN/advanced/ai'}
                    ]
                },{
                    text: 'DeepWiki',
                    items: [
                        {text: 'banbot', link: 'https://deepwiki.com/banbox/banbot'},
                        {text: 'banexg', link: 'https://deepwiki.com/banbox/banexg'},
                        {text: 'banta', link: 'https://deepwiki.com/banbox/banta'},
                    ]
                }
            ],
            '/zh-CN/api/': getApiMenus('/zh-CN')
        },

        docFooter: {
            prev: '上一篇',
            next: '下一篇'
        },
        editLink: {
            pattern: 'https://github.com/banbox/bandoc/edit/master/bot/:path',
            text: '在 GitHub 上编辑此页面'
        },
        lastUpdated: {
            text: '最后更新于',
            formatOptions: {
                dateStyle: 'short',
                timeStyle: 'medium'
            }
        },
        sidebarMenuLabel: '菜单',
        darkModeSwitchLabel: '主题',
        outline: {
            label: '页面导航'
        },
        returnToTopLabel: '返回顶部',
        lightModeSwitchTitle: '切换到浅色模式',
        darkModeSwitchTitle: '切换到深色模式'
    }
})

export const search = {
    'zh-CN': {
        placeholder: '搜索文档',
        translations: {
            button: {
                buttonText: '搜索文档',
                buttonAriaLabel: '搜索文档'
            },
            modal: {
                noResultsText: '无法找到相关结果',
                resetButtonTitle: '清除查询条件',
                footer: {
                    selectText: '选择',
                    navigateText: '切换',
                    closeText: '关闭',
                }
            }
        }
    }
}