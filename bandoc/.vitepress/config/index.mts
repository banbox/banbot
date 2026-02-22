import { defineConfig } from 'vitepress'

import zhCN, { search as zhCNSearch } from './zh-CN'
import enUS, { search as enUSSearch } from './en-US'


// https://vitepress.dev/reference/site-config
function config () {
    return defineConfig({
        cleanUrls: true,
        metaChunk: true,
        title: 'Banbot',
        lastUpdated: true,
        markdown: {
            theme: {
                light: 'github-light',
                dark: 'github-dark'
            }
        },
        locales: {
            'en-US': enUS,
            'zh-CN': zhCN
        },
        themeConfig: {
            search: {
                provider: 'local',
                options: {
                    locales: { ...zhCNSearch, ...enUSSearch }
                }
            },
            socialLinks: [
                { icon: 'github', link: 'https://github.com/banbox/banbot' }
            ],
            footer: {
                message: 'Released under the <a href="https://github.com/banbox/banbot/blob/main/LICENSE">AGPL-3.0 License</a>.',
                copyright: `Copyright © 2023-${new Date().getFullYear()} <a href="https://github.com/anyongjin">anyongjin</a>`
            }
        }
    })
}

export default config()
