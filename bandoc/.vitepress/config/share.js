export function getApiMenus(lang=''){
    const prefix = `${lang}/api`
    return [
        {text: 'core', link: `${prefix}/core`},
        {text: 'btime', link: `${prefix}/btime`},
        {text: 'utils', link: `${prefix}/utils`},
        {text: 'config', link: `${prefix}/config`},
        {text: 'exg', link: `${prefix}/exg`},
        {text: 'orm', link: `${prefix}/orm`},
        {text: 'data', link: `${prefix}/data`},
        {text: 'strat', link: `${prefix}/strat`},
        {text: 'goods', link: `${prefix}/goods`},
        {text: 'biz', link: `${prefix}/biz`},
        {text: 'opt', link: `${prefix}/opt`},
        {text: 'live', link: `${prefix}/live`},
        {text: 'entry', link: `${prefix}/entry`},
    ]
}