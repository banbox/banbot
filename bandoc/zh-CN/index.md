---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Banbot"
  text: "一个高性能、易用的事件驱动交易机器人"
  tagline: "多品种、多策略、多周期、多账户"
  actions:
    - theme: brand
      text: 文档
      link: /zh-CN/guide/start

    - theme: alt
      text: 多因子与截面
      link: /zh-CN/guide/factor

features:
  - title: 高性能
    details: Go 与 banta 状态缓存指标库；耗时取决于策略、数据和品种规模
  - title: 更易用
    details: 一个版本的策略，同时支持回测、模拟实时交易和实盘
  - title: 双策略引擎
    details: 时序策略、多因子截面、配对统计套利与混合 events 回放
  - title: 规模化
    details: 可将一批策略同时应用到多个交易所账户
  - title: 任意时序数据
    details: 统一读取 K 线、自定义字段与第三方序列，保留字段类型和 NULL
  - title: 超参数调优
    details: 支持bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
---
