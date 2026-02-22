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

features:
  - title: 高性能
    details: 采用支持状态缓存的指标库，1秒轻松回测1年的数据
  - title: 更易用
    details: 一个版本的策略，同时支持回测、模拟实时交易和实盘
  - title: 更灵活
    details: 自由组合不同的品种、策略和时间周期
  - title: 规模化
    details: 可将一批策略同时应用到多个交易所账户
  - title: LLM集成
    details: 内置大模型调用与管理能力，支持多模型故障转移与并发控制
  - title: 超参数调优
    details: 支持bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
---
