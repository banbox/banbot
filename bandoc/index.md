---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Banbot"
  text: "a high-performance, easy-to-use event-driven trading robot"
  tagline: "multi-symbol, multi-strategy, multi-period, multi-account"
  actions:
    - theme: brand
      text: Document
      link: /en-US/guide/start
    - theme: brand
      text: 中文文档
      link: /zh-CN/guide/start

features:
  - title: High Performance
    details: Use an indicator library that supports state caching, 1 second backtesting 1 year of data
  - title: Easy to use
    details: one version of the strategy, supporting backtesting, simulated real-time trading and real trading at the same time
  - title: Flexible
    details: Freely combine different products, strategies and time periods
  - title: Event-driven
    details: No lookahead and implement your strategy logic more freely
  - title: Scalability
    details: A batch of strategies can be applied to multiple exchange accounts at the same time
  - title: Hyperparameter tuning
    details: Support bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
---

