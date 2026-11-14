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

    - theme: alt
      text: Multi-factor strategies
      link: /en-US/guide/factor

features:
  - title: High Performance
    details: Go and banta cache indicator state; runtime depends on strategies, data and universe size
  - title: Easy to use
    details: one version of the strategy, supporting backtesting, simulated real-time trading and real trading at the same time
  - title: Two strategy engines
    details: Time-series strategies, factor portfolios, statistical pairs and mixed events replay
  - title: Scalability
    details: A batch of strategies can be applied to multiple exchange accounts at the same time
  - title: Arbitrary time-series data
    details: Read K-lines, extension fields and external series while preserving types and NULL
  - title: Hyperparameter tuning
    details: Support bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
---
