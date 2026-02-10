<script lang="ts">
  import * as m from '$lib/paraglide/messages.js'
  import CodeMirror from '$lib/dev/CodeMirror.svelte';
  import { oneDark } from '@codemirror/theme-one-dark';
  import type { Extension } from '@codemirror/state';
  import { onMount } from 'svelte';
  import {getLocale} from "$lib/paraglide/runtime";

  const cfgYml = `name: local  # ${m.cfg_name()}
env: prod  # ${m.cfg_env()}
leverage: 2  # ${m.cfg_leverage()}
limit_vol_secs: 5  # ${m.cfg_limit_vol_secs()}
put_limit_secs: 120  # ${m.cfg_put_limit_secs()}
account_pull_secs: 60  # ${m.cfg_account_pull_secs()}
market_type: spot  # ${m.cfg_market_type()}
contract_type: swap  # ${m.cfg_contract_type()}
odbook_ttl: 1000  # ${m.cfg_odbook_ttl()}
concur_num: 2  # ${m.cfg_concur_num()}
order_type: market  # ${m.cfg_order_type()}
stop_enter_bars: 20  # ${m.cfg_stop_enter_bars()}
prefire: 0  # ${m.cfg_prefire()}
margin_add_rate: 0.66  # ${m.cfg_margin_add_rate()}
stake_amount: 15  # ${m.cfg_stake_amount()}
stake_pct: 50  # ${m.cfg_stake_pct()}
max_stake_amt: 5000  # ${m.cfg_max_stake_amount()}
draw_balance_over: 0  # ${m.cfg_draw_balance_over()}
charge_on_bomb: false # ${m.cfg_charge_on_bomb()}
take_over_strat: ma:demo # ${m.cfg_take_over_strat()}
close_on_stuck: 20  # ${m.cfg_close_on_stuck()}
open_vol_rate: 1  # ${m.cfg_open_vol_rate()}
min_open_rate: 0.5  # ${m.cfg_min_open_rate()}
low_cost_action: ignore  # ${m.cfg_low_cost_action()}
bt_net_cost: 15  # ${m.cfg_bt_net_cost()}
relay_sim_unfinish: false  # ${m.cfg_relay_sim_unfinish()}
order_bar_max: 500  # ${m.cfg_order_bar_max()}
ntp_lang_code: none  # ${m.cfg_ntp_lang_code()}
wallet_amounts:  # ${m.cfg_wallet_amounts()}
  USDT: 10000
stake_currency: [USDT, TUSD]  # ${m.cfg_stake_currency()}
fatal_stop:  # ${m.cfg_fatal_stop()}
  '1440': 0.1  # ${m.cfg_fatal_stop_1440()}
  '180': 0.2  # ${m.cfg_fatal_stop_180()}
  '30': 0.3  # ${m.cfg_fatal_stop_30()}
fatal_stop_hours: 8  # ${m.cfg_fatal_stop_hours()}
time_start: "20240701"  # ${m.cfg_time_start()}
time_end: "20250701"
run_timeframes: [5m]  # ${m.cfg_run_timeframes()}
run_policy:  # ${m.cfg_run_policy()}
  - name: Demo  # ${m.cfg_run_policy_name()}
    run_timeframes: [5m]  # ${m.cfg_run_policy_timeframes()}
    refine_tf: 1m  # ${m.cfg_run_policy_refine_tf()}
    filters:  # ${m.cfg_run_policy_filters()}
    - name: OffsetFilter
      offset: 10
      limit: 30
    max_pair: 999  # ${m.cfg_run_policy_max_pair()}
    max_open: 10  # ${m.cfg_run_policy_max_open()}
    max_simul_open: 0 # ${m.cfg_run_policy_max_simul_open()}
    order_bar_max: 0  # ${m.cfg_run_policy_order_bar_max()}
    stake_rate: 1  # ${m.cfg_run_policy_stake_rate()}
    stop_loss: 1  # ${m.cfg_run_policy_stop_loss()}
    dirt: any  # ${m.cfg_run_policy_dirt()}
    pairs: [BTC/USDT:USDT]
    params: {atr: 15}
    pair_params:
      BTC/USDT:USDT: {atr:14}
    strat_perf:
      enable: false
strat_perf:
  enable: false  # ${m.cfg_strat_perf_enable()}
  min_od_num: 5  # ${m.cfg_strat_perf_min_od_num()}
  max_od_num: 30  # ${m.cfg_strat_perf_max_od_num()}
  min_job_num: 10  # ${m.cfg_strat_perf_min_job_num()}
  mid_weight: 0.2  # ${m.cfg_strat_perf_mid_weight()}
  bad_weight: 0.1  # ${m.cfg_strat_perf_bad_weight()}
pairs:  # ${m.cfg_pairs()}
- SOL/USDT:USDT
- UNFI/USDT:USDT
- SFP/USDT:USDT
pairmgr:
  cron: '25 1 0 */2 * *' # ${m.cfg_pairmgr_corn()}
  offset: 0  # ${m.cfg_pairmgr_offset()}
  limit: 999  # ${m.cfg_pairmgr_limit()}
  force_filters: false # ${m.cfg_pairmgr_force_filters()}
  pos_on_rotation: hold  # ${m.cfg_pairmgr_pos_on_rotation()}
  use_latest: false  # ${m.cfg_pairmgr_use_latest()}
pairlists:  # ${m.cfg_pairlists()}
  - name: VolumePairList  # ${m.cfg_pairlists_vol()}
    limit: 100  # ${m.cfg_pairlists_limit100()}
    min_value: 100000  # ${m.cfg_pairlists_min_value()}
    cache_secs: 7200  # ${m.cfg_pairlists_refresh_secs()}
    back_period: 3d  # ${m.cfg_pairlists_back_period()}
  - name: PriceFilter
    max_unit_value: 100  # ${m.cfg_pairlists_max_unit_value()}
    precision: 0.0015  # ${m.cfg_price_precision()}
    min: 0.001  # ${m.cfg_min_price()}
    max: 100000  # ${m.cfg_max_price()}
  - name: RateOfChangeFilter
    back_days: 5  # ${m.cfg_back_days()}
    min: 0.03  # ${m.cfg_roc_min()}
    max: 10  # ${m.cfg_roc_max()}
    cache_secs: 1440  # ${m.cfg_refresh_period()}
  - name: SpreadFilter  # ${m.cfg_spread_filter()}
    max_ratio: 0.005  # ${m.cfg_spread_max()}
  - name: CorrelationFilter  # ${m.cfg_correlation()}
    min: -1  # ${m.cfg_correlation_val()}
    max: 1  # ${m.cfg_correlation_val()}
    timeframe: 5m  # ${m.cfg_correlation_tf()}
    back_num: 70  # ${m.cfg_correlation_back()}
    sort: asc  # asc/desc/""
    top_n: 50  # ${m.cfg_correlation_topn()}
  - name: VolatilityFilter  # ${m.cfg_volatility()}
    back_days: 10  # ${m.cfg_back_days()}
    max: 1  # ${m.cfg_volatility_max()}
    min: 0.05  # ${m.cfg_volatility_min()}
  - name: AgeFilter  # ${m.cfg_pairlists_age()}
    min: 5
  - name: OffsetFilter  # ${m.cfg_pairlists_offset()}
    reverse: false  # reverse array
    offset: 10
    rate: 0.5  # 50% of array
    limit: 30
  - name: ShuffleFilter  # ${m.cfg_pairlists_shuffle()}
    seed: 42
accounts:
  user1:  # ${m.cfg_acc_name()}
    no_trade: false  # ${m.cfg_acc_no_trade()}
    stake_rate: 1  # ${m.cfg_acc_stake_rate()}
    leverage: 0  # ${m.cfg_acc_lvg()}
    max_stake_amt: 0  # ${m.cfg_acc_max_stake()}
    max_pair: 0  # ${m.cfg_acc_max_pair()}
    max_open_orders: 0  # ${m.cfg_acc_max_open_orders()}
    binance:
      prod:  # ${m.cfg_acc_prod()}
        api_key: vvv
        api_secret: vvv
      test:  # ${m.cfg_acc_test()}
        api_key: vvv
        api_secret: vvv
    rpc_channels:  # ${m.cfg_rpc_channels()}
      - name: wx_bot
        to_user: ChannelUserID
    api_server:  # ${m.cfg_acc_api_server()}
      pwd: abc
      role: admin
exchange:
  name: binance  # ${m.cfg_exg_name()}
  binance:  # ${m.cfg_exg_options()}
    fees:
      linear:
        taker: 0.0005
        maker: 0.0002
database:
  retention: all
  max_pool_size: 50
  auto_create: true  # ${m.cfg_db_auto_create()}
  url: postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable
spider_addr: 127.0.0.1:6789  # ${m.cfg_spider()}
rpc_channels:  # ${m.cfg_rpc_channels()}
  wx_notify:  # ${m.cfg_rpc_name()}
    corp_id: ww0f12345678b7e
    agent_id: '1000005'
    corp_secret: b123456789_1Cx1234YB9K-MuVW1234
    touser: '@all'
    type: wework  # ${m.cfg_rpc_type()}
    msg_types: [exception]  # ${m.cfg_rpc_msg_types()}
    accounts: []  # ${m.cfg_rpc_account()}
    keywords: []  # ${m.cfg_rpc_keyword()}
    retry_delay: 1000
    disable: true
webhook:  # ${m.cfg_webhook()}
  entry:
    content: "{name} {action}\\nSymbol: {pair} {timeframe}\\nTag: {strategy}  {enter_tag}\\nPrice: {price:.5f}\\nCost: {value:.2f}"
  exit:
    content: "{name} {action}\\nSymbol: {pair} {timeframe}\\nTag: {strategy}  {exit_tag}\\nPrice: {price:.5f}\\nCost: {value:.2f}\\nProfit: {profit:.2f}"
  status:  # ${m.cfg_webhook_status()}
    content: '{name}: {status}'
  exception:
    content: '{name}: {status}'
api_server:  # ${m.cfg_api_server()}
  enable: true
  bind_ip: 127.0.0.1  # ${m.cfg_api_bind_ip()}
  port: 8001
  jwt_secret_key: fn234njkcu89234nbf
  users:
    - user: ban
      pwd: 123
      allow_ips: []
      acc_roles: {user1: admin}
`
  let theme: Extension | null = $state(oneDark);
  let editor: CodeMirror | null = $state(null);

  onMount(() => {
    if(editor){
      editor.setValue('config.yml', cfgYml);
    }else{
      setTimeout(() => {
        editor?.setValue('config.yml', cfgYml);
      }, 10);
    }
  });
</script>

<div class="flex justify-between items-center mb-4">
  <h3 class="text-lg font-semibold">{m.full_config()}</h3>
  <label for="config-drawer" class="btn btn-sm btn-circle">✕</label>
</div>
<a href="https://docs.banbot.site/{getLocale()}/guide/configuration.html" target="_blank" class="link">{m.doc_config()}</a>
<CodeMirror bind:this={editor} {theme} editable={false}/>