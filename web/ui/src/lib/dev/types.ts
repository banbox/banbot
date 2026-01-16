export interface BtTask {
  id: number; // 任务唯一标识
  mode: string; // 模式
  args: string; // 参数
  config: string; // 配置
  path: string; // 路径
  strats: string; // 策略
  periods: string; // 时间周期
  pairs: string; // 交易对
  createAt: number; // 创建时间
  startAt: number; // 开始时间
  stopAt: number; // 结束时间
  status: number; // 当前任务状态
  progress: number; // 进度
  orderNum: number; // 订单数量
  profitRate: number; // 收益率
  winRate: number; // 胜率
  maxDrawdown: number; // 最大回撤
  sharpe: number; // 夏普比率

  reals?: number[];
  used?: number[];

  maxOpenOrders?: number; // 最大开单数
  showDrawDownPct?: number; // 显示的回撤百分比
  barNum?: number; // 数据柱数量
  maxDrawDownVal?: number; // 最大回撤值
  showDrawDownVal?: number; // 显示的回撤值
  totFee?: number; // 总费用
  sortinoRatio?: number; // 索提诺比率
  leverage?: number;
  walletAmount?: number;
  stakeAmount?: number;
  info?: string;
  note?: string;
}
