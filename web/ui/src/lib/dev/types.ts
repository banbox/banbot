export interface BtTask {
  id: number; // 任务唯一标识
  mode: string; // 模式
  engines?: string[];
  executionMode?: string;
  unified?: boolean;
  run?: unknown;
  reportPaths?: string[];
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

// Version 1 run.json uses Go's exported field names (PascalCase).
export interface FactorNumeric {
  Value: number | null;
  Validity: 'valid' | 'missing' | 'null' | 'not-numeric' | 'non-finite' | 'warmup';
}

export interface FactorSeriesSummary {
  Sections: number;
  MeanIC: number;
  MeanRankIC: number;
  ICIR: FactorNumeric;
  RankICIR: FactorNumeric;
  QuintileMean: FactorNumeric[];
}

export interface ResultBook {
  Cash: number;
  NAV: number;
  Fees: number;
  Slippage: number;
  Funding: number;
  Turnover: number;
  Quantities: Record<string, number> | null;
}

export interface ResultManifest {
  Currency: string;
  CodeRevision: string;
  FactorPlanHash: string;
  UniverseVersion: string;
  VisibilityPolicy: string;
  ExecutionMode: string;
  LatencyAssumption: string;
  StaticUniverse: boolean;
  Combo: { Method: string; Columns: string[] | null; Weights: Record<string, number> | null };
  Portfolio: { Builder: string; K: number; LongNotional: number; ShortNotional: number; Mode: string };
  Labels: { Name: string; Kind: string; Horizon: number; Overlapping: boolean; PeriodsPerYear: number }[] | null;
  Parameters: Record<string, number> | null;
  Costs: { FeeRate: number; SlippageRate: number; FundingPolicy: string };
  Snapshots: {
    ID: string; ContentDigest: string; AdjustmentVersion: string;
    Schemas: Record<string, string> | null;
    SourceVersions: Record<string, string> | null;
    Revisions: Record<string, number> | null;
  }[] | null;
}

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface UnifiedResult {
  Engine: string;
  StrategyID: string;
  AccountID: string;
  TargetsAccepted: number;
  Fills: number;
  AccountFills: number;
  Account: {
    AccountSettledCash: string;
    UnassignedCash: string;
    RiskFrozen: boolean;
    SyntheticStrategyCash: Record<string, string> | null;
    PnLReclassification: string;
    Lots: JsonValue[] | null;
    ActualPositions: JsonValue[] | null;
    ExternalPositions: JsonValue[] | null;
    Orders: JsonValue[] | null;
    Checkpoint: number;
  } | null;
  Decisions: number;
  Executions: number;
  Skipped: number;
  Incomplete: number;
  Unresolved: number;
  ManifestID: string;
  StrategyHash: string;
  Book: ResultBook;
  Summary: Record<string, Record<string, FactorSeriesSummary>> | null;
  Manifest: ResultManifest;
  NodeCount: number;
  MaxRawRecords: number;
  MaxPendingEvaluations: number;
  MaxRetainedValues: number;
  NodeUpdates: Record<string, number> | null;
}

export interface UnifiedBacktestReport {
  Version: 1;
  Status: 'complete' | 'incomplete';
  Errors?: string[];
  Results: UnifiedResult[] | null;
}
