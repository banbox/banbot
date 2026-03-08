package orm

import "time"

type AdjFactor struct {
	Sid     int32   `json:"sid"`
	SubID   int32   `json:"sub_id"`
	StartMs int64   `json:"start_ms"`
	Factor  float64 `json:"factor"`
}

type Calendar struct {
	Market  string `json:"market"`
	StartMs int64  `json:"start_ms"`
	StopMs  int64  `json:"stop_ms"`
}

type ExSymbol struct {
	ID       int32  `json:"id"`
	Exchange string `json:"exchange"`
	ExgReal  string `json:"exg_real"`
	Market   string `json:"market"`
	Symbol   string `json:"symbol"`
	Combined bool   `json:"combined"`
	ListMs   int64  `json:"list_ms"`
	DelistMs int64  `json:"delist_ms"`
}

type InsKline struct {
	Sid       int32     `json:"sid"`
	Timeframe string    `json:"timeframe"`
	Ts        time.Time `json:"ts"`
	StartMs   int64     `json:"start_ms"`
	StopMs    int64     `json:"stop_ms"`
}

type SRange struct {
	Sid       int32  `json:"sid"`
	Table     string `json:"table"`
	Timeframe string `json:"timeframe"`
	StartMs   int64  `json:"start_ms"`
	StopMs    int64  `json:"stop_ms"`
	HasData   bool   `json:"has_data"`
}

type KlineUn struct {
	Sid       int32   `json:"sid"`
	StartMs   int64   `json:"start_ms"`
	StopMs    int64   `json:"stop_ms"`
	ExpireMs  int64   `json:"expire_ms"`
	Timeframe string  `json:"timeframe"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Quote     float64 `json:"quote"`
	BuyVolume float64 `json:"buy_volume"`
	TradeNum  int64   `json:"trade_num"`
}
