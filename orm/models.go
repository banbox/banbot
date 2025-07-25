// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0

package orm

type AdjFactor struct {
	ID      int32   `json:"id"`
	Sid     int32   `json:"sid"`
	SubID   int32   `json:"sub_id"`
	StartMs int64   `json:"start_ms"`
	Factor  float64 `json:"factor"`
}

type Calendar struct {
	ID      int32  `json:"id"`
	Name    string `json:"name"`
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
	ID        int32  `json:"id"`
	Sid       int32  `json:"sid"`
	Timeframe string `json:"timeframe"`
	StartMs   int64  `json:"start_ms"`
	StopMs    int64  `json:"stop_ms"`
}

type KHole struct {
	ID        int64  `json:"id"`
	Sid       int32  `json:"sid"`
	Timeframe string `json:"timeframe"`
	Start     int64  `json:"start"`
	Stop      int64  `json:"stop"`
	NoData    bool   `json:"no_data"`
}

type KInfo struct {
	Sid       int32  `json:"sid"`
	Timeframe string `json:"timeframe"`
	Start     int64  `json:"start"`
	Stop      int64  `json:"stop"`
}

type KlineUn struct {
	Sid       int32   `json:"sid"`
	StartMs   int64   `json:"start_ms"`
	StopMs    int64   `json:"stop_ms"`
	Timeframe string  `json:"timeframe"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Info      float64 `json:"info"`
}
