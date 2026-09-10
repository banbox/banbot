package orm

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"math"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
)

// PhysicalKlineGap describes a missing half-open interval in a physical K-line
// table. The bounds use Unix milliseconds.
type PhysicalKlineGap struct {
	StartMS int64 `json:"start_ms"`
	StopMS  int64 `json:"stop_ms"`
}

// PhysicalKlineManifest proves which physical rows back a requested consumer
// timeframe. DataSHA256 hashes timestamps and every stored OHLCV field in order.
type PhysicalKlineManifest struct {
	SID                   int32              `json:"sid"`
	Exchange              string             `json:"exchange"`
	ExgReal               string             `json:"exg_real"`
	Market                string             `json:"market"`
	Symbol                string             `json:"symbol"`
	Combined              bool               `json:"combined"`
	RequestedTF           string             `json:"requested_timeframe"`
	StorageTF             string             `json:"storage_timeframe"`
	Table                 string             `json:"table"`
	RequestedStart        int64              `json:"requested_start_ms"`
	RequestedEnd          int64              `json:"requested_end_ms"`
	ConsumerStartMS       int64              `json:"consumer_start_ms"`
	ConsumerStopMS        int64              `json:"consumer_stop_ms"`
	ConsumerAlignMS       int64              `json:"consumer_align_ms"`
	StartMS               int64              `json:"start_ms"`
	StopMS                int64              `json:"stop_ms"`
	StorageAlignMS        int64              `json:"storage_align_ms"`
	AuditedStorageStartMS int64              `json:"audited_storage_start_ms,omitempty"`
	StartBoundaryReason   string             `json:"start_boundary_reason,omitempty"`
	ListMS                int64              `json:"list_ms"`
	DelistMS              int64              `json:"delist_ms"`
	FirstMS               int64              `json:"first_ms"`
	LastMS                int64              `json:"last_ms"`
	RowCount              int64              `json:"row_count"`
	ExpectedRows          int64              `json:"expected_rows"`
	Missing               []PhysicalKlineGap `json:"missing"`
	TimestampSHA256       string             `json:"timestamp_sha256"`
	DataSHA256            string             `json:"data_sha256"`
	Complete              bool               `json:"complete"`
	NonApplicable         bool               `json:"non_applicable,omitempty"`
	BoundaryReason        string             `json:"boundary_reason,omitempty"`
}

type physicalKlineCollector struct {
	startMS    int64
	stopMS     int64
	stepMS     int64
	nextMS     int64
	firstMS    int64
	lastMS     int64
	rowCount   int64
	missing    []PhysicalKlineGap
	hasher     hash.Hash
	dataHasher hash.Hash
}

func newPhysicalKlineCollector(startMS, stopMS, stepMS int64) *physicalKlineCollector {
	return &physicalKlineCollector{
		startMS: startMS,
		stopMS:  stopMS,
		stepMS:  stepMS,
		nextMS:  startMS,
		hasher:  sha256.New(),
		dataHasher: func() hash.Hash {
			hasher := sha256.New()
			_, _ = hasher.Write([]byte("banbot-physical-kline-data-v1\x00"))
			return hasher
		}(),
	}
}

func (c *physicalKlineCollector) add(timestamp int64) error {
	if timestamp < c.startMS || timestamp >= c.stopMS {
		return fmt.Errorf("physical K-line timestamp %d is outside [%d,%d)", timestamp, c.startMS, c.stopMS)
	}
	if (timestamp-c.startMS)%c.stepMS != 0 {
		return fmt.Errorf("physical K-line timestamp %d is not aligned to %d", timestamp, c.stepMS)
	}
	if timestamp < c.nextMS {
		return fmt.Errorf("physical K-line timestamps are duplicated or out of order at %d", timestamp)
	}
	if timestamp > c.nextMS {
		c.missing = append(c.missing, PhysicalKlineGap{StartMS: c.nextMS, StopMS: timestamp})
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(timestamp))
	_, _ = c.hasher.Write(encoded[:])
	if c.rowCount == 0 {
		c.firstMS = timestamp
	}
	c.lastMS = timestamp
	c.rowCount++
	c.nextMS = timestamp + c.stepMS
	return nil
}

func (c *physicalKlineCollector) addRow(timestamp int64, open, high, low, close, volume, quote,
	buyVolume float64, tradeNum int64,
) error {
	if err := c.add(timestamp); err != nil {
		return err
	}
	values := []float64{open, high, low, close, volume, quote, buyVolume}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(timestamp))
	_, _ = c.dataHasher.Write(encoded[:])
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("physical K-line contains non-finite value")
		}
		if value == 0 {
			value = 0
		}
		binary.BigEndian.PutUint64(encoded[:], math.Float64bits(value))
		_, _ = c.dataHasher.Write(encoded[:])
	}
	binary.BigEndian.PutUint64(encoded[:], uint64(tradeNum))
	_, _ = c.dataHasher.Write(encoded[:])
	return nil
}

func (c *physicalKlineCollector) finish() (firstMS, lastMS, rowCount int64, missing []PhysicalKlineGap,
	timestampDigest, dataDigest string,
) {
	if c.nextMS < c.stopMS {
		c.missing = append(c.missing, PhysicalKlineGap{StartMS: c.nextMS, StopMS: c.stopMS})
	}
	return c.firstMS, c.lastMS, c.rowCount, c.missing,
		hex.EncodeToString(c.hasher.Sum(nil)), hex.EncodeToString(c.dataHasher.Sum(nil))
}

func physicalKlineStorage(requestedTF string) (string, string, *errs.Error) {
	if secs, err := utils2.TFToSecSafe(requestedTF); err != nil || secs <= 0 {
		return "", "", errs.NewMsg(core.ErrInvalidTF, "invalid timeframe: %s", requestedTF)
	}
	table, storageTF, _ := resolveTablePg(requestedTF)
	if storageTF == "" {
		storageTF = requestedTF
	}
	switch table {
	case "kline_1m", "kline_5m", "kline_15m", "kline_1h", "kline_1d":
	default:
		return "", "", errs.NewMsg(core.ErrInvalidTF, "timeframe %s has no physical K-line table", requestedTF)
	}
	if table != "kline_"+storageTF {
		return "", "", errs.NewMsg(core.ErrInvalidTF, "timeframe %s resolved to inconsistent storage", requestedTF)
	}
	return storageTF, table, nil
}

// PhysicalKlineStorageTimeframe returns the canonical physical timeframe used
// to serve requestedTF in TimescaleDB.
func PhysicalKlineStorageTimeframe(requestedTF string) (string, *errs.Error) {
	storageTF, _, err := physicalKlineStorage(requestedTF)
	return storageTF, err
}

func alignPhysicalKlineFloor(value, step, offset int64) int64 {
	return (value-offset)/step*step + offset
}

func alignPhysicalKlineCeil(value, step, offset int64) int64 {
	floor := alignPhysicalKlineFloor(value, step, offset)
	if floor < value {
		return floor + step
	}
	return floor
}

type physicalKlineBounds struct {
	consumerStart       int64
	consumerStop        int64
	storageStart        int64
	storageStop         int64
	auditedStorageStart int64
	startReason         string
	reason              string
}

func physicalKlineCoverageBounds(startMS, stopMS, listMS, delistMS, consumerStepMS,
	consumerOffsetMS, storageStepMS, storageOffsetMS int64,
) physicalKlineBounds {
	consumerStart := alignPhysicalKlineCeil(startMS, consumerStepMS, consumerOffsetMS)
	if listMS > consumerStart {
		consumerStart = alignPhysicalKlineCeil(listMS, consumerStepMS, consumerOffsetMS)
	}
	consumerStop := alignPhysicalKlineFloor(stopMS, consumerStepMS, consumerOffsetMS)
	storageStart := alignPhysicalKlineFloor(consumerStart, storageStepMS, storageOffsetMS)
	storageStop := alignPhysicalKlineFloor(consumerStop, storageStepMS, storageOffsetMS)
	reason := ""
	if delistMS > 0 && delistMS < stopMS {
		consumerStop = alignPhysicalKlineCeil(delistMS, consumerStepMS, consumerOffsetMS)
		storageStop = alignPhysicalKlineCeil(delistMS, storageStepMS, storageOffsetMS)
		reason = "delisted_market"
	}
	return physicalKlineBounds{
		consumerStart: consumerStart, consumerStop: consumerStop,
		storageStart: storageStart, storageStop: storageStop, reason: reason,
	}
}

func applyAuditedPhysicalKlineStorageStart(bounds physicalKlineBounds, storageStartMS,
	consumerStepMS, storageStepMS, storageOffsetMS int64,
) (physicalKlineBounds, error) {
	if storageStepMS >= consumerStepMS {
		return bounds, fmt.Errorf("audited physical K-line storage start requires a derived consumer timeframe")
	}
	if storageStartMS <= bounds.consumerStart {
		return bounds, fmt.Errorf("audited physical K-line storage start must be later than the first consumer bucket start")
	}
	if storageStartMS <= bounds.storageStart {
		return bounds, fmt.Errorf("audited physical K-line storage start must be later than the default storage start")
	}
	if storageStartMS >= bounds.consumerStart+consumerStepMS {
		return bounds, fmt.Errorf("audited physical K-line storage start must remain within the first consumer bucket")
	}
	if storageStartMS >= bounds.storageStop {
		return bounds, fmt.Errorf("audited physical K-line storage start must be before the storage stop")
	}
	if alignPhysicalKlineFloor(storageStartMS, storageStepMS, storageOffsetMS) != storageStartMS {
		return bounds, fmt.Errorf("audited physical K-line storage start is not aligned to storage timeframe")
	}
	bounds.storageStart = storageStartMS
	bounds.auditedStorageStart = storageStartMS
	bounds.startReason = "archived_storage_prefix"
	return bounds, nil
}

// InspectPhysicalKlineCoverage streams physical Timescale timestamps without
// loading OHLCV rows into memory. It never downloads, repairs, or trusts
// sranges metadata.
func (q *Queries) InspectPhysicalKlineCoverage(ctx context.Context, exs *ExSymbol, requestedTF string,
	startMS, stopMS int64,
) (*PhysicalKlineManifest, *errs.Error) {
	return q.inspectPhysicalKlineCoverage(ctx, exs, requestedTF, startMS, stopMS, 0)
}

// InspectPhysicalKlineCoverageWithStorageStart permits an audited physical
// storage prefix inside the first consumer bucket. It never changes ListMS or
// the consumer coverage boundary.
func (q *Queries) InspectPhysicalKlineCoverageWithStorageStart(ctx context.Context, exs *ExSymbol,
	requestedTF string, startMS, stopMS, storageStartMS int64,
) (*PhysicalKlineManifest, *errs.Error) {
	if storageStartMS <= 0 {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "audited physical K-line storage start is required")
	}
	return q.inspectPhysicalKlineCoverage(ctx, exs, requestedTF, startMS, stopMS, storageStartMS)
}

func (q *Queries) inspectPhysicalKlineCoverage(ctx context.Context, exs *ExSymbol, requestedTF string,
	startMS, stopMS, auditedStorageStartMS int64,
) (*PhysicalKlineManifest, *errs.Error) {
	if q == nil || exs == nil || exs.ID <= 0 || startMS <= 0 || stopMS <= startMS {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "physical K-line coverage input is incomplete")
	}
	if q.isQuestDB() {
		return nil, errs.NewMsg(errs.CodeNotSupport, "physical K-line manifest currently requires TimescaleDB")
	}
	storageTF, table, err := physicalKlineStorage(requestedTF)
	if err != nil {
		return nil, err
	}
	consumerStepMS := int64(utils2.TFToSecs(requestedTF) * 1000)
	storageStepMS := int64(utils2.TFToSecs(storageTF) * 1000)
	_, consumerOffsetSecs := utils2.GetTfAlignOrigin(int(consumerStepMS / 1000))
	consumerOffsetMS := int64(consumerOffsetSecs * 1000)
	storageOffsetMS := seriesAlignOff(exs, storageStepMS)
	if storageOffsetMS == 0 && q.symbolByID(exs.ID) == nil && q.usesLegacySymbolCatalog() {
		storageOffsetMS = GetAlignOff(exs.ID, storageStepMS)
	}
	requestedStart, requestedStop := startMS, stopMS
	bounds := physicalKlineCoverageBounds(startMS, stopMS, exs.ListMs, exs.DelistMs,
		consumerStepMS, consumerOffsetMS, storageStepMS, storageOffsetMS)
	if auditedStorageStartMS > 0 {
		var boundsErr error
		bounds, boundsErr = applyAuditedPhysicalKlineStorageStart(bounds, auditedStorageStartMS,
			consumerStepMS, storageStepMS, storageOffsetMS)
		if boundsErr != nil {
			return nil, errs.New(errs.CodeParamInvalid, boundsErr)
		}
	}
	startMS, stopMS = bounds.storageStart, bounds.storageStop
	base := PhysicalKlineManifest{
		SID: exs.ID, Exchange: exs.Exchange, ExgReal: exs.ExgReal, Market: exs.Market,
		Symbol: exs.Symbol, Combined: exs.Combined, RequestedTF: requestedTF, StorageTF: storageTF, Table: table,
		RequestedStart: requestedStart, RequestedEnd: requestedStop,
		ConsumerStartMS: bounds.consumerStart, ConsumerStopMS: bounds.consumerStop, ConsumerAlignMS: consumerOffsetMS,
		StartMS: startMS, StopMS: stopMS, StorageAlignMS: storageOffsetMS,
		AuditedStorageStartMS: bounds.auditedStorageStart, StartBoundaryReason: bounds.startReason,
		ListMS: exs.ListMs, DelistMS: exs.DelistMs, BoundaryReason: bounds.reason,
	}
	if stopMS <= startMS {
		base.NonApplicable = true
		base.BoundaryReason = "no_tradable_range"
		base.TimestampSHA256 = hex.EncodeToString(sha256.New().Sum(nil))
		emptyData := sha256.New()
		_, _ = emptyData.Write([]byte("banbot-physical-kline-data-v1\x00"))
		base.DataSHA256 = hex.EncodeToString(emptyData.Sum(nil))
		return &base, nil
	}

	rows, queryErr := q.db.Query(ctx, fmt.Sprintf(
		"SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num FROM %s "+
			"WHERE sid=$1 AND time >= $2 AND time < $3 ORDER BY time", table),
		exs.ID, startMS, stopMS)
	if queryErr != nil {
		return nil, NewDbErr(core.ErrDbReadFail, queryErr)
	}
	defer rows.Close()
	collector := newPhysicalKlineCollector(startMS, stopMS, storageStepMS)
	for rows.Next() {
		var timestamp, tradeNum int64
		var open, high, low, close, volume, quote, buyVolume float64
		if scanErr := rows.Scan(&timestamp, &open, &high, &low, &close, &volume, &quote, &buyVolume,
			&tradeNum); scanErr != nil {
			return nil, NewDbErr(core.ErrDbReadFail, scanErr)
		}
		if collectErr := collector.addRow(timestamp, open, high, low, close, volume, quote, buyVolume,
			tradeNum); collectErr != nil {
			return nil, errs.New(core.ErrInvalidBars, collectErr)
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, NewDbErr(core.ErrDbReadFail, rowsErr)
	}
	firstMS, lastMS, rowCount, missing, timestampDigest, dataDigest := collector.finish()
	expected := (stopMS - startMS) / storageStepMS
	base.FirstMS, base.LastMS, base.RowCount, base.ExpectedRows = firstMS, lastMS, rowCount, expected
	base.Missing, base.TimestampSHA256, base.DataSHA256 = missing, timestampDigest, dataDigest
	base.Complete = rowCount == expected && len(missing) == 0
	return &base, nil
}
