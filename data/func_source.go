package data

import (
	"context"
	"fmt"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

type FetchHistoryFunc func(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error)

type SubscribeLiveFunc func(ctx context.Context, subs []*strat.DataSub, sink DataSink) error

type FuncDataSource struct {
	info      *orm.SeriesInfo
	fetch     FetchHistoryFunc
	subscribe SubscribeLiveFunc
}

func NewFuncDataSource(info *orm.SeriesInfo, fetch FetchHistoryFunc, subscribe SubscribeLiveFunc) (*FuncDataSource, error) {
	if fetch == nil {
		return nil, fmt.Errorf("data source fetch function is required")
	}
	if err := orm.ValidateSeriesInfo(info); err != nil {
		return nil, err
	}
	return &FuncDataSource{
		info:      info,
		fetch:     fetch,
		subscribe: subscribe,
	}, nil
}

func RegisterFuncDataSource(info *orm.SeriesInfo, fetch FetchHistoryFunc, subscribe SubscribeLiveFunc) error {
	src, err := NewFuncDataSource(info, fetch, subscribe)
	if err != nil {
		return err
	}
	return RegisterDataSource(src)
}

func (s *FuncDataSource) Info() *orm.SeriesInfo {
	if s == nil {
		return nil
	}
	return s.info
}

func (s *FuncDataSource) FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
	if s == nil || s.fetch == nil {
		return nil, fmt.Errorf("data source fetch function is required")
	}
	return s.fetch(ctx, sub, startMS, endMS)
}

func (s *FuncDataSource) SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error {
	if s == nil {
		return fmt.Errorf("data source is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.subscribe == nil {
		return nil
	}
	return s.subscribe(ctx, subs, sink)
}

var _ DataSource = (*FuncDataSource)(nil)
