package data

import (
	"context"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type periodSta struct {
	stamps map[int32]int64
	lock   deadlock.Mutex
	msecs  int64
}

func newPeriodSta(tf string) *periodSta {
	msecs := int64(utils2.TFToSecs(tf) * 1000)
	return &periodSta{
		stamps: make(map[int32]int64),
		msecs:  msecs,
	}
}

// 返回指定周期对齐时间戳，此周期已入库的最新bar时间戳
func (p *periodSta) alignAndLast(sess *orm.Queries, sid int32, tf string, curMS int64) (int64, int64) {
	p.lock.Lock()
	// 上一个小时对齐时间戳
	lastMS, _ := p.stamps[sid]
	if lastMS == 0 {
		_, kinfoEnd := sess.GetKlineRange(sid, tf)
		if kinfoEnd > 0 {
			lastMS = kinfoEnd - p.msecs
		}
	}
	hourAlign := utils2.AlignTfMSecs(curMS, p.msecs)
	if lastMS == 0 {
		lastMS = hourAlign - p.msecs
	}
	if hourAlign > lastMS {
		p.stamps[sid] = hourAlign
	}
	p.lock.Unlock()
	return hourAlign, lastMS
}

func (p *periodSta) reset(sid int32, timeMS int64) {
	p.lock.Lock()
	p.stamps[sid] = timeMS
	p.lock.Unlock()
}

func trySaveKlines(job *SaveKline, tfSecs int, mntSta *periodSta, hourSta *periodSta) {
	ctx := context.Background()
	sess, conn, err := orm.Conn(ctx)
	sid := job.Sid
	if err == nil {
		defer conn.Release()
		var addBars = job.Arr
		endMS := addBars[len(addBars)-1].Time + int64(tfSecs*1000)
		savedNewBars := false
		if tfSecs < 60 {
			// 最小保存1m级别k线
			mntAlign, prevMS := mntSta.alignAndLast(sess, sid, "1m", endMS)
			if mntAlign <= prevMS {
				// 未出现新的1m完成k线
				return
			}
			var newEndMS int64
			newEndMS, _, err = downKlineTo(sess, sid, "1m", prevMS, mntAlign)
			if err != nil {
				mntSta.reset(sid, newEndMS)
				log.Error("down kline 1m fail", zap.Int32("sid", sid), zap.Error(err))
				return
			} else if newEndMS < mntAlign {
				log.Warn("down kline 1m insufficient", zap.Int32("sid", sid),
					zap.Int64("exp", mntAlign), zap.Int64("end", newEndMS))
			}
			savedNewBars = newEndMS > prevMS
		} else {
			// 1m级别，可直接入库
			expEndMS := addBars[0].Time
			var nextMS int64
			mntAlign, prevMS := mntSta.alignAndLast(sess, sid, "1m", expEndMS)
			if mntAlign > prevMS {
				// 有缺口，需要先下载缺失部分
				nextMS, _, err = downKlineTo(sess, sid, job.TimeFrame, prevMS, expEndMS)
				if nextMS < expEndMS {
					log.Warn("fetch lack 1m bad", zap.Int32("sid", sid), zap.Int64("end", endMS),
						zap.Int64("expEnd", expEndMS))
				}
			}
			if nextMS > expEndMS {
				// 待插入的k线头部有冗余
				var cutIdx = 0
				for i, bar := range addBars {
					if bar.Time < nextMS {
						cutIdx = i + 1
					} else {
						break
					}
				}
				addBars = addBars[cutIdx:]
			}
			if err == nil && len(addBars) > 0 {
				exs := orm.GetSymbolByID(sid)
				_, err = sess.InsertKLinesAuto(job.TimeFrame, exs, addBars, true)
				if err == nil {
					savedNewBars = true
					mntSta.reset(sid, endMS)
				}
			}
		}
		if err == nil && savedNewBars {
			// 下载1h及以上周期K线数据
			hourAlign, lastMS := hourSta.alignAndLast(sess, sid, "1h", endMS)
			if hourAlign > lastMS {
				_, _, err = downKlineTo(sess, sid, "1h", lastMS, hourAlign)
			}
		}
	}
	if err != nil {
		log.Error("consumeWriteQ: fail", zap.Int32("sid", sid), zap.Error(err))
	} else {
		log.Debug("save kline ok", zap.Int32("sid", sid), zap.Int("num", len(job.Arr)))
	}
}

func downKlineTo(sess *orm.Queries, sid int32, tf string, oldEndMS, toEndMS int64) (int64, *banexg.Kline, *errs.Error) {
	if oldEndMS == 0 {
		_, oldEndMS = sess.GetKlineRange(sid, tf)
	}

	var err *errs.Error
	if oldEndMS == 0 || toEndMS <= oldEndMS {
		// The new coin has no historical data, or the current bar and the inserted data are continuous, and the subsequent new bar can be directly inserted
		// 新的币无历史数据、或当前bar和已插入数据连续，直接插入后续新bar即可
		return oldEndMS, nil, nil
	}
	exs := orm.GetSymbolByID(sid)
	tfMSecs := int64(utils2.TFToSecs(tf) * 1000)
	tryCount := 0
	exchange, err := exg.GetWith(exs.Exchange, exs.Market, "")
	if err != nil {
		return oldEndMS, nil, err
	}
	var newEndMS = oldEndMS
	var saveNum int
	if tf == "1h" {
		orm.DebugDownKLine = true
		defer func() {
			orm.DebugDownKLine = false
		}()
	}
	var last *banexg.Kline
	for tryCount <= 5 {
		tryCount += 1
		saveNum, err = sess.DownOHLCV2DB(exchange, exs, tf, oldEndMS, toEndMS, nil)
		if err != nil {
			_, oldEndMS = sess.GetKlineRange(sid, tf)
			return oldEndMS, nil, err
		}
		saveBars, err := sess.QueryOHLCV(exs, tf, 0, 0, 1, false)
		if err != nil {
			_, oldEndMS = sess.GetKlineRange(sid, tf)
			return oldEndMS, nil, err
		}
		var lastMS = int64(0)
		if len(saveBars) > 0 {
			last = saveBars[len(saveBars)-1]
			lastMS = last.Time
			newEndMS = lastMS + tfMSecs
		}
		if newEndMS >= toEndMS {
			break
		} else {
			//If the latest bar is not obtained, wait for 2s to try again
			//如果未成功获取最新的bar，等待2s重试
			log.Warn("downKlineTo not complete, wait 2s, Your system time may be inaccurate, "+
				"you may need delete ban_ntp.json in Temp directory and retry",
				zap.String("pair", exs.Symbol), zap.Int("ins", saveNum),
				zap.Int64("last", lastMS), zap.Int64("newEnd", newEndMS))
			core.Sleep(time.Second * 2)
		}
	}
	return newEndMS, last, nil
}

func DownEmitHourKlines(dp *LiveProvider, endsMap map[int32]int64) {
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		log.Error("get kline Conn fail", zap.Error(err))
		return
	}
	defer conn.Release()
	curMS := btime.UTCStamp()
	curEndMS := utils2.AlignTfMSecs(curMS, 3600000)
	for exsID, lastEnd := range endsMap {
		exs := orm.GetSymbolByID(exsID)
		if exs == nil {
			continue
		}
		newEnd, last, err := downKlineTo(sess, exs.ID, "1h", lastEnd, curEndMS)
		if err != nil {
			log.Error("downKlineTo 1h fail", zap.Int32("sid", exs.ID), zap.Error(err))
			continue
		}
		endsMap[exsID] = newEnd
		if last == nil {
			continue
		}
		dp.OnKLineMsg(&KLineMsg{
			NotifyKLines: NotifyKLines{
				TFSecs:   3600,
				Interval: 3600,
				Arr:      []*banexg.Kline{last},
			},
			ExgName: core.ExgName,
			Market:  core.Market,
			Pair:    exs.Symbol,
		})
	}
}

/*
Check whether there are any missing K lines, and automatically query and update if there are any.
检查是否有缺失的K线，有则自动查询更新（一般在刚启动时，收到的爬虫推送1mK线不含前面的，需要下载前面的并保存到WaitBar中）
*/
func (j *PairTFCache) fillLacks(pair string, subTfSecs int, startMS, endMS int64) ([]*banexg.Kline, *errs.Error) {
	if j.SubNextMS == 0 || j.SubNextMS >= startMS {
		j.SubNextMS = endMS
		return nil, nil
	}
	// 这里NextMS < startMS，出现了bar缺失，查询更新。
	fetchTF := utils2.SecsToTF(subTfSecs)
	tfMSecs := int64(j.TFSecs * 1000)
	bigStartMS := utils2.AlignTfMSecs(j.SubNextMS, tfMSecs)
	exs, err := orm.GetExSymbolCur(pair)
	if err != nil {
		return nil, err
	}
	_, preBars, err := autoFetchOhlcv(exs, fetchTF, bigStartMS, startMS)
	if err != nil {
		return nil, err
	}
	var doneBars []*banexg.Kline
	j.WaitBar = nil
	if len(preBars) > 0 {
		fromTFMS := int64(subTfSecs * 1000)
		oldBars, _ := utils.BuildOHLCV(preBars, tfMSecs, 0, nil, fromTFMS, j.AlignOffMS, exs.InfoBy())
		if len(oldBars) > 0 {
			j.WaitBar = oldBars[len(oldBars)-1]
			doneBars = oldBars[:len(oldBars)-1]
		}
	}
	j.SubNextMS = endMS
	return doneBars, nil
}

func autoFetchOhlcv(exs *orm.ExSymbol, tf string, startMS, endMS int64) ([]*orm.AdjInfo, []*banexg.Kline, *errs.Error) {
	exchange := exg.Default
	if !exchange.HasApi(banexg.ApiFetchOHLCV, exs.Market) {
		// Downloading K lines is currently not allowed, skip
		// 当前不允许下载K线，跳过
		return nil, nil, nil
	}
	return orm.AutoFetchOHLCV(exchange, exs, tf, startMS, endMS, 0, false, nil)
}
