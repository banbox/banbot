package orm

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/banbox/banexg/utils"
)

type FrequencyKind string

const (
	FrequencyBar   FrequencyKind = "bar"
	FrequencyEvent FrequencyKind = "event"
)

type FieldProjection string

const (
	ProjectionDefault  FieldProjection = "default"
	ProjectionAll      FieldProjection = "all"
	ProjectionSelected FieldProjection = "selected"
)

// Subscription is a source dependency, independent of strategy jobs. Fields
// select raw values; SeriesFields select derived numeric views only.
type Subscription struct {
	Source       string
	ExSymbol     *ExSymbol
	TimeFrame    string
	WarmupNum    int
	Fields       []string
	SeriesFields []string
	Frequency    FrequencyKind
	Projection   FieldProjection
}

// StreamKey identifies a stream without including a consumer or account.
// TimeFrame is "event" for irregular streams. Namespace isolation remains the
// responsibility of the catalog/repository owning this key.
type StreamKey struct {
	Source    string
	SID       int32
	TimeFrame string
}

func (k StreamKey) String() string {
	source := NormalizeSeriesSource(k.Source)
	var sidBuf [12]byte
	sidText := strconv.AppendInt(sidBuf[:0], int64(k.SID), 10)
	var key strings.Builder
	key.Grow(len(source) + len(sidText) + len(k.TimeFrame) + 2)
	key.WriteString(source)
	key.WriteByte(':')
	key.Write(sidText)
	key.WriteByte(':')
	key.WriteString(k.TimeFrame)
	return key.String()
}

func ParseStreamKey(key string) (StreamKey, bool) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 {
		return StreamKey{}, false
	}
	sid, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return StreamKey{}, false
	}
	return StreamKey{Source: parts[0], SID: int32(sid), TimeFrame: parts[2]}, true
}

func (s Subscription) Key() StreamKey {
	var sid int32
	if s.ExSymbol != nil {
		sid = s.ExSymbol.ID
	}
	return StreamKey{Source: NormalizeSeriesSource(s.Source), SID: sid, TimeFrame: s.TimeFrame}
}

// NormalizeSubscription validates the declaration without requiring a reader.
// A valid event declaration does not imply a historical reader supports it.
func NormalizeSubscription(sub Subscription) (Subscription, error) {
	sub.Source = NormalizeSeriesSource(strings.TrimSpace(sub.Source))
	if strings.Contains(sub.Source, ":") {
		return Subscription{}, fmt.Errorf("data sub source must not contain ':'")
	}
	if sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return Subscription{}, fmt.Errorf("data sub exsymbol is required")
	}
	sub.TimeFrame = strings.TrimSpace(sub.TimeFrame)
	if sub.TimeFrame == "" {
		return Subscription{}, fmt.Errorf("data sub timeframe is required")
	}
	if sub.Frequency == "" {
		sub.Frequency = FrequencyBar
		if sub.TimeFrame == "event" {
			sub.Frequency = FrequencyEvent
		}
	}
	switch sub.Frequency {
	case FrequencyBar:
		if err := validateBarAmount(sub.TimeFrame); err != nil {
			return Subscription{}, err
		}
		secs, err := utils.TFToSecSafe(sub.TimeFrame)
		if err != nil || secs <= 0 || int64(secs) > math.MaxInt64/1000 {
			return Subscription{}, fmt.Errorf("invalid bar timeframe %q", sub.TimeFrame)
		}
	case FrequencyEvent:
		if sub.TimeFrame != "event" {
			return Subscription{}, fmt.Errorf("event frequency requires timeframe event")
		}
		if sub.Source == SeriesSourceKline {
			return Subscription{}, fmt.Errorf("kline source requires bar frequency")
		}
	default:
		return Subscription{}, fmt.Errorf("unsupported frequency %q", sub.Frequency)
	}
	if sub.WarmupNum < 0 {
		return Subscription{}, fmt.Errorf("warmup must not be negative")
	}
	if sub.Projection == "" {
		sub.Projection = ProjectionDefault
		if len(sub.Fields) > 0 {
			sub.Projection = ProjectionSelected
		}
	}
	switch sub.Projection {
	case ProjectionDefault, ProjectionAll:
		if len(sub.Fields) > 0 {
			return Subscription{}, fmt.Errorf("%s projection cannot include selected fields", sub.Projection)
		}
	case ProjectionSelected:
		if len(MergeSeriesFields(sub.Fields)) == 0 {
			return Subscription{}, fmt.Errorf("selected projection requires fields")
		}
	default:
		return Subscription{}, fmt.Errorf("unsupported field projection %q", sub.Projection)
	}
	sub.Fields = MergeSeriesFields(sub.Fields)
	sub.SeriesFields = MergeSeriesFields(sub.SeriesFields)
	return sub, nil
}

// banexg supports registered timeframe names, but its numeric parser multiplies
// int values. Check numeric inputs before that multiplication can overflow.
func validateBarAmount(tf string) error {
	if len(tf) < 2 || !(tf[0] >= '0' && tf[0] <= '9' || tf[0] == '+' || tf[0] == '-') {
		return nil
	}
	amount, err := strconv.ParseInt(tf[:len(tf)-1], 10, 64)
	var scale int64
	switch tf[len(tf)-1] {
	case 's', 'S':
		scale = 1
	case 'm':
		scale = utils.SecsMin
	case 'h', 'H':
		scale = utils.SecsHour
	case 'd', 'D':
		scale = utils.SecsDay
	case 'w', 'W':
		scale = utils.SecsWeek
	case 'M':
		scale = utils.SecsMon
	case 'q', 'Q':
		scale = utils.SecsQtr
	case 'y', 'Y':
		scale = utils.SecsYear
	default:
		return nil
	}
	if err != nil || amount <= 0 || amount > math.MaxInt64/(scale*1000) {
		return fmt.Errorf("invalid bar timeframe %q", tf)
	}
	return nil
}

// SubscriptionWarmupStart computes bar lookback only. Event count warmup must
// be implemented by an observation-aware adapter rather than a fake interval.
func SubscriptionWarmupStart(sub Subscription, anchorMS int64) (int64, error) {
	normalized, err := NormalizeSubscription(sub)
	if err != nil {
		return 0, err
	}
	if normalized.Frequency == FrequencyEvent {
		if normalized.WarmupNum > 0 {
			return 0, fmt.Errorf("event count warmup requires an observation-based source bootstrap")
		}
		return anchorMS, nil
	}
	secs, _ := utils.TFToSecSafe(normalized.TimeFrame)
	tfMS := int64(secs) * 1000
	if int64(normalized.WarmupNum) > math.MaxInt64/tfMS {
		return 0, fmt.Errorf("warmup duration overflows")
	}
	duration := int64(normalized.WarmupNum) * tfMS
	if anchorMS < math.MinInt64+duration {
		return 0, fmt.Errorf("warmup start overflows")
	}
	return anchorMS - duration, nil
}
