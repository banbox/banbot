package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/shopspring/decimal"
)

// CrossVenuePaper owns actual cash and spot units at each venue. It deliberately
// does not use perpetual lots or claim atomic execution across live exchanges.
// Each local paper leg commits its fill and balances in one SQLite transaction.
type CrossVenuePaper struct{ db *sql.DB }

type CrossVenueBalance struct {
	Venue, Asset, Currency string
	Cash, Units            decimal.Decimal
}

type CrossVenueLevel struct{ Price, Units decimal.Decimal }

// ErrCrossVenueDepth reports a valid book with insufficient executable size.
// Opportunity searches may skip that orientation without hiding invalid books.
var ErrCrossVenueDepth = errors.New("execution: insufficient displayed cross-venue depth")

// Book timestamps distinguish market observation, publication and reception.
// Fees are cash commissions, charged on the actual depth-weighted notional.
type CrossVenueBook struct {
	Venue, Asset, Currency        string
	AtMS, PublishedMS, ReceivedMS int64
	Bids, Asks                    []CrossVenueLevel
	FeeRate                       decimal.Decimal
}

type CrossVenueRequest struct {
	ID                                       string
	Buy, Sell                                CrossVenueBook
	Units, MinProfit, EstimatedRebalanceCash decimal.Decimal
	NowMS, MaxAgeMS                          int64
}

type CrossVenueEstimate struct {
	BuyNotional, SellNotional, BuyFee, SellFee, NetProfit decimal.Decimal
}

// EstimateCrossVenue uses executable depth and visible timestamps, before any
// mutation. NetProfit includes the declared rebalance allowance, not a fictional
// ledger debit; actual subsequent transfer fees are booked separately.
func EstimateCrossVenue(r CrossVenueRequest) (CrossVenueEstimate, error) {
	var e CrossVenueEstimate
	if !canonicalID(r.ID) || r.NowMS <= 0 || !r.Units.IsPositive() || r.MinProfit.IsNegative() || r.EstimatedRebalanceCash.IsNegative() {
		return e, errors.New("execution: invalid cross-venue request")
	}
	if err := crossVenueBookCheck(r.Buy, r.NowMS, r.MaxAgeMS); err != nil {
		return e, err
	}
	if err := crossVenueBookCheck(r.Sell, r.NowMS, r.MaxAgeMS); err != nil {
		return e, err
	}
	if r.Buy.Venue == r.Sell.Venue || r.Buy.Asset != r.Sell.Asset || r.Buy.Currency != r.Sell.Currency {
		return e, errors.New("execution: cross-venue trade requires distinct same-asset/currency venues")
	}
	var err error
	e.BuyNotional, err = crossVenueNotional(r.Buy.Asks, r.Units)
	if err != nil {
		return e, err
	}
	e.SellNotional, err = crossVenueNotional(r.Sell.Bids, r.Units)
	if err != nil {
		return e, err
	}
	e.BuyFee = e.BuyNotional.Mul(r.Buy.FeeRate)
	e.SellFee = e.SellNotional.Mul(r.Sell.FeeRate)
	e.NetProfit = e.SellNotional.Sub(e.SellFee).Sub(e.BuyNotional).Sub(e.BuyFee).Sub(r.EstimatedRebalanceCash)
	return e, nil
}

type CrossVenueFill struct {
	ID, TradeID, Venue, Side string
	Units, Notional, Fee     decimal.Decimal
	AtMS, BookAtMS           int64
}

type CrossVenueTrade struct {
	Request   CrossVenueRequest
	Status    string // bought (exposed), complete, or unwound
	Buy       CrossVenueFill
	Exit      *CrossVenueFill `json:",omitempty"`
	ExitQuote *CrossVenueBook `json:",omitempty"`
	NetCash   decimal.Decimal
}

// Transfers debit immediately and credit only at explicit completion at or
// after ArrivalMS. Cash and asset fees are charged once at the origin.
type CrossVenueTransfer struct {
	ID, From, To                  string
	Units, Cash, UnitFee, CashFee decimal.Decimal
	AtMS, ArrivalMS               int64
	CompletedMS                   int64
	Completed                     bool
}

type CrossVenueSnapshot struct {
	Balances  []CrossVenueBalance
	Trades    map[string]CrossVenueTrade
	Transfers map[string]CrossVenueTransfer
	Fills     []CrossVenueFill
	LastMS    int64
	Liquidity map[string]CrossVenueLiquidity
}

// Liquidity prevents repeated paper fills from reusing displayed size from one
// immutable observed book. A later observed book explicitly replenishes depth
// and supersedes older books for both sides of the same venue/asset/currency.
// The persisted book journal also provides the latest vintage across reopen;
// transfers never reset it. Publication/reception changes do not create depth.
type CrossVenueLiquidity struct {
	Book         CrossVenueBook
	Bought, Sold decimal.Decimal
}

// OpenCrossVenuePaper creates or reopens a single paper experiment. Reopening
// requires identical genesis balances; it never resets an existing portfolio.
func OpenCrossVenuePaper(path string, initial []CrossVenueBalance) (*CrossVenuePaper, error) {
	if !filepath.IsAbs(path) || len(initial) < 2 || len(initial) > 1000 {
		return nil, errors.New("execution: cross-venue absolute path and 2..1000 venues required")
	}
	initial = append([]CrossVenueBalance(nil), initial...)
	sort.Slice(initial, func(i, j int) bool { return initial[i].Venue < initial[j].Venue })
	for i, b := range initial {
		if !canonicalID(b.Venue) || !canonicalID(b.Asset) || !canonicalID(b.Currency) || b.Asset == b.Currency || b.Cash.IsNegative() || b.Units.IsNegative() || (i > 0 && initial[i-1].Venue == b.Venue) || b.Asset != initial[0].Asset || b.Currency != initial[0].Currency {
			return nil, errors.New("execution: invalid or mixed-currency cross-venue genesis")
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	p := &CrossVenuePaper{db: db}
	fail := func(err error) (*CrossVenuePaper, error) { _ = db.Close(); return nil, err }
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS exec_crossvenue_paper (id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,genesis TEXT NOT NULL,state TEXT NOT NULL)`); err != nil {
		return fail(err)
	}
	genesis, _ := json.Marshal(initial)
	s := CrossVenueSnapshot{Balances: initial, Trades: map[string]CrossVenueTrade{}, Transfers: map[string]CrossVenueTransfer{}, Liquidity: map[string]CrossVenueLiquidity{}}
	raw, _ := json.Marshal(s)
	if _, err = db.Exec(`INSERT OR IGNORE INTO exec_crossvenue_paper VALUES(1,2,?,?)`, string(genesis), string(raw)); err != nil {
		return fail(err)
	}
	var version int
	var stored string
	if err = db.QueryRow(`SELECT version,genesis FROM exec_crossvenue_paper WHERE id=1`).Scan(&version, &stored); err != nil {
		return fail(err)
	}
	if version != 2 || stored != string(genesis) {
		return fail(errors.New("execution: cross-venue genesis/schema mismatch"))
	}
	return p, nil
}

func (p *CrossVenuePaper) Close() error { return p.db.Close() }

func (p *CrossVenuePaper) Snapshot(ctx context.Context) (CrossVenueSnapshot, error) {
	var raw string
	err := p.db.QueryRowContext(ctx, `SELECT state FROM exec_crossvenue_paper WHERE id=1`).Scan(&raw)
	var s CrossVenueSnapshot
	if err == nil {
		err = json.Unmarshal([]byte(raw), &s)
	}
	return s, err
}

func (p *CrossVenuePaper) mutate(ctx context.Context, fn func(*CrossVenueSnapshot) error) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw string
	if err = tx.QueryRowContext(ctx, `SELECT state FROM exec_crossvenue_paper WHERE id=1`).Scan(&raw); err != nil {
		return err
	}
	var s CrossVenueSnapshot
	if err = json.Unmarshal([]byte(raw), &s); err != nil {
		return err
	}
	if err = fn(&s); err != nil {
		return err
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE exec_crossvenue_paper SET state=? WHERE id=1`, string(data)); err != nil {
		return err
	}
	return tx.Commit()
}

func crossVenueClock(s *CrossVenueSnapshot, now int64) error {
	if now <= 0 || now < s.LastMS {
		return errors.New("execution: cross-venue clock moved backwards")
	}
	return nil
}

func crossVenueIdle(s *CrossVenueSnapshot) error {
	for _, t := range s.Trades {
		if t.Status == "bought" {
			return errors.New("execution: recover exposed cross-venue leg before further activity")
		}
	}
	if len(s.Trades)+len(s.Transfers) >= 100000 {
		return errors.New("execution: cross-venue experiment journal bound exceeded")
	}
	return nil
}

func crossVenueBalance(s *CrossVenueSnapshot, venue string) (*CrossVenueBalance, error) {
	for i := range s.Balances {
		if s.Balances[i].Venue == venue {
			return &s.Balances[i], nil
		}
	}
	return nil, errors.New("execution: unknown cross-venue spot account")
}

func crossVenueBookCheck(b CrossVenueBook, now, age int64) error {
	if !canonicalID(b.Venue) || !canonicalID(b.Asset) || !canonicalID(b.Currency) || age < 0 || b.AtMS <= 0 || b.PublishedMS < b.AtMS || b.ReceivedMS < b.PublishedMS || b.ReceivedMS > now || now-b.AtMS > age || b.FeeRate.IsNegative() || !b.FeeRate.LessThan(decimal.NewFromInt(1)) || len(b.Bids) == 0 || len(b.Asks) == 0 || len(b.Bids) > 1024 || len(b.Asks) > 1024 {
		return errors.New("execution: invalid, unavailable or stale cross-venue book")
	}
	for side, levels := range [][]CrossVenueLevel{b.Bids, b.Asks} {
		for i, l := range levels {
			if !l.Price.IsPositive() || !l.Units.IsPositive() || (i > 0 && ((side == 0 && !levels[i-1].Price.GreaterThan(l.Price)) || (side == 1 && !levels[i-1].Price.LessThan(l.Price)))) {
				return errors.New("execution: invalid cross-venue depth ordering")
			}
		}
	}
	if b.Bids[0].Price.GreaterThan(b.Asks[0].Price) {
		return errors.New("execution: internally crossed cross-venue book")
	}
	return nil
}

func crossVenueNotional(levels []CrossVenueLevel, units decimal.Decimal) (decimal.Decimal, error) {
	remaining, cost := units, decimal.Zero
	for _, l := range levels {
		part := decimal.Min(remaining, l.Units)
		cost = cost.Add(part.Mul(l.Price))
		remaining = remaining.Sub(part)
		if remaining.IsZero() {
			return cost, nil
		}
	}
	return decimal.Zero, ErrCrossVenueDepth
}

func crossVenueSame(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func crossVenueLiquidity(s *CrossVenueSnapshot, b CrossVenueBook) (string, CrossVenueLiquidity, error) {
	// Begin and Exit resolve exact durable retries before reaching this check.
	// New mutations must use the latest committed observation, even if an older
	// vintage still has unused depth. A book describes both sides, so observing
	// its asks also supersedes its older bids (including for a recovery unwind).
	for _, liquidity := range s.Liquidity {
		old := liquidity.Book
		if old.Venue == b.Venue && old.Asset == b.Asset && old.Currency == b.Currency && old.AtMS > b.AtMS {
			return "", CrossVenueLiquidity{}, errors.New("execution: superseded cross-venue book vintage")
		}
	}
	raw, _ := json.Marshal([]any{b.Venue, b.AtMS})
	key := string(raw)
	if old, ok := s.Liquidity[key]; ok {
		if !crossVenueSame(old.Book, b) {
			return key, old, errors.New("execution: conflicting cross-venue book identity")
		}
		return key, old, nil
	}
	return key, CrossVenueLiquidity{Book: b}, nil
}

func crossVenueRemaining(levels []CrossVenueLevel, used decimal.Decimal) []CrossVenueLevel {
	remaining := make([]CrossVenueLevel, 0, len(levels))
	for _, l := range levels {
		part := decimal.Min(used, l.Units)
		used = used.Sub(part)
		l.Units = l.Units.Sub(part)
		if l.Units.IsPositive() {
			remaining = append(remaining, l)
		}
	}
	return remaining
}

// Begin verifies both visible books and prepositioned funds, then executes only
// the buy leg. A crash or subsequent sell rejection leaves explicit exposure;
// retry Exit with a fresh sell book, or unwind on the buy venue. Neither path
// promises the initially estimated profit.
func (p *CrossVenuePaper) Begin(ctx context.Context, r CrossVenueRequest) (CrossVenueTrade, error) {
	var out CrossVenueTrade
	err := p.mutate(ctx, func(s *CrossVenueSnapshot) error {
		if t, ok := s.Trades[r.ID]; ok {
			if !crossVenueSame(t.Request, r) {
				return errors.New("execution: conflicting cross-venue trade identity")
			}
			out = t
			return nil
		}
		estimate, err := EstimateCrossVenue(r)
		if err != nil {
			return err
		}
		if err := crossVenueClock(s, r.NowMS); err != nil {
			return err
		}
		if err := crossVenueIdle(s); err != nil {
			return err
		}
		buy, err := crossVenueBalance(s, r.Buy.Venue)
		if err != nil {
			return err
		}
		sell, err := crossVenueBalance(s, r.Sell.Venue)
		if err != nil {
			return err
		}
		if buy.Asset != r.Buy.Asset || buy.Currency != r.Buy.Currency {
			return errors.New("execution: cross-venue account asset mismatch")
		}
		bk, bl, err := crossVenueLiquidity(s, r.Buy)
		if err != nil {
			return err
		}
		sk, sl, err := crossVenueLiquidity(s, r.Sell)
		if err != nil {
			return err
		}
		bc, err := crossVenueNotional(crossVenueRemaining(r.Buy.Asks, bl.Bought), r.Units)
		if err != nil {
			return err
		}
		sc, err := crossVenueNotional(crossVenueRemaining(r.Sell.Bids, sl.Sold), r.Units)
		if err != nil {
			return err
		}
		bf, sf := bc.Mul(r.Buy.FeeRate), sc.Mul(r.Sell.FeeRate)
		estimate.NetProfit = sc.Sub(sf).Sub(bc).Sub(bf).Sub(r.EstimatedRebalanceCash)
		if bc.Add(bf).GreaterThan(buy.Cash) || r.Units.GreaterThan(sell.Units) {
			return errors.New("execution: insufficient prepositioned cross-venue cash/inventory")
		}
		if !estimate.NetProfit.GreaterThan(r.MinProfit) {
			return errors.New("execution: cross-venue costs erase profit threshold")
		}
		buy.Cash = buy.Cash.Sub(bc).Sub(bf)
		buy.Units = buy.Units.Add(r.Units)
		bl.Bought = bl.Bought.Add(r.Units)
		s.Liquidity[bk], s.Liquidity[sk] = bl, sl
		fill := CrossVenueFill{r.ID + "/buy", r.ID, buy.Venue, "buy", r.Units, bc, bf, r.NowMS, r.Buy.AtMS}
		out = CrossVenueTrade{Request: r, Status: "bought", Buy: fill, NetCash: bc.Add(bf).Neg()}
		s.Trades[r.ID] = out
		s.Fills = append(s.Fills, fill)
		s.LastMS = r.NowMS
		return nil
	})
	return out, err
}

// Exit completes the reserved sell leg, or explicitly unwinds bought units.
// Failed attempts commit nothing; the prior bought state remains recoverable.
func (p *CrossVenuePaper) Exit(ctx context.Context, id string, book CrossVenueBook, now int64, unwind bool) (CrossVenueTrade, error) {
	var out CrossVenueTrade
	err := p.mutate(ctx, func(s *CrossVenueSnapshot) error {
		t, ok := s.Trades[id]
		if !ok {
			return errors.New("execution: unknown cross-venue trade")
		}
		want := "complete"
		venue := t.Request.Sell.Venue
		if unwind {
			want = "unwound"
			venue = t.Request.Buy.Venue
		}
		if t.Status != "bought" {
			if t.Status != want || t.Exit == nil || t.Exit.AtMS != now || !crossVenueSame(t.ExitQuote, book) {
				return errors.New("execution: conflicting cross-venue exit identity")
			}
			out = t
			return nil
		}
		if err := crossVenueClock(s, now); err != nil {
			return err
		}
		if err := crossVenueBookCheck(book, now, t.Request.MaxAgeMS); err != nil {
			return err
		}
		if book.Venue != venue || book.Asset != t.Request.Buy.Asset || book.Currency != t.Request.Buy.Currency {
			return errors.New("execution: cross-venue exit account mismatch")
		}
		b, err := crossVenueBalance(s, venue)
		if err != nil {
			return err
		}
		key, liquidity, err := crossVenueLiquidity(s, book)
		if err != nil {
			return err
		}
		notional, err := crossVenueNotional(crossVenueRemaining(book.Bids, liquidity.Sold), t.Request.Units)
		if err != nil {
			return err
		}
		if b.Units.LessThan(t.Request.Units) {
			return errors.New("execution: cross-venue exit inventory unavailable")
		}
		fee := notional.Mul(book.FeeRate)
		b.Units = b.Units.Sub(t.Request.Units)
		b.Cash = b.Cash.Add(notional).Sub(fee)
		liquidity.Sold = liquidity.Sold.Add(t.Request.Units)
		s.Liquidity[key] = liquidity
		fill := CrossVenueFill{id + "/" + want, id, venue, "sell", t.Request.Units, notional, fee, now, book.AtMS}
		t.Status = want
		t.Exit = &fill
		t.ExitQuote = &book
		t.NetCash = t.NetCash.Add(notional).Sub(fee)
		s.Trades[id] = t
		s.Fills = append(s.Fills, fill)
		s.LastMS = now
		out = t
		return nil
	})
	return out, err
}

func (p *CrossVenuePaper) StartTransfer(ctx context.Context, r CrossVenueTransfer) error {
	return p.mutate(ctx, func(s *CrossVenueSnapshot) error {
		if old, ok := s.Transfers[r.ID]; ok {
			old.Completed = false
			old.CompletedMS = 0
			if r.Completed || r.CompletedMS != 0 || !crossVenueSame(old, r) {
				return errors.New("execution: conflicting cross-venue transfer identity")
			}
			return nil
		}
		if !canonicalID(r.ID) || r.From == r.To || r.Completed || r.CompletedMS != 0 || r.Units.IsNegative() || r.Cash.IsNegative() || (!r.Units.IsPositive() && !r.Cash.IsPositive()) || r.UnitFee.IsNegative() || r.CashFee.IsNegative() || r.ArrivalMS <= r.AtMS {
			return errors.New("execution: invalid cross-venue transfer")
		}
		if err := crossVenueClock(s, r.AtMS); err != nil {
			return err
		}
		if err := crossVenueIdle(s); err != nil {
			return err
		}
		from, err := crossVenueBalance(s, r.From)
		if err != nil {
			return err
		}
		if _, err = crossVenueBalance(s, r.To); err != nil {
			return err
		}
		units, cash := r.Units.Add(r.UnitFee), r.Cash.Add(r.CashFee)
		if from.Units.LessThan(units) || from.Cash.LessThan(cash) {
			return errors.New("execution: insufficient cross-venue transfer balances")
		}
		from.Units = from.Units.Sub(units)
		from.Cash = from.Cash.Sub(cash)
		s.Transfers[r.ID] = r
		s.LastMS = r.AtMS
		return nil
	})
}

func (p *CrossVenuePaper) CompleteTransfer(ctx context.Context, id string, now int64) error {
	return p.mutate(ctx, func(s *CrossVenueSnapshot) error {
		t, ok := s.Transfers[id]
		if !ok {
			return errors.New("execution: unknown cross-venue transfer")
		}
		if t.Completed {
			if now < t.CompletedMS {
				return errors.New("execution: cross-venue transfer completion predates recorded receipt")
			}
			return nil
		}
		if err := crossVenueClock(s, now); err != nil {
			return err
		}
		if now < t.ArrivalMS {
			return errors.New("execution: cross-venue transfer not yet available")
		}
		to, err := crossVenueBalance(s, t.To)
		if err != nil {
			return err
		}
		to.Units = to.Units.Add(t.Units)
		to.Cash = to.Cash.Add(t.Cash)
		t.Completed = true
		t.CompletedMS = now
		s.Transfers[id] = t
		s.LastMS = now
		return nil
	})
}

// Equity includes pending transfers, excludes already charged fees, and uses a
// single explicit common-asset mark. It is aggregate economics, not venue NAV.
func (s CrossVenueSnapshot) Equity(mark decimal.Decimal) (decimal.Decimal, error) {
	if mark.IsNegative() {
		return decimal.Zero, fmt.Errorf("execution: negative cross-venue spot mark")
	}
	value := decimal.Zero
	for _, b := range s.Balances {
		value = value.Add(b.Cash).Add(b.Units.Mul(mark))
	}
	for _, t := range s.Transfers {
		if !t.Completed {
			value = value.Add(t.Cash).Add(t.Units.Mul(mark))
		}
	}
	return value, nil
}
