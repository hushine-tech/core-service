// Package reconciliation implements Phase C shadow-compare for Binance-backed
// wallets. It runs out-of-band from the main gRPC request path: a fire-and-
// forget goroutine diffs the strategy-computed local snapshot against the
// exchange-authoritative snapshot, persists the result to reconciliation_runs,
// and emits ELK-visible metric counters. See design.md for rationale.
package reconciliation

import (
	"math"
	"strings"

	"github.com/hushine-tech/core-service/internal/config"
	"github.com/hushine-tech/core-service/internal/domain"
)

// Thresholds wraps config.ReconciliationThresholds with sane accessors.
type Thresholds struct {
	raw config.ReconciliationThresholds
}

// NewThresholds returns a Thresholds view of the config section.
func NewThresholds(cfg config.ReconciliationThresholds) Thresholds {
	return Thresholds{raw: cfg}
}

// CompareResult is the outcome of diffing a local snapshot against an
// exchange snapshot. `FieldDiffs` contains Hard + Soft tier diffs; pass/fail
// is derived from them. `AdvisoryDiffs` is observation-only and does not
// affect pass/fail.
type CompareResult struct {
	FieldDiffs    []domain.FieldDiff
	AdvisoryDiffs []domain.FieldDiff
	HardPass      bool
	SoftPass      bool
}

// Compare runs the Phase C field-tier compare between two canonical wallet
// snapshots. Both MUST be in canonical shape (core-service already
// standardizes Binance responses, and strategy-service sends canonical).
//
// All run types (checkpoint / event / sampled) produce the same compare
// structure — Hard + Soft are always evaluated, Advisory is always recorded.
func Compare(local, exchange domain.OnlineAccountInfo, t Thresholds) CompareResult {
	res := CompareResult{
		FieldDiffs:    []domain.FieldDiff{},
		AdvisoryDiffs: []domain.FieldDiff{},
		HardPass:      true,
		SoftPass:      true,
	}

	// ── Account-level Soft fields ────────────────────────────────────────
	res.addSoft("futures.wallet_balance",
		exchange.Futures.WalletBalance, local.Futures.WalletBalance,
		t.raw.WalletBalanceAbsToleranceUSDT, t.raw.WalletBalanceRatioTolerance)

	res.addSoft("futures.available_balance",
		exchange.Futures.AvailableBalance, local.Futures.AvailableBalance,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	res.addSoft("futures.margin_balance",
		exchange.Futures.MarginBalance, local.Futures.MarginBalance,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	res.addSoft("futures.total_margin_balance",
		exchange.Futures.TotalMarginBalance, local.Futures.TotalMarginBalance,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	res.addSoft("futures.unrealized_pnl",
		exchange.Futures.UnrealizedPnl, local.Futures.UnrealizedPnl,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	res.addSoft("futures.total_unrealized_pnl",
		exchange.Futures.TotalUnrealizedPnl, local.Futures.TotalUnrealizedPnl,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	res.addSoft("futures.total_position_initial_margin",
		exchange.Futures.TotalPositionInitialMargin, local.Futures.TotalPositionInitialMargin,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	res.addSoft("futures.total_open_order_initial_margin",
		exchange.Futures.TotalOpenOrderInitialMargin, local.Futures.TotalOpenOrderInitialMargin,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	res.addSoft("futures.total_maint_margin",
		exchange.Futures.TotalMaintMargin, local.Futures.TotalMaintMargin,
		t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

	// ── Position-level ─────────────────────────────────────────────────
	// Align positions by symbol+side; a missing position on one side is
	// treated as a structural diff (Hard fail for symbol/side presence,
	// then continue checking numeric fields for present-on-both cases).
	exPos := indexPositions(exchange.Futures.Positions)
	loPos := indexPositions(local.Futures.Positions)
	metadataBySymbol := indexRiskMetadata(exchange.Futures.RiskMetadata, local.Futures.RiskMetadata)

	// Every key that appears on either side
	keys := make(map[positionKey]struct{})
	for k := range exPos {
		keys[k] = struct{}{}
	}
	for k := range loPos {
		keys[k] = struct{}{}
	}

	for k := range keys {
		prefix := "futures.positions[" + k.label() + "]"
		ep := exPos[k] // may be nil if side didn't include this key at all
		lp := loPos[k]

		// Treat "absent from map" and "qty=0 in map" as the same state
		// for diff purposes. Closed positions transiently manifest either
		// way (one side drops the row from its dict, the other keeps it at
		// qty=0 until the next propagation), and Binance specifically keeps
		// stale qty=0 rows in /fapi/v3/positionRisk for closed positions.
		exFlat := ep == nil || positionQty(ep) == 0
		loFlat := lp == nil || positionQty(lp) == 0

		if exFlat && loFlat {
			// Both sides effectively have no position here — nothing to diff.
			continue
		}

		// Genuine structural mismatch: one side doesn't track the position
		// at all (not even as qty=0), while the other side has a live
		// position. This is a real presence drift, not a post-close
		// transient.
		if ep == nil || lp == nil {
			exchangeQty := 0.0
			localQty := 0.0
			if ep != nil {
				exchangeQty = positionQty(ep)
			}
			if lp != nil {
				localQty = positionQty(lp)
			}
			res.FieldDiffs = append(res.FieldDiffs, domain.FieldDiff{
				Field:     prefix + ".exists",
				Severity:  domain.FieldDiffHard,
				Exchange:  boolToFloat(ep != nil),
				Local:     boolToFloat(lp != nil),
				DiffAbs:   math.Abs(exchangeQty - localQty),
				DiffRatio: 0,
				Threshold: map[string]any{"rule": "exact_match_required"},
				Passed:    false,
			})
			res.HardPass = false
			continue
		}

		// Both sides have the position (one side may be at qty=0 mid-close).
		// Field-level diffs handle the qty=0 vs residual transient via the
		// position_qty step tolerance instead of misclassifying it as a
		// structural "missing" fail.
		md := metadataBySymbol[strings.ToUpper(ep.Symbol)]

		// Hard: position_qty (stepSize-based)
		stepSize := md.StepSize
		res.addHardStepTolerance(prefix+".position_qty",
			positionQty(ep), positionQty(lp), stepSize, t.raw.PositionQtyStepTolerance)

		// Hard: entry_price (tickSize + ratio)
		tickSize := md.TickSize
		res.addHardPriceTolerance(prefix+".entry_price",
			ep.EntryPrice, lp.EntryPrice, tickSize,
			t.raw.EntryPriceTickTolerance, t.raw.EntryPriceRatioTolerance)

		// Soft: risk-derived per-position fields
		res.addSoft(prefix+".unrealized_pnl",
			ep.UnrealizedPnl, lp.UnrealizedPnl,
			t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

		res.addSoft(prefix+".initial_margin",
			ep.InitialMargin, lp.InitialMargin,
			t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

		res.addSoft(prefix+".position_initial_margin",
			ep.PositionInitialMargin, lp.PositionInitialMargin,
			t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

		res.addSoft(prefix+".open_order_initial_margin",
			ep.OpenOrderInitialMargin, lp.OpenOrderInitialMargin,
			t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

		res.addSoft(prefix+".maint_margin",
			ep.MaintMargin, lp.MaintMargin,
			t.raw.DerivedRiskAbsToleranceUSDT, t.raw.DerivedRiskRatioTolerance)

		res.addSoft(prefix+".liquidation_price",
			ep.LiquidationPrice, lp.LiquidationPrice,
			t.raw.LiquidationPriceAbsToleranceUSDT, t.raw.LiquidationPriceRatioTolerance)

		// Advisory: mark_price drift, break_even, isolated_wallet, notional
		res.addAdvisory(prefix+".mark_price",
			ep.MarkPrice, lp.MarkPrice,
			map[string]any{"rule": "drift_only", "tick_warn_multiple": t.raw.MarkPriceDriftTickWarn, "tick_size": tickSize})

		res.addAdvisory(prefix+".break_even_price",
			ep.BreakEvenPrice, lp.BreakEvenPrice,
			map[string]any{"rule": "observation_only"})

		res.addAdvisory(prefix+".isolated_wallet",
			ep.IsolatedWallet, lp.IsolatedWallet,
			map[string]any{"rule": "observation_only"})

		res.addAdvisory(prefix+".notional",
			ep.Notional, lp.Notional,
			map[string]any{"rule": "observation_only"})
	}

	return res
}

// ── field tier helpers ────────────────────────────────────────────────────

// addSoft appends a soft-severity diff using max(abs, ratio) tolerance.
//
// Ratio denominator is max(|exchange|, |local|), NOT just |exchange|. The
// symmetric form keeps the threshold behavior identical when either side is
// the "authoritative" reference — exchange is still treated as the intended
// source of truth for pass/fail semantics because `exchange - local` defines
// sign and magnitude — but edge cases like "fresh account: exchange=0, local
// just bootstrapped to X" collapse to abs-only under an asymmetric denom and
// get unexpectedly strict calibration. The symmetric denom gives the same
// result regardless of which side happens to be zero.
//
// When BOTH sides are zero (denom=0), ratio threshold collapses to 0 and
// absTol is the only gate — correct, because there's no scale to base
// relative tolerance on.
func (r *CompareResult) addSoft(field string, exchange, local, absTol, ratioTol float64) {
	diffAbs := math.Abs(exchange - local)
	denom := math.Max(math.Abs(exchange), math.Abs(local))
	diffRatio := 0.0
	if denom > 0 {
		diffRatio = diffAbs / denom
	}

	absThreshold := absTol
	ratioThreshold := denom * ratioTol
	effectiveThreshold := math.Max(absThreshold, ratioThreshold)

	passed := diffAbs <= effectiveThreshold
	if !passed {
		r.SoftPass = false
	}
	r.FieldDiffs = append(r.FieldDiffs, domain.FieldDiff{
		Field:     field,
		Severity:  domain.FieldDiffSoft,
		Exchange:  exchange,
		Local:     local,
		DiffAbs:   diffAbs,
		DiffRatio: diffRatio,
		Threshold: map[string]any{"abs": absTol, "ratio": ratioTol, "effective": effectiveThreshold},
		Passed:    passed,
	})
}

// addHardStepTolerance appends a hard-severity diff using stepSize multiples.
// When stepSize is unknown (0), falls back to exact equality.
func (r *CompareResult) addHardStepTolerance(field string, exchange, local, stepSize, multiplier float64) {
	diffAbs := math.Abs(exchange - local)
	var threshold float64
	var ruleDesc map[string]any
	if stepSize > 0 {
		threshold = stepSize * multiplier
		ruleDesc = map[string]any{"step_size": stepSize, "tolerance_multiple": multiplier, "effective": threshold}
	} else {
		threshold = 0
		ruleDesc = map[string]any{"step_size": 0, "rule": "exact_match_no_stepsize_known"}
	}
	passed := diffAbs <= threshold
	if !passed {
		r.HardPass = false
	}
	denom := math.Max(math.Abs(exchange), math.Abs(local))
	ratio := 0.0
	if denom > 0 {
		ratio = diffAbs / denom
	}
	r.FieldDiffs = append(r.FieldDiffs, domain.FieldDiff{
		Field:     field,
		Severity:  domain.FieldDiffHard,
		Exchange:  exchange,
		Local:     local,
		DiffAbs:   diffAbs,
		DiffRatio: ratio,
		Threshold: ruleDesc,
		Passed:    passed,
	})
}

// addHardPriceTolerance combines tick-size multiplier and ratio tolerance —
// whichever is larger (looser) wins. When tickSize is unknown, falls back to
// ratio-only. Ratio denominator is symmetrized max(|exchange|, |local|) for
// the same reason as addSoft: avoids strict-mode surprises when one side is
// briefly zero.
func (r *CompareResult) addHardPriceTolerance(field string, exchange, local, tickSize, tickMultiplier, ratioTol float64) {
	diffAbs := math.Abs(exchange - local)
	tickThreshold := 0.0
	if tickSize > 0 {
		tickThreshold = tickSize * tickMultiplier
	}
	denom := math.Max(math.Abs(exchange), math.Abs(local))
	ratioThreshold := denom * ratioTol
	threshold := math.Max(tickThreshold, ratioThreshold)
	passed := diffAbs <= threshold
	if !passed {
		r.HardPass = false
	}
	ratio := 0.0
	if denom > 0 {
		ratio = diffAbs / denom
	}
	r.FieldDiffs = append(r.FieldDiffs, domain.FieldDiff{
		Field:     field,
		Severity:  domain.FieldDiffHard,
		Exchange:  exchange,
		Local:     local,
		DiffAbs:   diffAbs,
		DiffRatio: ratio,
		Threshold: map[string]any{"tick_size": tickSize, "tick_multiple": tickMultiplier, "ratio": ratioTol, "effective": threshold},
		Passed:    passed,
	})
}

// addAdvisory records a field observation without affecting pass/fail.
// DiffRatio uses symmetrized max(|exchange|, |local|) to match the hard/soft
// tier convention; observation-only so the choice affects display, not gating.
func (r *CompareResult) addAdvisory(field string, exchange, local float64, rule map[string]any) {
	diffAbs := math.Abs(exchange - local)
	denom := math.Max(math.Abs(exchange), math.Abs(local))
	ratio := 0.0
	if denom > 0 {
		ratio = diffAbs / denom
	}
	r.AdvisoryDiffs = append(r.AdvisoryDiffs, domain.FieldDiff{
		Field:     field,
		Severity:  domain.FieldDiffAdvisory,
		Exchange:  exchange,
		Local:     local,
		DiffAbs:   diffAbs,
		DiffRatio: ratio,
		Threshold: rule,
		Passed:    true, // advisory is never a fail
	})
}

// ── position indexing ─────────────────────────────────────────────────────

// positionKey uniquely identifies a position across local/exchange snapshots.
// For one-way positions Side will be BOTH or empty; for hedge positions it's
// LONG or SHORT.
type positionKey struct {
	Symbol string
	Side   string
}

func (k positionKey) label() string {
	if k.Side == "" || strings.EqualFold(k.Side, "BOTH") {
		return k.Symbol
	}
	return k.Symbol + "/" + strings.ToUpper(k.Side)
}

// normalizePositionSide collapses the two synonymous forms of "one-way mode"
// — empty string and "BOTH" — to a single canonical value. Both sides must
// agree on the key or we'd spuriously report the same one-way position as
// two distinct rows and raise a structural diff on both.
func normalizePositionSide(side string) string {
	s := strings.ToUpper(strings.TrimSpace(side))
	if s == "" {
		return "BOTH"
	}
	return s
}

// indexPositions maps (symbol, side) -> *FuturesPosition without filtering
// qty=0 rows. Flat-vs-absent equivalence is handled symmetrically in Compare
// so that a post-close transient (one side qty=0, the other side still
// propagating a residual) surfaces as a position_qty field diff — not as a
// structural "missing" hard fail.
func indexPositions(positions []domain.FuturesPosition) map[positionKey]*domain.FuturesPosition {
	out := make(map[positionKey]*domain.FuturesPosition, len(positions))
	for i := range positions {
		p := &positions[i]
		key := positionKey{
			Symbol: strings.ToUpper(p.Symbol),
			Side:   normalizePositionSide(p.PositionSide),
		}
		out[key] = p
	}
	return out
}

// indexRiskMetadata prefers exchange metadata, falls back to local.
// Keyed by uppercase symbol.
func indexRiskMetadata(exchangeMeta, localMeta []domain.FuturesRiskMetadata) map[string]domain.FuturesRiskMetadata {
	out := make(map[string]domain.FuturesRiskMetadata)
	for _, m := range localMeta {
		out[strings.ToUpper(m.Symbol)] = m
	}
	// exchange wins when both are present (authoritative)
	for _, m := range exchangeMeta {
		out[strings.ToUpper(m.Symbol)] = m
	}
	return out
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func positionQty(p *domain.FuturesPosition) float64 {
	if p == nil {
		return 0.0
	}
	if p.PositionQty != 0.0 {
		return p.PositionQty
	}
	return p.Qty
}
