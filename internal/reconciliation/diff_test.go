package reconciliation

import (
	"testing"

	"github.com/hushine-tech/core-service/internal/config"
	"github.com/hushine-tech/core-service/internal/domain"
)

// testThresholds returns the Phase C calibration defaults used in all tests.
func testThresholds() Thresholds {
	return NewThresholds(config.DefaultReconciliationConfig().Thresholds)
}

func withPosition(symbol, side string, qty, entry, mark, upnl, im, mm, liq float64) domain.FuturesPosition {
	return domain.FuturesPosition{
		Symbol:                symbol,
		PositionSide:          side,
		Qty:                   qty,
		EntryPrice:            entry,
		MarkPrice:             mark,
		UnrealizedPnl:         upnl,
		InitialMargin:         im,
		PositionInitialMargin: im,
		MaintMargin:           mm,
		LiquidationPrice:      liq,
	}
}

func minimalRiskMetadata(symbol string, tick, step float64) domain.FuturesRiskMetadata {
	return domain.FuturesRiskMetadata{
		Symbol:   symbol,
		TickSize: tick,
		StepSize: step,
	}
}

func TestCompare_AllPass_IdenticalSnapshots(t *testing.T) {
	wallet := domain.FuturesWallet{
		WalletBalance:      10_000.0,
		AvailableBalance:   9_500.0,
		MarginBalance:      10_050.0,
		TotalMarginBalance: 10_050.0,
		UnrealizedPnl:      50.0,
		TotalUnrealizedPnl: 50.0,
		Positions: []domain.FuturesPosition{
			withPosition("BTCUSDT", "BOTH", 0.1, 45000.0, 45500.0, 50.0, 500.0, 100.0, 42000.0),
		},
		RiskMetadata: []domain.FuturesRiskMetadata{
			minimalRiskMetadata("BTCUSDT", 0.1, 0.001),
		},
	}
	snap := domain.OnlineAccountInfo{
		Mode:    domain.AccountModeBinanceTestnet,
		Futures: wallet,
	}

	res := Compare(snap, snap, testThresholds())

	if !res.HardPass {
		t.Errorf("expected HardPass=true for identical snapshots, got false; diffs=%v", res.FieldDiffs)
	}
	if !res.SoftPass {
		t.Errorf("expected SoftPass=true for identical snapshots")
	}
	// No field should be flagged as failed.
	for _, d := range res.FieldDiffs {
		if !d.Passed {
			t.Errorf("field %s flagged as failed on identical snapshots: %+v", d.Field, d)
		}
	}
	// Advisory diffs should still be recorded (even with 0 delta).
	if len(res.AdvisoryDiffs) == 0 {
		t.Error("expected advisory diffs to be recorded even when values match")
	}
}

func TestCompare_SoftFail_WalletBalanceDrift(t *testing.T) {
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			WalletBalance: 10_000.0,
		},
	}
	// Effective threshold on $10k is max(0.01 USDT, 10000 * 0.0002) = 2.0 USDT.
	// Use $10 drift to clearly exceed it.
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			WalletBalance: 10_010.0,
		},
	}
	res := Compare(local, exchange, testThresholds())

	if res.HardPass == false {
		// hard should still pass since it's only soft-tier fields here
		t.Error("hard pass should be true when only soft fields differ")
	}
	if res.SoftPass {
		t.Error("expected SoftPass=false for 1 USDT wallet_balance drift (well over threshold)")
	}
	found := false
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.wallet_balance" {
			found = true
			if d.Passed {
				t.Error("wallet_balance diff should be marked as failed")
			}
			if d.Severity != domain.FieldDiffSoft {
				t.Errorf("wallet_balance severity: got %s, want soft", d.Severity)
			}
		}
	}
	if !found {
		t.Error("expected wallet_balance in field_diffs")
	}
}

func TestCompare_HardFail_PositionMissingOnLocal(t *testing.T) {
	// Exchange reports a position, local doesn't know about it.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.1, 45000, 45000, 0, 450, 90, 40000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode:    domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{}, // no positions
	}
	res := Compare(local, exchange, testThresholds())

	if res.HardPass {
		t.Error("expected HardPass=false when local is missing a position")
	}
	// Expect the exists-diff record.
	foundExists := false
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].exists" {
			foundExists = true
			if d.Severity != domain.FieldDiffHard {
				t.Errorf("exists diff severity: got %s, want hard", d.Severity)
			}
			if d.Passed {
				t.Error("exists diff should be failed")
			}
		}
	}
	if !foundExists {
		t.Error("expected futures.positions[BTCUSDT].exists in field_diffs")
	}
}

func TestCompare_HardFail_EntryPriceBeyondTickTolerance(t *testing.T) {
	// tickSize=0.1, tolerance=1.0 → threshold is 0.1; plus ratio=0.0002.
	// On $45000 the ratio gives ~$9 which actually dominates. So we need
	// a diff that breaks BOTH (>$9 AND >0.1 tick). Use $50 diff — a
	// substantial drift.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.1, 45000.0, 45000.0, 0, 450, 90, 40000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.1, 45050.0, 45000.0, 0, 450, 90, 40000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	res := Compare(local, exchange, testThresholds())

	if res.HardPass {
		t.Error("expected HardPass=false for entry_price drift of $50 on $45000")
	}
	found := false
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].entry_price" {
			found = true
			if d.Passed {
				t.Error("entry_price diff should be failed")
			}
		}
	}
	if !found {
		t.Error("expected futures.positions[BTCUSDT].entry_price in field_diffs")
	}
}

func TestCompare_HardPass_EntryPriceWithinTickTolerance(t *testing.T) {
	// $0.1 drift = exactly 1 tick = at the tick tolerance boundary (≤ threshold → pass).
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.1, 45000.0, 45000.0, 0, 450, 90, 40000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.1, 45000.1, 45000.0, 0, 450, 90, 40000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	res := Compare(local, exchange, testThresholds())

	if !res.HardPass {
		t.Error("expected HardPass=true for a 1-tick entry_price diff (within tolerance)")
	}
}

func TestCompare_AdvisoryRecordedButNotGated(t *testing.T) {
	// Mark price drift by $100 — way above any reasonable advisory threshold,
	// and break-even drift by much more. Both are advisory, so they MUST NOT
	// affect hard_pass or soft_pass.
	exchangePos := withPosition("BTCUSDT", "BOTH", 0.1, 45000.0, 45000.0, 0, 450, 90, 40000)
	exchangePos.BreakEvenPrice = 45001.0
	localPos := withPosition("BTCUSDT", "BOTH", 0.1, 45000.0, 44900.0, 0, 450, 90, 40000)
	localPos.BreakEvenPrice = 99999.0

	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				exchangePos,
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				localPos,
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	res := Compare(local, exchange, testThresholds())

	if !res.HardPass {
		t.Error("mark_price is advisory — must NOT affect HardPass")
	}
	if !res.SoftPass {
		t.Error("mark_price is advisory — must NOT affect SoftPass")
	}
	foundMark := false
	foundBreakEven := false
	for _, d := range res.AdvisoryDiffs {
		if d.Field == "futures.positions[BTCUSDT].mark_price" {
			foundMark = true
			if d.Severity != domain.FieldDiffAdvisory {
				t.Errorf("mark_price severity: got %s, want advisory", d.Severity)
			}
		}
		if d.Field == "futures.positions[BTCUSDT].break_even_price" {
			foundBreakEven = true
			if d.Severity != domain.FieldDiffAdvisory {
				t.Errorf("break_even_price severity: got %s, want advisory", d.Severity)
			}
		}
	}
	if !foundMark {
		t.Error("expected mark_price in advisory_diffs")
	}
	if !foundBreakEven {
		t.Error("expected break_even_price in advisory_diffs")
	}
}

func TestCompare_PositionQtyUsesCanonicalField(t *testing.T) {
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				{
					Symbol:       "BTCUSDT",
					PositionSide: "BOTH",
					Qty:          0.2, // legacy alias intentionally wrong
					PositionQty:  0.1, // canonical value should win
					EntryPrice:   45000.0,
					MarkPrice:    45000.0,
				},
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				{
					Symbol:       "BTCUSDT",
					PositionSide: "BOTH",
					Qty:          0.3, // different legacy alias should be ignored
					PositionQty:  0.1,
					EntryPrice:   45000.0,
					MarkPrice:    45000.0,
				},
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}

	res := Compare(local, exchange, testThresholds())

	if !res.HardPass {
		t.Fatalf("expected HardPass=true when canonical position_qty matches; diffs=%+v", res.FieldDiffs)
	}
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].position_qty" && !d.Passed {
			t.Fatalf("position_qty diff should have passed when PositionQty matches: %+v", d)
		}
	}
}

func TestCompare_SoftTier_ZeroExchangeFallsBackToAbsTolerance(t *testing.T) {
	// exchange=0, local=0.5 → denom=max(0, 0.5)=0.5, ratio threshold =
	// 0.5 * 0.0002 = 0.0001, abs threshold = 0.01 → effective threshold
	// is 0.01 (abs wins at these scales). A 0.5 drift MUST soft-fail.
	// Symmetric denominator means flipping exchange/local still fails.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			WalletBalance: 0, // fresh account or pre-deposit state
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			WalletBalance: 0.5, // above the 0.01 abs threshold
		},
	}
	res := Compare(local, exchange, testThresholds())

	if res.SoftPass {
		t.Error("expected SoftPass=false: 0.5 drift vs exchange=0 must exceed abs tolerance 0.01")
	}

	// Symmetric case: small drift (< abs) below threshold — must pass.
	local.Futures.WalletBalance = 0.005
	res2 := Compare(local, exchange, testThresholds())
	if !res2.SoftPass {
		t.Error("expected SoftPass=true: 0.005 drift is within 0.01 abs tolerance when exchange=0")
	}
}

func TestCompare_SoftTier_SymmetricDenominator(t *testing.T) {
	// Swapping exchange <-> local must produce the same pass/fail verdict.
	// Before L1 fix the ratio denominator was |exchange|, so
	//   exchange=0,   local=X   → ratio threshold = 0 (abs-only)
	//   exchange=X,   local=0   → ratio threshold = X * ratioTol
	// and at large X they could land on opposite sides of the threshold.
	// After the fix denom=max(|ex|,|lo|) collapses both to the same result.
	th := testThresholds()

	// Scenario: wallet_balance 10000 vs 0. Symmetric behavior expected.
	a := domain.OnlineAccountInfo{Futures: domain.FuturesWallet{WalletBalance: 10_000.0}}
	b := domain.OnlineAccountInfo{Futures: domain.FuturesWallet{WalletBalance: 0.0}}

	resAB := Compare(a, b, th)
	resBA := Compare(b, a, th)

	if resAB.SoftPass != resBA.SoftPass {
		t.Errorf("soft-tier should be symmetric: compare(a,b).SoftPass=%v vs compare(b,a).SoftPass=%v",
			resAB.SoftPass, resBA.SoftPass)
	}
	if resAB.SoftPass {
		t.Error("10_000 vs 0 must soft-fail in either direction")
	}

	// Scenario: well within 0.02% (2 bps) ratio tolerance at large scale.
	// exchange=100_000, local=100_001.5 → diff=1.5, ratio threshold =
	// 100_001.5 * 0.0002 = ~20 → should pass in both directions.
	aa := domain.OnlineAccountInfo{Futures: domain.FuturesWallet{WalletBalance: 100_000.0}}
	bb := domain.OnlineAccountInfo{Futures: domain.FuturesWallet{WalletBalance: 100_001.5}}
	resAABB := Compare(aa, bb, th)
	resBBAA := Compare(bb, aa, th)
	if !resAABB.SoftPass || !resBBAA.SoftPass {
		t.Errorf("1.5 drift at 100k scale should pass in either direction: ab=%v ba=%v",
			resAABB.SoftPass, resBBAA.SoftPass)
	}
}

func TestCompare_OneWayPositions_EmptyAndBothSideAliased(t *testing.T) {
	// One-way mode has two legitimate wire representations for a position
	// side: "" (older / absent) and "BOTH". Both MUST hash to the same
	// positionKey — otherwise the same one-way position would appear as
	// two distinct keyed rows and Compare would raise spurious structural
	// diffs on the union.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "", 0.1, 45_000, 45_000, 0, 450, 90, 40_000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.1, 45_000, 45_000, 0, 450, 90, 40_000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	res := Compare(local, exchange, testThresholds())
	if !res.HardPass || !res.SoftPass {
		t.Errorf("'' and 'BOTH' for one-way side must match as the same key; got hard=%v soft=%v diffs=%+v",
			res.HardPass, res.SoftPass, res.FieldDiffs)
	}
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].exists" {
			t.Errorf("'' vs 'BOTH' must not surface as .exists diff: %+v", d)
		}
	}
}

func TestCompare_OneWayShortBothDoesNotProduceShortExistsDiff(t *testing.T) {
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("ETHUSDT", "BOTH", -0.021, 2328.08, 2327.57, 0.01, 2.44, 0.19, 2100),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("ETHUSDT", 0.01, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("ETHUSDT", "BOTH", -0.021, 2328.08, 2327.57, 0.01, 2.44, 0.19, 2100),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("ETHUSDT", 0.01, 0.001)},
		},
	}

	res := Compare(local, exchange, testThresholds())

	if !res.HardPass || !res.SoftPass {
		t.Fatalf("matching one-way short BOTH snapshots should pass: hard=%v soft=%v diffs=%+v",
			res.HardPass, res.SoftPass, res.FieldDiffs)
	}
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[ETHUSDT/SHORT].exists" {
			t.Fatalf("one-way short must not be indexed as ETHUSDT/SHORT: %+v", d)
		}
		if d.Field == "futures.positions[ETHUSDT].exists" {
			t.Fatalf("matching BOTH position must not surface .exists diff: %+v", d)
		}
	}
}

func TestCompare_LeverageAwarePositionInitialMarginUsesConfiguredLeverage(t *testing.T) {
	notional := 0.021 * 2328.08476
	im20x := notional / 20.0
	metadata := minimalRiskMetadata("ETHUSDT", 0.01, 0.001)
	metadata.ConfiguredLeverage = 20.0
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("ETHUSDT", "BOTH", -0.021, 2328.08476, 2328.08476, 0, im20x, notional*0.004, 2100),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{metadata},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("ETHUSDT", "BOTH", -0.021, 2328.08476, 2328.08476, 0, im20x, notional*0.004, 2100),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{metadata},
		},
	}

	res := Compare(local, exchange, testThresholds())
	if !res.SoftPass {
		t.Fatalf("20x position_initial_margin should pass: diffs=%+v", res.FieldDiffs)
	}

	local.Futures.Positions[0].InitialMargin = notional
	local.Futures.Positions[0].PositionInitialMargin = notional
	bad := Compare(local, exchange, testThresholds())
	found := false
	for _, d := range bad.FieldDiffs {
		if d.Field == "futures.positions[ETHUSDT].position_initial_margin" {
			found = true
			if d.Passed {
				t.Fatalf("full-notional IM must fail against 20x exchange IM: %+v", d)
			}
		}
	}
	if !found {
		t.Fatal("expected position_initial_margin diff")
	}
}

func TestCompare_HedgeModePositionsKeyedBySymbolAndSide(t *testing.T) {
	// Same symbol, different sides — MUST be treated as two distinct positions.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "LONG", 0.1, 45000, 45000, 0, 450, 90, 40000),
				withPosition("BTCUSDT", "SHORT", 0.05, 46000, 45000, 50, 230, 50, 50000),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	// Local matches exactly.
	res := Compare(exchange, exchange, testThresholds())

	if !res.HardPass || !res.SoftPass {
		t.Errorf("hedge mode positions should pass when matching: hard=%v soft=%v", res.HardPass, res.SoftPass)
	}
	// Both positions should appear in advisory diffs (e.g. mark_price entries)
	// — one per side.
	longSeen, shortSeen := false, false
	for _, d := range res.AdvisoryDiffs {
		if d.Field == "futures.positions[BTCUSDT/LONG].mark_price" {
			longSeen = true
		}
		if d.Field == "futures.positions[BTCUSDT/SHORT].mark_price" {
			shortSeen = true
		}
	}
	if !longSeen || !shortSeen {
		t.Errorf("hedge sides should produce separate keyed diffs: long=%v short=%v", longSeen, shortSeen)
	}
}

func TestCompare_PostCloseTransient_SurfacesAsQtyDiffNotStructural(t *testing.T) {
	// Regression: one side just closed (qty=0, still in its position map),
	// the other side still reports a residual qty that hasn't propagated yet.
	// BEFORE the fix indexPositions dropped the qty=0 row, collapsing the
	// union into "present on one side only" → Hard-Fail on .exists. AFTER
	// the fix both sides stay indexed; the diff surfaces as a position_qty
	// field-tier comparison (and passes when the residual is within
	// stepSize * PositionQtyStepTolerance).

	// stepSize = 0.001, tolerance multiplier = default 0.5 (config).
	// So a 0.0001 residual is well within step tolerance → should pass.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				// Still reporting a tiny residual — propagation lag.
				withPosition("BTCUSDT", "BOTH", 0.0001, 45000.0, 45000.0, 0, 0, 0, 40000.0),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			// Already closed (qty=0) but still carried in the position map.
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.0, 45000.0, 45000.0, 0, 0, 0, 0),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	res := Compare(local, exchange, testThresholds())

	// Must NOT be classified as a structural fail.
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].exists" {
			t.Fatalf("post-close transient must surface as position_qty diff, not structural .exists: %+v", d)
		}
	}

	// Should show up as a position_qty diff in Hard tier (and pass with
	// residual within step tolerance).
	foundQty := false
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].position_qty" {
			foundQty = true
			if d.Severity != domain.FieldDiffHard {
				t.Errorf("position_qty severity: got %s, want hard", d.Severity)
			}
			if !d.Passed {
				t.Errorf("residual within step tolerance should pass: %+v", d)
			}
		}
	}
	if !foundQty {
		t.Error("expected futures.positions[BTCUSDT].position_qty in field_diffs")
	}
	if !res.HardPass {
		t.Errorf("HardPass should be true for small residual within step tolerance; diffs=%+v", res.FieldDiffs)
	}
}

func TestCompare_PostCloseTransient_LargeResidualStillQtyDiff(t *testing.T) {
	// Same setup as the transient test, but the "residual" is large enough
	// to blow through the step tolerance. It MUST still be a position_qty
	// hard fail — NOT a structural .exists fail.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.05, 45000.0, 45000.0, 0, 0, 0, 40000.0),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("BTCUSDT", "BOTH", 0.0, 45000.0, 45000.0, 0, 0, 0, 0),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	res := Compare(local, exchange, testThresholds())

	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].exists" {
			t.Fatalf("large-residual close transient must NOT become structural .exists: %+v", d)
		}
	}
	foundQty := false
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].position_qty" {
			foundQty = true
			if d.Passed {
				t.Errorf("0.05 residual well over step tolerance should fail: %+v", d)
			}
		}
	}
	if !foundQty {
		t.Error("expected position_qty in field_diffs")
	}
	if res.HardPass {
		t.Error("expected HardPass=false for 0.05 qty drift (well over step tolerance)")
	}
}

func TestCompare_ExchangeStaleZeroRow_LocalAbsent_NoDiff(t *testing.T) {
	// Preserve prior behavior: Binance commonly keeps qty=0 "stale" rows
	// in /fapi/v3/positionRisk for positions that have been fully closed.
	// If local has already dropped the key from its dict, both sides are
	// effectively flat and we must produce NO diff (no structural, no qty).
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				// Stale qty=0 row — closed on exchange, still reported.
				withPosition("BTCUSDT", "BOTH", 0.0, 45000.0, 45000.0, 0, 0, 0, 0),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("BTCUSDT", 0.1, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{}, // cleaned up
		},
	}
	res := Compare(local, exchange, testThresholds())

	if !res.HardPass || !res.SoftPass {
		t.Errorf("stale qty=0 row on exchange + absent on local must be silent; got hard=%v soft=%v diffs=%+v",
			res.HardPass, res.SoftPass, res.FieldDiffs)
	}
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[BTCUSDT].exists" {
			t.Errorf("stale qty=0 vs absent must not surface as .exists diff: %+v", d)
		}
		if d.Field == "futures.positions[BTCUSDT].position_qty" {
			t.Errorf("stale qty=0 vs absent must not surface as position_qty diff: %+v", d)
		}
	}
}

func TestCompare_GenuineStructuralMissing_StillHardFails(t *testing.T) {
	// Sanity: ensure the fix didn't neutralize the real structural drift
	// case. Exchange has a live position, local has no trace of the symbol.
	// This is genuinely "absent on local" and must still be Hard-Fail .exists.
	exchange := domain.OnlineAccountInfo{
		Mode: domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{
			Positions: []domain.FuturesPosition{
				withPosition("ETHUSDT", "BOTH", 1.5, 2500.0, 2500.0, 0, 375, 75, 2200.0),
			},
			RiskMetadata: []domain.FuturesRiskMetadata{minimalRiskMetadata("ETHUSDT", 0.01, 0.001)},
		},
	}
	local := domain.OnlineAccountInfo{
		Mode:    domain.AccountModeBinanceTestnet,
		Futures: domain.FuturesWallet{}, // totally absent
	}
	res := Compare(local, exchange, testThresholds())

	if res.HardPass {
		t.Error("genuine one-side-only live position must still Hard-Fail")
	}
	found := false
	for _, d := range res.FieldDiffs {
		if d.Field == "futures.positions[ETHUSDT].exists" {
			found = true
			if d.Passed {
				t.Error(".exists diff for genuine missing must be failed")
			}
		}
	}
	if !found {
		t.Error("expected futures.positions[ETHUSDT].exists in field_diffs")
	}
}

func TestRunTypeFromReason_Classification(t *testing.T) {
	// Reason → expected RunType table.
	cases := []struct {
		name   string
		reason domain.SnapshotReason
		want   domain.ReconciliationRunType
	}{
		{"OrderFill → event", domain.SnapshotReasonOrderFill, domain.ReconciliationRunEvent},
		{"StrategyStart → checkpoint", domain.SnapshotReasonStrategyStart, domain.ReconciliationRunCheckpoint},
		{"StrategyEnd → checkpoint", domain.SnapshotReasonStrategyEnd, domain.ReconciliationRunCheckpoint},
		{"RestartRecovery → checkpoint", domain.SnapshotReasonRestartRecovery, domain.ReconciliationRunCheckpoint},
		{"PeriodicSample → sampled", domain.SnapshotReasonPeriodicSample, domain.ReconciliationRunSampled},
		{"InitialSeed → empty (no run)", domain.SnapshotReasonInitialSeed, ""},
		{"ReconciliationLocal → empty", domain.SnapshotReasonReconciliationLocal, ""},
		{"ReconciliationExchange → empty", domain.SnapshotReasonReconciliationExchange, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := domain.RunTypeFromReason(c.reason)
			if got != c.want {
				t.Errorf("RunTypeFromReason(%d) = %q, want %q", c.reason, got, c.want)
			}
		})
	}
}
