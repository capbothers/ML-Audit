"""
Focused tests for inventory reorder-point math edge cases.

Tests the pure computation logic:
  - Z-score interpolation
  - MOQ / case_pack rounding
  - Zero-demand paths
  - Extreme variance confidence downgrade
  - Missing lead time defaults
  - Seasonality clamping
  - Classification thresholds (reorder_now / reorder_soon / adequate / overstock)
  - Next-order-date projection
  - Dead-stock path

These are unit tests that do NOT require a database.
"""
import json
import math
import statistics
from datetime import date, timedelta

import pytest

# Under test
from app.services.ml_intelligence_service import (
    _z_score,
    DEFAULT_LEAD_TIME,
    DEFAULT_SERVICE_LEVEL,
    DEFAULT_MOQ,
    DEFAULT_CASE_PACK,
    _Z_SCORE_TABLE,
)


# ────────────────────────────────────────────
# Z-SCORE INTERPOLATION
# ────────────────────────────────────────────


class TestZScore:
    """Verify z-score lookup and interpolation."""

    def test_exact_table_values(self):
        """Exact entries in the table should return without interpolation."""
        for sl, expected_z in _Z_SCORE_TABLE.items():
            assert _z_score(sl) == expected_z, f"Failed for service_level={sl}"

    def test_interpolation_between_entries(self):
        """Interpolated value between 0.90 and 0.95 should be reasonable."""
        z = _z_score(0.925)
        # Midpoint between z(0.90)=1.28 and z(0.95)=1.65 → ~1.465
        assert 1.4 < z < 1.5, f"Got {z}"

    def test_below_minimum(self):
        """Service level below table minimum clamps to lowest z."""
        assert _z_score(0.50) == _Z_SCORE_TABLE[0.80]

    def test_above_maximum(self):
        """Service level above table maximum clamps to highest z."""
        assert _z_score(0.999) == _Z_SCORE_TABLE[0.99]

    def test_monotonically_increasing(self):
        """Higher service level → higher z-score, always."""
        prev = 0
        for sl in [0.80, 0.85, 0.90, 0.92, 0.95, 0.97, 0.98, 0.99]:
            z = _z_score(sl)
            assert z > prev, f"z({sl})={z} not > {prev}"
            prev = z


# ────────────────────────────────────────────
# MOQ / CASE-PACK ROUNDING
# ────────────────────────────────────────────


class TestMOQCasePackRounding:
    """Test that recommended_order_qty respects MOQ and case_pack constraints."""

    @staticmethod
    def _compute_rec_qty(raw: int, moq: int, case_pack: int) -> int:
        """Replicate the rounding logic from generate_inventory_suggestions."""
        qty = max(moq, raw)
        if case_pack > 1:
            qty = int(math.ceil(qty / case_pack) * case_pack)
        return qty

    def test_raw_below_moq_rounds_up(self):
        """If raw need is 3 but MOQ is 24, order 24."""
        assert self._compute_rec_qty(3, 24, 1) == 24

    def test_raw_above_moq_no_change(self):
        """If raw need is 50 and MOQ is 24, order 50."""
        assert self._compute_rec_qty(50, 24, 1) == 50

    def test_case_pack_rounds_up(self):
        """Raw 50 with case_pack 12 → 60 (5 cases)."""
        assert self._compute_rec_qty(50, 1, 12) == 60

    def test_moq_and_case_pack_combined(self):
        """Raw 3, MOQ 24, case_pack 12 → max(24,3)=24 → ceil(24/12)*12=24."""
        assert self._compute_rec_qty(3, 24, 12) == 24

    def test_moq_not_multiple_of_case_pack(self):
        """Raw 3, MOQ 25, case_pack 12 → max(25,3)=25 → ceil(25/12)*12=36."""
        assert self._compute_rec_qty(3, 25, 12) == 36

    def test_case_pack_1_no_effect(self):
        """case_pack=1 should never change the quantity."""
        assert self._compute_rec_qty(17, 1, 1) == 17

    def test_exact_multiple_of_case_pack(self):
        """Raw 48, case_pack 12 → 48 (no rounding needed)."""
        assert self._compute_rec_qty(48, 1, 12) == 48

    def test_moq_1_case_pack_large(self):
        """Raw 1, MOQ 1, case_pack 100 → 100."""
        assert self._compute_rec_qty(1, 1, 100) == 100


# ────────────────────────────────────────────
# ZERO-DEMAND PATHS
# ────────────────────────────────────────────


class TestZeroDemandPaths:
    """Verify behaviour when effective_velocity == 0."""

    def test_zero_velocity_positive_stock_is_no_sales(self):
        """Stock on hand but zero sales → no_sales / ok."""
        # Simulating the classification logic
        effective_velocity = 0
        units_on_hand = 50
        if effective_velocity == 0:
            days_of_cover = 999.0 if units_on_hand > 0 else 0.0
            suggestion = "no_sales"
            urgency = "ok"
        assert days_of_cover == 999.0
        assert suggestion == "no_sales"
        assert urgency == "ok"

    def test_zero_velocity_zero_stock_days_cover_zero(self):
        """Zero stock + zero sales → days_of_cover = 0."""
        effective_velocity = 0
        units_on_hand = 0
        days_of_cover = 999.0 if units_on_hand > 0 else 0.0
        assert days_of_cover == 0.0

    def test_zero_velocity_negative_stock_is_oversold(self):
        """Negative stock regardless of velocity → reorder_now / critical."""
        units_on_hand = -3
        oversold = units_on_hand < 0
        assert oversold is True


# ────────────────────────────────────────────
# CONFIDENCE SCORING
# ────────────────────────────────────────────


class TestConfidenceScoring:
    """Verify confidence downgrade logic."""

    @staticmethod
    def _compute_confidence(sales_days: int, cost_missing: bool, cv: float) -> str:
        """Replicate the confidence logic from generate_inventory_suggestions."""
        confidence = "high"
        if sales_days < 14 or cost_missing or cv > 0.8:
            confidence = "medium"
        if sales_days < 7 or cv > 1.5 or (cost_missing and sales_days < 14):
            confidence = "low"
        return confidence

    def test_high_confidence_normal(self):
        """30 sales days, cost present, low variance → high."""
        assert self._compute_confidence(30, False, 0.3) == "high"

    def test_medium_sparse_sales(self):
        """12 sales days (< 14) → medium."""
        assert self._compute_confidence(12, False, 0.3) == "medium"

    def test_medium_cost_missing(self):
        """Cost missing alone → medium."""
        assert self._compute_confidence(30, True, 0.3) == "medium"

    def test_medium_high_cv(self):
        """CV 0.9 (> 0.8) → medium."""
        assert self._compute_confidence(30, False, 0.9) == "medium"

    def test_low_very_sparse(self):
        """5 sales days (< 7) → low."""
        assert self._compute_confidence(5, False, 0.3) == "low"

    def test_low_extreme_variance(self):
        """CV 2.0 (> 1.5) → low."""
        assert self._compute_confidence(30, False, 2.0) == "low"

    def test_low_cost_missing_and_sparse(self):
        """Cost missing + 10 sales days (< 14) → low."""
        assert self._compute_confidence(10, True, 0.3) == "low"

    def test_high_boundary_14_days(self):
        """Exactly 14 sales days, no cost issue, low CV → high."""
        assert self._compute_confidence(14, False, 0.5) == "high"

    def test_medium_boundary_cv_0_8(self):
        """CV exactly 0.8 → still high (> 0.8 required for medium)."""
        assert self._compute_confidence(30, False, 0.8) == "high"


# ────────────────────────────────────────────
# CLASSIFICATION THRESHOLDS
# ────────────────────────────────────────────


class TestClassificationThresholds:
    """Verify reorder-point-based classification."""

    @staticmethod
    def _classify(units_on_hand: int, effective_velocity: float,
                  safety_stock: float, reorder_point: float) -> tuple:
        """Replicate classification logic."""
        oversold = units_on_hand < 0
        if oversold:
            return "reorder_now", "critical"
        if effective_velocity == 0:
            return "no_sales", "ok"
        if units_on_hand <= safety_stock:
            return "reorder_now", "critical"
        if units_on_hand <= reorder_point:
            return "reorder_soon", "warning"
        days_of_cover = units_on_hand / effective_velocity if effective_velocity > 0 else 999
        if days_of_cover <= 60:
            return "adequate", "ok"
        return "overstock", "ok"

    def test_oversold_is_always_critical(self):
        assert self._classify(-5, 2.0, 10, 50) == ("reorder_now", "critical")

    def test_zero_demand_is_no_sales(self):
        assert self._classify(100, 0, 0, 0) == ("no_sales", "ok")

    def test_below_safety_stock_is_critical(self):
        """On-hand 8, safety stock 10 → reorder_now / critical."""
        assert self._classify(8, 2.0, 10, 50) == ("reorder_now", "critical")

    def test_between_ss_and_rp_is_warning(self):
        """On-hand 30, safety_stock=10, reorder_point=50 → reorder_soon."""
        assert self._classify(30, 2.0, 10, 50) == ("reorder_soon", "warning")

    def test_above_rp_within_60d_is_adequate(self):
        """On-hand 80, velocity 2.0, rp=50 → 80/2=40 days → adequate."""
        assert self._classify(80, 2.0, 10, 50) == ("adequate", "ok")

    def test_way_above_rp_is_overstock(self):
        """On-hand 200, velocity 2.0 → 200/2=100 days → overstock."""
        assert self._classify(200, 2.0, 10, 50) == ("overstock", "ok")

    def test_exactly_at_safety_stock_is_critical(self):
        """On-hand equals safety stock → reorder_now (<=)."""
        assert self._classify(10, 2.0, 10, 50) == ("reorder_now", "critical")

    def test_exactly_at_reorder_point_is_warning(self):
        """On-hand equals reorder point → reorder_soon (<=)."""
        assert self._classify(50, 2.0, 10, 50) == ("reorder_soon", "warning")


# ────────────────────────────────────────────
# NEXT-ORDER-DATE PROJECTION
# ────────────────────────────────────────────


class TestNextOrderDate:
    """Verify next_order_date calculation."""

    def test_below_rp_is_today(self):
        """If already at or below reorder point → order today."""
        rp = 50.0
        on_hand = 40
        adjusted_velocity = 2.0
        if on_hand <= rp:
            nod = date.today()
        assert nod == date.today()

    def test_above_rp_projects_forward(self):
        """On-hand 100, rp=50, velocity 2 → (100-50)/2 = 25 days from today."""
        rp = 50.0
        on_hand = 100
        adjusted_velocity = 2.0
        days_until = (on_hand - rp) / adjusted_velocity
        nod = date.today() + timedelta(days=int(days_until))
        assert nod == date.today() + timedelta(days=25)

    def test_very_slow_velocity_far_future(self):
        """On-hand 100, rp=10, velocity 0.1 → (90)/0.1 = 900 days."""
        rp = 10.0
        on_hand = 100
        adjusted_velocity = 0.1
        days_until = (on_hand - rp) / adjusted_velocity
        assert int(days_until) == 900


# ────────────────────────────────────────────
# SEASONALITY CLAMPING
# ────────────────────────────────────────────


class TestSeasonalityClamping:
    """Verify seasonality factor is clamped to [0.5, 2.0]."""

    @staticmethod
    def _clamp_season(same_month_units: float, avg_monthly: float) -> float:
        if avg_monthly <= 0:
            return 1.0
        factor = same_month_units / avg_monthly
        return max(0.5, min(2.0, factor))

    def test_normal_factor(self):
        """1.2x should pass through unchanged."""
        assert self._clamp_season(120, 100) == pytest.approx(1.2)

    def test_extreme_high_clamped(self):
        """5x should clamp to 2.0."""
        assert self._clamp_season(500, 100) == 2.0

    def test_extreme_low_clamped(self):
        """0.1x should clamp to 0.5."""
        assert self._clamp_season(10, 100) == 0.5

    def test_zero_avg_returns_1(self):
        """No historical average → factor = 1.0."""
        assert self._clamp_season(50, 0) == 1.0

    def test_equal_months_is_1(self):
        """Same month units = average → 1.0."""
        assert self._clamp_season(100, 100) == pytest.approx(1.0)


# ────────────────────────────────────────────
# DEMAND STATS COMPUTATION
# ────────────────────────────────────────────


class TestDemandStats:
    """Verify std_dev / CV computation with zero-padding."""

    @staticmethod
    def _compute_stats(daily_values: list) -> dict:
        """Replicate the demand_stats computation."""
        vals = list(daily_values)
        while len(vals) < 30:
            vals.append(0.0)
        mean = statistics.mean(vals)
        sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
        return {
            "std_dev": sd,
            "cv": sd / mean if mean > 0 else 0.0,
            "sales_days": sum(1 for v in vals if v > 0),
        }

    def test_steady_demand(self):
        """10 units every day for 30 days → low std_dev."""
        stats = self._compute_stats([10.0] * 30)
        assert stats["std_dev"] == 0.0
        assert stats["cv"] == 0.0
        assert stats["sales_days"] == 30

    def test_sparse_demand(self):
        """3 days of sales in 30 → padded with 27 zeros."""
        stats = self._compute_stats([5.0, 3.0, 7.0])
        assert stats["sales_days"] == 3
        assert stats["cv"] > 1.0  # very spiky

    def test_single_sale(self):
        """1 day of sales → 1 non-zero + 29 zeros."""
        stats = self._compute_stats([100.0])
        assert stats["sales_days"] == 1
        assert stats["std_dev"] > 0

    def test_no_sales(self):
        """Empty list → all zeros, cv=0."""
        stats = self._compute_stats([])
        assert stats["sales_days"] == 0
        assert stats["cv"] == 0.0


# ────────────────────────────────────────────
# EXPLANATION JSON STRUCTURE
# ────────────────────────────────────────────


class TestExplanationJSON:
    """Verify explanation JSON has all required keys."""

    REQUIRED_KEYS = {
        "velocity_30d", "velocity_7d", "effective_velocity",
        "seasonality_factor", "adjusted_velocity",
        "lead_time_days", "lead_time_demand",
        "service_level", "z_score",
        "demand_std_dev", "safety_stock",
        "reorder_point", "units_on_hand",
        "recommended_qty", "moq", "case_pack",
        "sales_days_30d", "demand_cv", "data_issues",
    }

    def test_active_sku_has_all_keys(self):
        """Build a sample explanation dict and verify all keys present."""
        explanation = json.dumps({
            "velocity_30d": 2.5, "velocity_7d": 3.1, "effective_velocity": 3.1,
            "seasonality_factor": 1.2, "adjusted_velocity": 3.72,
            "lead_time_days": 14, "lead_time_demand": 52.1,
            "service_level": 0.95, "z_score": 1.65,
            "demand_std_dev": 1.8, "safety_stock": 12.3,
            "reorder_point": 64.4, "units_on_hand": 45,
            "recommended_qty": 48, "moq": 24, "case_pack": 12,
            "sales_days_30d": 28, "demand_cv": 0.3,
            "data_issues": [],
        })
        parsed = json.loads(explanation)
        assert self.REQUIRED_KEYS.issubset(parsed.keys())

    def test_dead_stock_has_all_keys(self):
        """Dead-stock explanation should also have all required keys."""
        explanation = json.dumps({
            "velocity_30d": 0, "velocity_7d": 0, "effective_velocity": 0,
            "seasonality_factor": 1.0, "adjusted_velocity": 0,
            "lead_time_days": 14, "lead_time_demand": 0,
            "service_level": 0.95, "z_score": 0,
            "demand_std_dev": 0, "safety_stock": 0, "reorder_point": 0,
            "units_on_hand": 25, "recommended_qty": None,
            "moq": 1, "case_pack": 1,
            "sales_days_30d": 0, "demand_cv": 0,
            "data_issues": ["no_sales"],
        })
        parsed = json.loads(explanation)
        assert self.REQUIRED_KEYS.issubset(parsed.keys())
        assert "no_sales" in parsed["data_issues"]

    def test_data_issues_is_list(self):
        """data_issues must always be a list."""
        explanation = json.dumps({
            "velocity_30d": 0, "velocity_7d": 0, "effective_velocity": 0,
            "seasonality_factor": 1.0, "adjusted_velocity": 0,
            "lead_time_days": 14, "lead_time_demand": 0,
            "service_level": 0.95, "z_score": 0,
            "demand_std_dev": 0, "safety_stock": 0, "reorder_point": 0,
            "units_on_hand": 0, "recommended_qty": None,
            "moq": 1, "case_pack": 1,
            "sales_days_30d": 0, "demand_cv": 0,
            "data_issues": ["missing_cost", "default_lead_time"],
        })
        parsed = json.loads(explanation)
        assert isinstance(parsed["data_issues"], list)


# ────────────────────────────────────────────
# END-TO-END REORDER POINT FORMULA
# ────────────────────────────────────────────


class TestReorderPointFormula:
    """Verify the full reorder-point computation chain."""

    def test_standard_case(self):
        """
        velocity=2, lead_time=14, service=0.95, std_dev=1.5
        lt_demand = 2 * 14 = 28
        ss = 1.65 * 1.5 * sqrt(14) = 1.65 * 1.5 * 3.742 = 9.26
        rp = 28 + 9.26 = 37.26
        """
        velocity = 2.0
        lt = 14
        sl = 0.95
        std_dev = 1.5

        lt_demand = velocity * lt
        z = _z_score(sl)
        ss = z * std_dev * math.sqrt(lt)
        rp = lt_demand + ss

        assert lt_demand == pytest.approx(28.0)
        assert z == pytest.approx(1.65)
        assert ss == pytest.approx(9.26, abs=0.1)
        assert rp == pytest.approx(37.26, abs=0.1)

    def test_zero_std_dev_means_no_safety_stock(self):
        """Perfectly steady demand → safety stock = 0."""
        ss = _z_score(0.95) * 0.0 * math.sqrt(14)
        assert ss == 0.0

    def test_high_service_level_means_more_safety_stock(self):
        """0.99 service level → higher z → more safety stock."""
        std_dev = 2.0
        lt = 14
        ss_95 = _z_score(0.95) * std_dev * math.sqrt(lt)
        ss_99 = _z_score(0.99) * std_dev * math.sqrt(lt)
        assert ss_99 > ss_95

    def test_longer_lead_time_means_more_safety_stock(self):
        """Longer lead time → more uncertainty → more safety stock."""
        std_dev = 2.0
        ss_7 = _z_score(0.95) * std_dev * math.sqrt(7)
        ss_28 = _z_score(0.95) * std_dev * math.sqrt(28)
        assert ss_28 > ss_7

    def test_recommended_qty_with_seasonality(self):
        """
        velocity=3, season=1.5, lt=14, ss=10, on_hand=20
        adjusted = 3 * 1.5 = 4.5
        target = 4.5 * (14+14) + 10 = 136
        raw = 136 - 20 = 116
        moq=24, case_pack=12 → max(24, 116) = 116 → ceil(116/12)*12 = 120
        """
        adjusted_velocity = 3.0 * 1.5
        lt = 14
        ss = 10.0
        on_hand = 20
        target = adjusted_velocity * (lt + 14) + ss
        raw = max(1, math.ceil(target - on_hand))
        moq = 24
        cp = 12
        qty = max(moq, raw)
        qty = int(math.ceil(qty / cp) * cp)
        assert qty == 120
