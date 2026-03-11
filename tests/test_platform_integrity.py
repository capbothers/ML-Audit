"""
Platform integrity tests.

Covers three high-value invariants identified in the platform audit:

1. Google Ads sheet import — manual API path and scheduler path both call
   update_data_sync_status() with a SyncResult that uses rows_created /
   rows_updated (not rows_imported), and both write a failed record when
   the importer signals failure.

2. Stale recovery — sync_stale_connectors() sources its thresholds from
   app.freshness.STALE_THRESHOLDS, so ga4 / search_console are not treated
   as stale at 24h when the rest of the platform uses 72h / 96h.

3. Freshness alias normalisation — product_costs and google_sheets_costs
   both resolve to the canonical DB key cost_sheet, and
   _check_degraded_state() uses the canonical key when consulting the
   freshness dict so pricing / profitability modules are not incorrectly
   marked stale.
"""
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sheet_success(rows_created=10, rows_updated=3):
    return {"success": True, "rows_created": rows_created, "rows_updated": rows_updated}


def _sheet_failure(error="Auth failed"):
    return {"success": False, "error": error}


# ---------------------------------------------------------------------------
# 1a. Scheduler path — success writes correct SyncResult fields
# ---------------------------------------------------------------------------

def test_scheduler_ads_sheet_success_uses_rows_created_updated():
    """sync_google_ads_sheet() must pass rows_created/rows_updated to SyncResult, not rows_imported."""
    from app.scheduler import sync_google_ads_sheet

    campaign = _sheet_success(rows_created=10, rows_updated=3)
    product = _sheet_success(rows_created=2, rows_updated=1)

    # clear_for_source and response_cache are imported locally inside the function;
    # patch at their source modules so the in-function import picks up the mock.
    with patch("app.services.google_ads_sheet_import.GoogleAdsSheetImportService") as mock_cls, \
         patch("app.scheduler.update_data_sync_status") as mock_uds, \
         patch("app.scheduler.settings") as mock_settings, \
         patch("app.utils.cache.clear_for_source"), \
         patch("app.utils.response_cache.response_cache"):

        mock_settings.google_ads_sheet_id = "sheet123"
        mock_settings.google_sheets_credentials_path = "/creds.json"
        mock_settings.google_ads_sheet_tab = "Campaign Data"

        svc = MagicMock()
        svc.import_from_sheet.return_value = campaign
        svc.import_products_from_sheet.return_value = product
        mock_cls.return_value = svc

        asyncio.run(sync_google_ads_sheet())

    assert mock_uds.called, "update_data_sync_status must be called on success"
    result = mock_uds.call_args[0][0]          # the SyncResult positional arg

    assert result.source == "google_ads"
    assert result.status == "success"
    assert result.records_created == 12        # 10 + 2
    assert result.records_updated == 4         # 3 + 1
    # Must NOT carry a rows_imported field (old, wrong semantics)
    assert not hasattr(result, "rows_imported"), "SyncResult must not have rows_imported"


# ---------------------------------------------------------------------------
# 1b. Scheduler path — logical failure writes failed SyncResult, no cache flush
# ---------------------------------------------------------------------------

def test_scheduler_ads_sheet_failure_writes_failed_status():
    """sync_google_ads_sheet() must write a failed SyncResult and NOT flush caches."""
    from app.scheduler import sync_google_ads_sheet

    with patch("app.services.google_ads_sheet_import.GoogleAdsSheetImportService") as mock_cls, \
         patch("app.scheduler.update_data_sync_status") as mock_uds, \
         patch("app.scheduler.settings") as mock_settings, \
         patch("app.utils.cache.clear_for_source") as mock_clear, \
         patch("app.utils.response_cache.response_cache") as mock_cache:

        mock_settings.google_ads_sheet_id = "sheet123"
        mock_settings.google_sheets_credentials_path = "/creds.json"
        mock_settings.google_ads_sheet_tab = "Campaign Data"

        svc = MagicMock()
        svc.import_from_sheet.return_value = _sheet_failure("Auth failed")
        svc.import_products_from_sheet.return_value = _sheet_success()
        mock_cls.return_value = svc

        asyncio.run(sync_google_ads_sheet())

    assert mock_uds.called, "update_data_sync_status must be called on failure"
    result = mock_uds.call_args[0][0]
    assert result.status == "failed"
    assert "Auth failed" in (result.error_message or "")
    mock_clear.assert_not_called()
    mock_cache.invalidate.assert_not_called()


# ---------------------------------------------------------------------------
# 1c. API path — success writes correct SyncResult fields
# ---------------------------------------------------------------------------

def test_api_ads_sheet_success_uses_rows_created_updated():
    """import_google_ads_from_sheet() must call update_data_sync_status with rows_created/rows_updated."""
    from app.api.sync import import_google_ads_from_sheet

    campaign = _sheet_success(rows_created=7, rows_updated=2)
    product = _sheet_success(rows_created=1, rows_updated=0)

    # get_settings is imported locally inside import_google_ads_from_sheet;
    # patch at the config module so the in-function import picks up the mock.
    with patch("app.services.google_ads_sheet_import.GoogleAdsSheetImportService") as mock_cls, \
         patch("app.services.data_sync_service.update_data_sync_status") as mock_uds, \
         patch("app.config.get_settings") as mock_cfg:

        cfg = MagicMock()
        cfg.google_ads_sheet_id = "sheet123"
        cfg.google_sheets_credentials_path = "/creds.json"
        cfg.google_ads_sheet_tab = "Campaign Data"
        mock_cfg.return_value = cfg

        svc = MagicMock()
        svc.import_from_sheet.return_value = campaign
        svc.import_products_from_sheet.return_value = product
        mock_cls.return_value = svc

        result = import_google_ads_from_sheet()

    assert result["success"] is True
    assert mock_uds.called
    sr = mock_uds.call_args[0][0]
    assert sr.source == "google_ads"
    assert sr.status == "success"
    assert sr.records_created == 8    # 7 + 1
    assert sr.records_updated == 2    # 2 + 0


# ---------------------------------------------------------------------------
# 1d. Both paths agree: SyncResult fields are semantically equivalent
# ---------------------------------------------------------------------------

def test_scheduler_and_api_sync_result_fields_are_equivalent():
    """Scheduler and API paths must pass the same SyncResult field names to update_data_sync_status."""
    from app.scheduler import sync_google_ads_sheet
    from app.api.sync import import_google_ads_from_sheet

    sched_result = None
    api_result = None

    campaign = _sheet_success(rows_created=5, rows_updated=1)
    product = _sheet_success(rows_created=0, rows_updated=0)

    # --- scheduler ---
    with patch("app.services.google_ads_sheet_import.GoogleAdsSheetImportService") as mock_cls, \
         patch("app.scheduler.update_data_sync_status") as mock_uds, \
         patch("app.scheduler.settings") as mock_settings, \
         patch("app.utils.cache.clear_for_source"), \
         patch("app.utils.response_cache.response_cache"):

        mock_settings.google_ads_sheet_id = "sheet123"
        mock_settings.google_sheets_credentials_path = "/creds.json"
        mock_settings.google_ads_sheet_tab = "Campaign Data"
        svc = MagicMock()
        svc.import_from_sheet.return_value = campaign
        svc.import_products_from_sheet.return_value = product
        mock_cls.return_value = svc
        asyncio.run(sync_google_ads_sheet())
        sched_result = mock_uds.call_args[0][0]

    # --- API ---
    with patch("app.services.google_ads_sheet_import.GoogleAdsSheetImportService") as mock_cls, \
         patch("app.services.data_sync_service.update_data_sync_status") as mock_uds, \
         patch("app.config.get_settings") as mock_cfg:

        cfg = MagicMock()
        cfg.google_ads_sheet_id = "sheet123"
        cfg.google_sheets_credentials_path = "/creds.json"
        cfg.google_ads_sheet_tab = "Campaign Data"
        mock_cfg.return_value = cfg
        svc = MagicMock()
        svc.import_from_sheet.return_value = campaign
        svc.import_products_from_sheet.return_value = product
        mock_cls.return_value = svc
        import_google_ads_from_sheet()
        api_result = mock_uds.call_args[0][0]

    assert sched_result.source == api_result.source == "google_ads"
    assert sched_result.status == api_result.status == "success"
    assert sched_result.records_created == api_result.records_created
    assert sched_result.records_updated == api_result.records_updated


# ---------------------------------------------------------------------------
# 2. Stale recovery thresholds come from app.freshness.STALE_THRESHOLDS
# ---------------------------------------------------------------------------

def test_stale_recovery_respects_freshness_thresholds():
    """Sources within their per-source threshold must NOT be synced; beyond it, must be synced."""
    from app.freshness import STALE_THRESHOLDS
    from app.scheduler import sync_stale_connectors

    now = datetime.utcnow()

    def _status(source, lag_hours):
        s = MagicMock()
        s.source_name = source
        s.last_successful_sync = now - timedelta(hours=lag_hours)
        return s

    # ga4 threshold is 72h — 60h lag should be fresh, 80h lag should be stale
    statuses = [
        _status("shopify", STALE_THRESHOLDS["shopify"] + 1),    # stale
        _status("ga4", STALE_THRESHOLDS["ga4"] - 1),            # fresh
        _status("search_console", STALE_THRESHOLDS["search_console"] + 1),  # stale
        _status("merchant_center", STALE_THRESHOLDS["merchant_center"] - 1),  # fresh
        _status("google_ads", STALE_THRESHOLDS["google_ads"] + 1),  # stale
    ]

    synced = []

    async def _fake_sync_fn(name):
        async def _inner():
            synced.append(name)
        return _inner

    async def run():
        with patch("app.models.base.SessionLocal") as mock_sl, \
             patch("app.scheduler.sync_shopify", new_callable=AsyncMock) as m_shopify, \
             patch("app.scheduler.sync_ga4", new_callable=AsyncMock) as m_ga4, \
             patch("app.scheduler.sync_search_console", new_callable=AsyncMock) as m_sc, \
             patch("app.scheduler.sync_merchant_center", new_callable=AsyncMock) as m_mc, \
             patch("app.scheduler.sync_google_ads_sheet", new_callable=AsyncMock) as m_ads, \
             patch("app.scheduler.sync_google_ads", new_callable=AsyncMock), \
             patch("app.scheduler.settings") as mock_settings, \
             patch("app.scheduler._check_memory", return_value=True):

            mock_settings.google_ads_sheet_id = "sheet123"
            db = MagicMock()
            db.query.return_value.filter.return_value.all.return_value = statuses
            mock_sl.return_value.__enter__ = MagicMock(return_value=db)
            mock_sl.return_value.__exit__ = MagicMock(return_value=False)
            mock_sl.return_value = db

            await sync_stale_connectors()

            return {
                "shopify": m_shopify.called,
                "ga4": m_ga4.called,
                "search_console": m_sc.called,
                "merchant_center": m_mc.called,
                "google_ads": m_ads.called,
            }

    called = asyncio.run(run())

    assert called["shopify"] is True,       "shopify is stale — should be synced"
    assert called["ga4"] is False,          "ga4 is within 72h threshold — must NOT be synced"
    assert called["search_console"] is True, "search_console is stale — should be synced"
    assert called["merchant_center"] is False, "merchant_center is within threshold — must NOT be synced"
    assert called["google_ads"] is True,    "google_ads is stale — should be synced"


def test_stale_recovery_ga4_threshold_is_not_24h():
    """ga4 stale threshold must be 72h (not the old hardcoded 24h)."""
    from app.freshness import STALE_THRESHOLDS
    assert STALE_THRESHOLDS["ga4"] == 72, \
        f"ga4 threshold should be 72h, got {STALE_THRESHOLDS['ga4']}h"


def test_stale_recovery_search_console_threshold_is_not_24h():
    """search_console stale threshold must be 96h (not the old hardcoded 24h)."""
    from app.freshness import STALE_THRESHOLDS
    assert STALE_THRESHOLDS["search_console"] == 96, \
        f"search_console threshold should be 96h, got {STALE_THRESHOLDS['search_console']}h"


# ---------------------------------------------------------------------------
# 3a. Freshness module — alias normalisation
# ---------------------------------------------------------------------------

def test_normalize_key_product_costs_resolves_to_cost_sheet():
    from app.freshness import normalize_key
    assert normalize_key("product_costs") == "cost_sheet"


def test_normalize_key_google_sheets_costs_resolves_to_cost_sheet():
    from app.freshness import normalize_key
    assert normalize_key("google_sheets_costs") == "cost_sheet"


def test_normalize_key_canonical_key_is_unchanged():
    from app.freshness import normalize_key
    for key in ("shopify", "ga4", "search_console", "google_ads", "merchant_center",
                "competitive_pricing", "cost_sheet"):
        assert normalize_key(key) == key, f"Canonical key '{key}' should not be aliased"


def test_get_threshold_via_alias():
    from app.freshness import get_threshold
    assert get_threshold("product_costs") == get_threshold("cost_sheet") == 720
    assert get_threshold("google_sheets_costs") == 720


# ---------------------------------------------------------------------------
# 3b. _check_degraded_state uses canonical key when looking up freshness dict
# ---------------------------------------------------------------------------

def _make_sis(db=None):
    """Construct a StrategicIntelligenceService with a minimal stub DB."""
    from app.services.strategic_intelligence_service import StrategicIntelligenceService
    return StrategicIntelligenceService(db or MagicMock())


def test_check_degraded_state_resolves_product_costs_to_cost_sheet():
    """pricing module depends on product_costs; freshness dict uses cost_sheet as key.
    _check_degraded_state must resolve the alias and correctly detect a stale cost_sheet.
    """
    svc = _make_sis()

    # freshness is keyed by canonical DB key
    freshness = {
        "competitive_pricing": {"is_stale": False, "lag_hours": 10, "last_sync": "2026-03-10T00:00:00"},
        "cost_sheet": {"is_stale": True,  "lag_hours": 800, "last_sync": "2025-12-01T00:00:00"},
    }
    module_meta = {
        "queried": ["pricing"],
        "succeeded": ["pricing"],
        "failed": [],
    }

    is_degraded, stale = svc._check_degraded_state(module_meta, freshness)

    stale_sources = [s["source"] for s in stale]
    # product_costs maps to cost_sheet which IS stale
    assert any(m["module"] == "pricing" for m in stale), \
        "pricing module should be marked stale when cost_sheet is stale"


def test_check_degraded_state_does_not_mark_fresh_cost_sheet_as_stale():
    """When cost_sheet is fresh, pricing module must not appear in stale list."""
    svc = _make_sis()

    freshness = {
        "competitive_pricing": {"is_stale": False, "lag_hours": 5, "last_sync": "2026-03-10T00:00:00"},
        "cost_sheet": {"is_stale": False, "lag_hours": 100, "last_sync": "2026-03-08T00:00:00"},
    }
    module_meta = {
        "queried": ["pricing"],
        "succeeded": ["pricing"],
        "failed": [],
    }

    is_degraded, stale = svc._check_degraded_state(module_meta, freshness)

    assert not any(m["module"] == "pricing" for m in stale), \
        "pricing module must not be stale when both dependencies are fresh"


def test_check_degraded_state_canonical_key_wins_over_non_canonical():
    """When cost_sheet is stale AND google_sheets_costs is falsely fresh, pricing must still be stale.
    Guards against regression where the non-canonical key bypassed the stale flag.
    """
    svc = _make_sis()

    freshness = {
        "competitive_pricing": {"is_stale": False, "lag_hours": 5, "last_sync": "2026-03-10T00:00:00"},
        "cost_sheet": {"is_stale": True, "lag_hours": 800, "last_sync": "2025-11-01T00:00:00"},
        # Deliberately add the non-canonical key marked as fresh — it must not override the canonical
        "google_sheets_costs": {"is_stale": False, "lag_hours": 10, "last_sync": "2026-03-10T00:00:00"},
    }
    module_meta = {
        "queried": ["pricing"],
        "succeeded": ["pricing"],
        "failed": [],
    }

    _is_degraded, stale = svc._check_degraded_state(module_meta, freshness)

    assert any(m["module"] == "pricing" for m in stale), \
        "pricing must be stale when cost_sheet (canonical key) is stale, regardless of google_sheets_costs"
