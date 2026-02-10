"""
Configuration management for ML-Audit platform
"""
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """Application settings"""

    # Application
    app_name: str = "ML-Audit Growth Intelligence Platform"
    environment: str = "development"
    debug: bool = True
    log_level: str = "INFO"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4

    # Database
    database_url: str
    redis_url: str = "redis://localhost:6379/0"

    # Shopify
    shopify_shop_url: str
    shopify_api_key: str
    shopify_api_secret: str
    shopify_access_token: str
    shopify_api_version: str = "2024-01"

    # Klaviyo
    klaviyo_api_key: str
    klaviyo_public_key: Optional[str] = None

    # Google Analytics 4
    ga4_property_id: str
    ga4_credentials_path: str = "./credentials/ga4-credentials.json"

    # Google Ads
    google_ads_client_id: str
    google_ads_client_secret: str
    google_ads_developer_token: str
    google_ads_refresh_token: str
    google_ads_customer_id: str
    google_ads_login_customer_id: Optional[str] = None

    # Google Search Console
    gsc_credentials_path: str = "./credentials/gsc-credentials.json"
    gsc_site_url: str
    # Backfill settings
    gsc_backfill_window_days: int = 14  # Days per fetch window (7-30)
    gsc_backfill_delay_seconds: float = 2.0  # Delay between windows
    gsc_backfill_max_retries: int = 3  # Retries per window on failure
    # Daily sync settings
    gsc_daily_sync_days: int = 3  # Days to sync in daily job (1-7)
    # Brand terms to exclude from non-brand query analysis (comma-separated)
    gsc_brand_terms: str = ""  # e.g., "cass,cass brothers,cassbrothers"
    # Brand term allow/deny lists for short or ambiguous brands (JSON mapping)
    # Example: {"zip":["zip tap","zip hydrotap"],"nike":["nike"]} / {"zip":["zip code","zip file"]}
    brand_term_allowlist: str = ""
    brand_term_denylist: str = ""

    # Google Sheets (for product cost data)
    google_sheets_credentials_path: str = "./credentials/google-sheets-credentials.json"
    cost_sheet_id: str  # Spreadsheet ID
    cost_sheet_range: str = "A:AA"  # Column range for vendor tabs
    cost_sheet_tab_prefix: str = ""  # Empty = all vendor tabs (excludes metadata tabs)

    # Google Ads via Google Sheets (automated export from Google Ads Scripts)
    google_ads_sheet_id: Optional[str] = None  # Spreadsheet ID for ads data
    google_ads_sheet_tab: str = "Campaign Data"  # Tab name in the sheet

    # Google Merchant Center
    merchant_center_id: str = ""
    merchant_center_credentials_path: str = "./credentials/google-sheets-sa.json"

    # Hotjar
    hotjar_site_id: Optional[str] = None
    hotjar_api_key: Optional[str] = None

    # Microsoft Clarity (alternative to Hotjar)
    clarity_project_id: Optional[str] = None
    clarity_api_key: Optional[str] = None

    # GitHub (Shopify Theme Repository)
    github_access_token: Optional[str] = None
    github_repo_owner: Optional[str] = None
    github_repo_name: Optional[str] = None

    # ML Configuration
    ml_model_path: str = "./models"
    ml_training_interval: int = 86400
    churn_prediction_threshold: float = 0.7
    anomaly_detection_sensitivity: float = 0.05

    # Alerts
    alert_email_from: Optional[str] = None
    alert_email_to: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    slack_webhook_url: Optional[str] = None

    # Sync Schedules
    sync_shopify_schedule: str = "0 */6 * * *"
    sync_klaviyo_schedule: str = "0 */4 * * *"
    sync_ga4_schedule: str = "0 2 * * *"
    sync_google_ads_schedule: str = "0 1 * * *"

    # LLM Configuration
    anthropic_api_key: Optional[str] = None
    llm_model: str = "claude-sonnet-4-20250514"
    enable_llm_insights: bool = True
    llm_max_tokens: int = 2000

    # Authentication
    initial_admin_email: str = ""
    initial_admin_password: str = ""
    session_duration_hours: int = 72

    # Dashboard Basic Auth (gate for the whole app)
    dash_user: str = ""
    dash_pass: str = ""

    # Feature Flags
    enable_churn_prediction: bool = True
    enable_anomaly_detection: bool = True
    enable_seo_analysis: bool = True
    enable_ad_monitoring: bool = True
    enable_auto_alerts: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
