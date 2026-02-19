"""
ML-Audit Growth Intelligence Platform
Main FastAPI application
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, PlainTextResponse
from contextlib import asynccontextmanager
import os

from app.config import get_settings
from app.utils.logger import log
from app import __version__

# Import routers
from app.api import health, insights, sync, llm, monitoring, profitability, attribution, data_quality, seo, email, journey, user_behavior, ad_spend, weekly_brief, content_gap, code_health, redirect_health, ml_intelligence, pricing_impact, performance, customer_intelligence, merchant_center_intelligence, strategic_intelligence, finance, site_health, auth, brand_intelligence, competitor_blog
from app.middleware.auth_middleware import AuthMiddleware
from app.middleware.security_middleware import SecurityMiddleware

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    log.info(f"Starting {settings.app_name} v{__version__}")
    log.info(f"Environment: {settings.environment}")

    # Bootstrap credential files from env vars (for Render / PaaS)
    from app.utils.credentials import bootstrap_credentials
    bootstrap_credentials()

    # Initialize database
    try:
        from app.models.base import init_db, SessionLocal
        init_db()
        log.info("Database initialized")

        # Seed initial admin user if configured
        from app.services import auth_service
        db = SessionLocal()
        try:
            auth_service.seed_initial_user(db)
        finally:
            db.close()
    except Exception as e:
        log.error(f"Database initialization error: {str(e)}")

    # Start the scheduler for automated data syncs
    try:
        from app.scheduler import start_scheduler, stop_scheduler
        start_scheduler()
        log.info("Scheduler started successfully")
    except Exception as e:
        log.error(f"Scheduler startup error: {str(e)}")

    yield

    # Shutdown
    try:
        stop_scheduler()
    except Exception:
        pass
    log.info("Shutting down application")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=__version__,
    description="""
    AI-Powered Growth Intelligence Platform

    Comprehensive ML-based platform that:
    - Predicts customer churn and identifies at-risk customers
    - Detects anomalies in campaigns, traffic, and revenue
    - Monitors Google Ads for disapproved ads and performance issues
    - Analyzes SEO and technical issues
    - Generates actionable recommendations
    - Automates alerts for critical issues
    - **NEW** AI-powered natural language insights using Claude

    Integrates data from:
    - Shopify (e-commerce)
    - Klaviyo (email marketing)
    - Google Analytics 4 (web analytics)
    - Google Ads (paid advertising)

    LLM Features:
    - Natural language explanations of insights
    - Conversational queries ("Why did revenue drop?")
    - Executive summaries in plain English
    - Personalized customer win-back emails
    """,
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security middleware (Basic Auth gate, X-Robots-Tag, Cache-Control)
app.add_middleware(SecurityMiddleware)

# Session-based authentication middleware
app.add_middleware(AuthMiddleware)

# Gzip compression (70-80% smaller JSON/HTML responses)
from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=500)

# Include routers
app.include_router(auth.router)
app.include_router(health.router, tags=["health"])
app.include_router(insights.router)
app.include_router(sync.router)
app.include_router(llm.router)
app.include_router(monitoring.router)
app.include_router(profitability.router)
app.include_router(attribution.router)
app.include_router(data_quality.router)
app.include_router(seo.router)
app.include_router(email.router)
app.include_router(journey.router)
app.include_router(user_behavior.router)
app.include_router(ad_spend.router)
app.include_router(weekly_brief.router)
app.include_router(content_gap.router)
app.include_router(code_health.router)
app.include_router(redirect_health.router)
app.include_router(ml_intelligence.router)
app.include_router(performance.router)
app.include_router(pricing_impact.router)
app.include_router(customer_intelligence.router)
app.include_router(merchant_center_intelligence.router)
app.include_router(strategic_intelligence.router)
app.include_router(finance.router)
app.include_router(site_health.router)
app.include_router(brand_intelligence.router)
app.include_router(competitor_blog.router)


@app.get("/robots.txt", response_class=PlainTextResponse)
async def robots_txt():
    """Block all crawlers"""
    return "User-agent: *\nDisallow: /\n"


# Mount static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/dashboard")
async def dashboard():
    """Serve the dashboard"""
    return FileResponse(os.path.join(static_dir, "dashboard.html"))

@app.get("/inventory")
async def inventory_dashboard():
    """Serve the inventory dashboard"""
    return FileResponse(os.path.join(static_dir, "inventory.html"))

@app.get("/seo-dashboard")
async def seo_dashboard():
    """Serve the SEO dashboard"""
    return FileResponse(os.path.join(static_dir, "seo.html"))

@app.get("/performance")
async def performance_dashboard():
    """Serve the performance dashboard"""
    return FileResponse(os.path.join(static_dir, "performance.html"))


@app.get("/pricing-intel")
async def pricing_intel_dashboard():
    """Serve the pricing intelligence dashboard"""
    return FileResponse(os.path.join(static_dir, "pricing.html"))


@app.get("/customer-intelligence")
async def customer_intelligence_dashboard():
    """Serve the customer intelligence dashboard"""
    return FileResponse(os.path.join(static_dir, "customer_intelligence.html"))


@app.get("/merchant-center-intel")
async def merchant_center_intel_dashboard():
    """Serve the merchant center intelligence dashboard"""
    return FileResponse(os.path.join(static_dir, "merchant_center.html"))


@app.get("/strategic-intelligence")
async def strategic_intelligence_dashboard():
    """Serve the strategic intelligence brief dashboard"""
    return FileResponse(os.path.join(static_dir, "strategic_intelligence.html"))


@app.get("/finance-dashboard")
async def finance_dashboard_page():
    """Serve the finance P&L dashboard"""
    return FileResponse(os.path.join(static_dir, "finance.html"))


@app.get("/ads-intelligence")
async def ads_intelligence_page():
    """Serve the ads intelligence dashboard"""
    return FileResponse(os.path.join(static_dir, "ads_intelligence.html"))


@app.get("/site-intelligence")
async def site_intelligence_page():
    """Serve the site intelligence dashboard"""
    return FileResponse(os.path.join(static_dir, "site_intelligence.html"))


@app.get("/brand-intelligence")
async def brand_intelligence_page():
    """Serve the brand intelligence dashboard"""
    return FileResponse(os.path.join(static_dir, "brand_intelligence.html"))


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "app": settings.app_name,
        "version": __version__,
        "description": "AI-Powered Growth Intelligence Platform",
        "docs": "/docs",
        "health": "/health",
        "status": "/status",
        "features": {
            "churn_prediction": settings.enable_churn_prediction,
            "anomaly_detection": settings.enable_anomaly_detection,
            "seo_analysis": settings.enable_seo_analysis,
            "ad_monitoring": settings.enable_ad_monitoring,
            "auto_alerts": settings.enable_auto_alerts,
            "llm_insights": settings.enable_llm_insights
        },
        "endpoints": {
            "sync_all_data": "POST /sync/all",
            "run_full_analysis": "POST /insights/analyze",
            "predict_churn": "POST /insights/churn/predict",
            "detect_anomalies": "POST /insights/anomalies/detect",
            "seo_audit": "POST /insights/seo/audit",
            "ask_llm": "POST /llm/ask",
            "llm_status": "GET /llm/status",
            "start_monitoring": "POST /monitor/start",
            "dashboard": "GET /monitor/dashboard",
            "profitability_analyze": "POST /profitability/analyze",
            "profitability_insights": "GET /profitability/insights",
            "profitability_dashboard": "GET /profitability/dashboard",
            "attribution_insights": "GET /attribution/insights",
            "attribution_budget_recs": "GET /attribution/budget-recommendations",
            "attribution_dashboard": "GET /attribution/dashboard",
            "data_quality_dashboard": "GET /data-quality/dashboard",
            "data_quality_check": "POST /data-quality/run-check",
            "tracking_discrepancies": "GET /data-quality/discrepancies",
            "data_quality_diagnosis": "GET /data-quality/llm-diagnosis",
            "seo_dashboard": "GET /seo/dashboard",
            "seo_opportunities": "GET /seo/opportunities",
            "seo_quick_wins": "GET /seo/low-ctr",
            "seo_close_to_page_1": "GET /seo/close-to-page-1",
            "seo_declining_pages": "GET /seo/declining",
            "seo_technical_issues": "GET /seo/technical-issues",
            "seo_llm_insights": "GET /seo/llm-insights",
            "email_dashboard": "GET /email/dashboard",
            "email_opportunities": "GET /email/opportunities",
            "email_flows": "GET /email/flows",
            "email_segments": "GET /email/segments",
            "email_frequency": "GET /email/frequency",
            "email_missing_flows": "GET /email/missing-flows",
            "email_llm_insights": "GET /email/llm-insights",
            "journey_dashboard": "GET /journey/dashboard",
            "journey_segments": "GET /journey/segments",
            "journey_gateway_products": "GET /journey/gateway-products",
            "journey_dead_end_products": "GET /journey/dead-end-products",
            "journey_patterns": "GET /journey/patterns",
            "journey_churn_risk": "GET /journey/churn-risk",
            "journey_optimal_timing": "GET /journey/optimal-timing",
            "journey_llm_insights": "GET /journey/llm-insights",
            "behavior_dashboard": "GET /behavior/dashboard",
            "behavior_friction": "GET /behavior/friction",
            "behavior_checkout_funnel": "GET /behavior/checkout-funnel",
            "behavior_mobile_issues": "GET /behavior/mobile-issues",
            "behavior_rage_clicks": "GET /behavior/rage-clicks",
            "behavior_session_patterns": "GET /behavior/session-patterns",
            "behavior_page_analysis": "GET /behavior/page/{page_path}",
            "behavior_llm_insights": "GET /behavior/llm-insights",
            "behavior_quick_wins": "GET /behavior/quick-wins",
            "ads_dashboard": "GET /ads/dashboard",
            "ads_campaigns": "GET /ads/campaigns",
            "ads_scaling_opportunities": "GET /ads/scaling-opportunities",
            "ads_waste": "GET /ads/waste",
            "ads_budget_reallocation": "GET /ads/budget-reallocation",
            "ads_product_performance": "GET /ads/product-performance",
            "ads_llm_insights": "GET /ads/llm-insights",
            "ads_quick_wins": "GET /ads/quick-wins",
            "brief_current": "GET /brief/current",
            "brief_generate": "POST /brief/generate",
            "brief_history": "GET /brief/history",
            "brief_priorities": "GET /brief/priorities",
            "brief_working": "GET /brief/working",
            "brief_watch": "GET /brief/watch",
            "brief_trends": "GET /brief/trends",
            "brief_llm_summary": "GET /brief/llm-summary",
            "content_dashboard": "GET /content/dashboard",
            "content_gaps": "GET /content/gaps",
            "content_merchandising_gaps": "GET /content/merchandising-gaps",
            "content_opportunities": "GET /content/opportunities",
            "content_underperforming": "GET /content/underperforming",
            "content_category_health": "GET /content/category-health",
            "content_missing_types": "GET /content/missing-content-types",
            "content_quick_wins": "GET /content/quick-wins",
            "content_llm_insights": "GET /content/llm-insights",
            "code_dashboard": "GET /code/dashboard",
            "code_analyze": "GET /code/analyze",
            "code_quality": "GET /code/quality",
            "code_theme_health": "GET /code/theme-health",
            "code_security": "GET /code/security",
            "code_technical_debt": "GET /code/technical-debt",
            "code_commits": "GET /code/commits",
            "code_dependencies": "GET /code/dependencies",
            "code_priorities": "GET /code/priorities",
            "code_llm_insights": "GET /code/llm-insights",
            "redirects_dashboard": "GET /redirects/dashboard",
            "redirects_404_errors": "GET /redirects/404-errors",
            "redirects_revenue_impact": "GET /redirects/revenue-impact",
            "redirects_redirect_issues": "GET /redirects/redirect-issues",
            "redirects_redirect_chains": "GET /redirects/redirect-chains",
            "redirects_broken_links": "GET /redirects/broken-links",
            "redirects_recommendations": "GET /redirects/recommendations",
            "redirects_llm_insights": "GET /redirects/llm-insights",
            "ml_forecast": "GET /ml/forecast",
            "ml_anomalies": "GET /ml/anomalies",
            "ml_acknowledge_anomaly": "PATCH /ml/anomalies/{id}/acknowledge",
            "ml_drivers": "GET /ml/drivers",
            "ml_tracking_health": "GET /ml/tracking-health",
            "ml_inventory_suggestions": "GET /ml/inventory-suggestions",
            "ml_run_pipeline": "POST /ml/run",
            "pricing_impact": "GET /pricing/impact",
            "pricing_brand_summary": "GET /pricing/brand-summary",
            "pricing_unmatchable": "GET /pricing/unmatchable",
            "pricing_llm_insights": "GET /pricing/llm-insights",
            "merchant_center_dashboard": "GET /merchant-center/dashboard",
            "merchant_center_product_detail": "GET /merchant-center/product-detail",
            "merchant_center_search": "GET /merchant-center/search"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        workers=settings.api_workers if not settings.debug else 1
    )
