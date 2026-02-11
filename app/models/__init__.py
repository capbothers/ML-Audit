"""Database models for ML-Audit platform"""

from app.models.customer import Customer

from app.models.product import (
    Product,
    ProductSale,
    ProductProfitability,
    ProductAdSpendAllocation
)

from app.models.attribution import (
    CustomerTouchpoint,
    CustomerJourney,
    ChannelAttribution,
    AttributionInsight
)

from app.models.data_quality import (
    DataSyncStatus,
    TrackingDiscrepancy,
    UTMHealthCheck,
    MerchantCenterHealth,
    GoogleAdsLinkHealth,
    DataQualityScore,
    TrackingAlert
)

from app.models.seo import (
    SearchQuery,
    PageSEO,
    SEOOpportunity,
    IndexCoverage,
    SEOInsight,
    CoreWebVitals
)

from app.models.shopify import (
    ShopifyOrder,
    ShopifyProduct,
    ShopifyCustomer,
    ShopifyRefund,
    ShopifyRefundLineItem,
    ShopifyInventory
)

from app.models.product_cost import ProductCost
from app.models.competitive_pricing import CompetitivePricing

from app.models.email import (
    EmailCampaign,
    EmailFlow,
    EmailSegment,
    EmailSendFrequency,
    EmailInsight,
    EmailRevenueOpportunity
)

from app.models.journey import (
    CustomerLTV,
    JourneyPattern,
    GatewayProduct,
    DeadEndProduct,
    ChurnRiskTiming,
    CustomerJourneyInsight
)

from app.models.user_behavior import (
    PageFriction,
    CheckoutFunnel,
    DeviceComparison,
    SessionInsight,
    UserBehaviorInsight
)

from app.models.ad_spend import (
    CampaignPerformance,
    AdSpendOptimization,
    AdWaste,
    ProductAdPerformance,
    AdSpendInsight
)

from app.models.weekly_brief import (
    WeeklyBrief,
    BriefPriority,
    BriefTrend,
    BriefWorkingWell,
    BriefWatchList,
    BriefInsight
)

from app.models.content_gap import (
    ContentGap,
    MerchandisingGap,
    ContentOpportunity,
    ContentPerformance,
    ContentInsight,
    CategoryContentHealth
)

from app.models.code_health import (
    CodeRepository,
    CodeQualityMetric,
    ThemeHealthCheck,
    SecurityVulnerability,
    CodeCommit,
    TechnicalDebt,
    CodeInsight,
    DependencyStatus
)

from app.models.redirect_health import (
    NotFoundError,
    RedirectRule,
    RedirectChain,
    LostRevenue,
    RedirectInsight,
    BrokenLink
)

# Data Connector Models

from app.models.google_ads_data import (
    GoogleAdsCampaign,
    GoogleAdsAdGroup,
    GoogleAdsProductPerformance,
    GoogleAdsSearchTerm,
    GoogleAdsClick
)

from app.models.ga4_data import (
    GA4TrafficSource,
    GA4LandingPage,
    GA4ProductPerformance,
    GA4ConversionPath,
    GA4Event,
    GA4PagePerformance
)

from app.models.search_console_data import (
    SearchConsoleQuery,
    SearchConsolePage,
    SearchConsoleIndexCoverage,
    SearchConsoleSitemap,
    SearchConsoleRichResult
)

from app.models.klaviyo_data import (
    KlaviyoCampaign,
    KlaviyoFlow,
    KlaviyoFlowMessage,
    KlaviyoSegment,
    KlaviyoProfile
)

from app.models.hotjar_data import (
    HotjarPageData,
    HotjarFunnelStep,
    HotjarRecordingSummary,
    HotjarPoll,
    ClaritySession
)

from app.models.github_data import (
    GitHubRepository,
    GitHubCommit,
    GitHubFile,
    GitHubPullRequest,
    GitHubIssue
)

from app.models.merchant_center_data import (
    MerchantCenterProductStatus,
    MerchantCenterDisapproval,
    MerchantCenterAccountStatus
)

from app.models.ml_intelligence import (
    MLForecast,
    MLAnomaly,
    MLInventorySuggestion
)

from app.models.strategic_intelligence import (
    StrategicBrief,
    BriefRecommendation,
    BriefCorrelation
)
from app.models.caprice_import import CapriceImportLog

from app.models.business_expense import (
    BusinessExpense,
    MonthlyPL
)

from app.models.shippit import ShippitOrder

from app.models.site_health import SiteHealthEvent
from app.models.user import User, UserSession
