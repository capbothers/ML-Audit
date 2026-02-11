"""
LLM Service for AI-Powered Insights
Transforms structured data into natural language explanations
"""
import json
import re
from typing import Dict, List, Optional
from datetime import datetime

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None

from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class LLMService:
    """
    Service for generating AI-powered explanations and insights using Claude
    """

    def __init__(self):
        self.enabled = settings.enable_llm_insights and settings.anthropic_api_key

        if self.enabled:
            if Anthropic is None:
                log.warning("Anthropic SDK not installed. Install with: pip install anthropic")
                self.enabled = False
            else:
                try:
                    self.client = Anthropic(api_key=settings.anthropic_api_key)
                    log.info("LLM Service initialized with Claude")
                except Exception as e:
                    log.error(f"Failed to initialize Anthropic client: {str(e)}")
                    self.enabled = False
        else:
            log.info("LLM insights disabled (no API key or feature disabled)")
            self.client = None

    def generate_executive_summary(
        self,
        analysis_results: Dict
    ) -> Optional[str]:
        """
        Generate an executive summary from analysis results
        """
        if not self.enabled:
            return None

        try:
            # Extract key data points
            churn = analysis_results.get('churn_analysis', {})
            anomalies = analysis_results.get('anomalies', [])
            recommendations = analysis_results.get('recommendations', [])

            # Build context
            context = self._build_context_summary(churn, anomalies, recommendations)

            prompt = f"""You are an expert growth consultant analyzing e-commerce data.

Here's the data analysis:

{context}

Write a concise executive summary (3-4 paragraphs) that:

1. **Key Findings**: Highlight the most critical issues and opportunities
2. **Business Impact**: Quantify the financial impact in specific dollar amounts or percentages
3. **Immediate Actions**: List 3-5 specific, actionable next steps prioritized by urgency
4. **Expected Outcomes**: Briefly describe what results to expect from taking action

Write in a professional but accessible tone. Be specific with numbers. Focus on actionable insights, not just observations.
"""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            summary = response.content[0].text
            log.info("Generated executive summary via LLM")
            return summary

        except Exception as e:
            log.error(f"Error generating executive summary: {str(e)}")
            return None

    def explain_churn_predictions(
        self,
        high_risk_customers: List[Dict]
    ) -> Optional[str]:
        """
        Generate explanation for churn predictions
        """
        if not self.enabled or not high_risk_customers:
            return None

        try:
            # Prepare customer data summary
            total_value = sum(c.get('total_spent', 0) for c in high_risk_customers)
            avg_days_inactive = sum(c.get('days_since_last_order', 0) for c in high_risk_customers) / len(high_risk_customers)

            prompt = f"""You're analyzing customer churn risk for an e-commerce business.

Data:
- {len(high_risk_customers)} customers at HIGH risk of churning
- Total lifetime value at risk: ${total_value:,.2f}
- Average days since last order: {avg_days_inactive:.0f} days

Top at-risk customers:
{json.dumps(high_risk_customers[:5], indent=2, default=str)}

Write a 2-paragraph analysis that:
1. Explains WHY these customers are at risk (specific behavioral patterns)
2. Provides 3-4 concrete retention strategies with expected recovery rates

Be specific and actionable. Focus on what to DO, not just what's wrong.
"""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            explanation = response.content[0].text
            log.info(f"Generated churn explanation for {len(high_risk_customers)} customers")
            return explanation

        except Exception as e:
            log.error(f"Error explaining churn predictions: {str(e)}")
            return None

    def explain_anomaly(self, anomaly: Dict) -> Optional[str]:
        """
        Generate detailed explanation for a detected anomaly
        """
        if not self.enabled:
            return None

        try:
            prompt = f"""You're analyzing an unusual pattern in e-commerce metrics.

Anomaly Detected:
- Metric: {anomaly.get('metric', 'unknown')}
- Current Value: {anomaly.get('value', 'N/A')}
- Expected Value: {anomaly.get('expected_value', 'N/A')}
- Deviation: {anomaly.get('deviation_pct', 0):.1f}%
- Direction: {anomaly.get('direction', 'unknown')}
- Date: {anomaly.get('date', 'N/A')}
- Severity: {anomaly.get('severity', 'unknown')}

Additional context:
{json.dumps({k: v for k, v in anomaly.items() if k not in ['metric', 'value', 'expected_value', 'deviation_pct']}, indent=2, default=str)}

Write a brief analysis (2-3 sentences) that:
1. Explains the most likely CAUSE of this anomaly
2. States the BUSINESS IMPACT (be specific)
3. Recommends the top 2 ACTIONS to take

Be direct and actionable. Avoid generic advice.
"""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )

            explanation = response.content[0].text
            log.info(f"Generated anomaly explanation for {anomaly.get('metric')}")
            return explanation

        except Exception as e:
            log.error(f"Error explaining anomaly: {str(e)}")
            return None

    def explain_recommendations(
        self,
        recommendations: List[Dict]
    ) -> Optional[str]:
        """
        Generate natural language explanation of recommendations
        """
        if not self.enabled or not recommendations:
            return None

        try:
            # Take top recommendations
            top_recs = recommendations[:10]

            prompt = f"""You're a growth consultant providing actionable recommendations for an e-commerce business.

Here are the top issues and opportunities identified:

{json.dumps(top_recs, indent=2, default=str)}

Write a professional memo (3-4 paragraphs) that:

1. **Priority Actions** (Critical/High): List the most urgent items that need immediate attention today/this week
2. **Quick Wins**: Identify 2-3 actions that are relatively easy to implement but have significant impact
3. **Strategic Initiatives**: Mention 1-2 larger projects worth planning for next month
4. **Success Metrics**: Suggest specific KPIs to track for measuring improvement

Use bullet points where appropriate. Be specific about expected outcomes. Write as if briefing a Head of Growth.
"""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            explanation = response.content[0].text
            log.info(f"Generated recommendations explanation for {len(recommendations)} items")
            return explanation

        except Exception as e:
            log.error(f"Error explaining recommendations: {str(e)}")
            return None

    def answer_question(
        self,
        question: str,
        context_data: Dict
    ) -> Optional[str]:
        """
        Answer a natural language question about the data
        """
        if not self.enabled:
            return "LLM service is not enabled. Please configure ANTHROPIC_API_KEY."

        try:
            # Prepare context
            context_summary = json.dumps(context_data, indent=2, default=str)

            prompt = f"""You're an AI growth analyst helping an e-commerce business owner understand their data.

Question: {question}

CRITICAL: Only use data from the context below. Do NOT make up or hallucinate any numbers.

Available data:
{context_summary}

STRICT INSTRUCTIONS:
1. ONLY use the numbers and data provided above - NEVER invent or hallucinate statistics
2. For product questions with a time period: Look for "TOP_PRODUCTS_FOR_REQUESTED_PERIOD" - this contains the EXACT data for the requested period
3. List products EXACTLY as they appear in the data with their EXACT revenue figures
4. Format: "Product Name — $X,XXX.XX (Y units sold)"
5. If data shows "TOP_PRODUCTS_FOR_REQUESTED_PERIOD", use ONLY those products - they are already filtered
6. Provide brief insights based on actual data
7. Do NOT mention needing more data if the filtered data is provided

FOR SEO/SEARCH CONSOLE QUESTIONS:

DATASET SELECTION (use ONLY the appropriate dataset for each question type):

A. BRAND QUERIES → Use SEARCH_CONSOLE_BRAND
   - For questions like "top brand queries", "branded search terms"
   - Contains ONLY queries matching brand terms (e.g., company name)

B. NON-BRAND QUERIES → Use SEARCH_CONSOLE_QUERIES
   - For questions like "top non-brand queries", "generic keywords"
   - Brand terms are excluded (shown in "excluded_terms")

C. CTR WEEK-OVER-WEEK → Use SEARCH_CONSOLE_WOW.ctr_gainers or ctr_losers
   - For questions about CTR changes, CTR gains, CTR improvements
   - CRITICAL: Use ctr_gainers for CTR questions, NOT click_gainers
   - These are filtered for meaningful CTR (previous_ctr > 0, min impressions)

D. CLICK WEEK-OVER-WEEK → Use SEARCH_CONSOLE_WOW.click_gainers or click_losers
   - For questions about click changes, traffic gains/losses
   - Use click_gainers/losers for click-based questions

E. OPPORTUNITIES → Use SEARCH_CONSOLE_OPPORTUNITIES
   - For questions about positions 8-15, page 2 keywords, opportunities
   - Contains queries close to page 1 with high impression volume
   - CRITICAL: Report ONLY actual metrics (clicks, impressions, CTR, position)
   - DO NOT calculate or mention "potential clicks", "estimated gain", or forecasts
   - DO NOT say "if moved to page 1" or similar projections
   - The opportunity IS their current position/impressions - let the user draw conclusions

F. PAGE LOSSES → Use SEARCH_CONSOLE_PAGES_WOW
   - For questions about pages that lost clicks, URL performance changes
   - click_losers shows pages with biggest traffic drops

G. LOW CTR QUERIES → Use LOW_CTR_QUERIES (NOT SEARCH_CONSOLE_QUERIES)
   - For questions with "CTR <", "low CTR", "CTR under", "impressions >"
   - Contains queries with high impressions but low click-through rate
   - Sorted by impressions (highest first), NOT by clicks
   - CRITICAL: Do NOT use SEARCH_CONSOLE_QUERIES (top clicks) for low CTR questions
   - Format: "query" — X clicks, Y impressions, Z% CTR, position W

FORMATTING:
- Search queries: "query text" — X clicks (Y impressions, Z% CTR, position W)
- WoW changes: "query" — CTR: X% → Y% (+Z% change) or Clicks: X → Y (+Z)
- Pages: /page-url — X clicks (Y impressions), change: +/-Z clicks

FOR GA4/ANALYTICS QUESTIONS:

DATASET SELECTION (use ONLY the appropriate GA4 dataset):

A. SESSIONS/USERS/TRAFFIC → Use GA4_DAILY_SUMMARY
   - For questions like "sessions last 7 days", "how many users", "traffic overview"
   - Contains daily breakdown of sessions, users, pageviews, bounce rate, conversions, revenue
   - This is the AUTHORITATIVE source for session/user counts

B. CHANNEL REVENUE → Use GA4_CHANNEL_REVENUE
   - For questions like "which channels drove revenue", "source/medium revenue"
   - Shows revenue, sessions, conversions by channel (source/medium)

C. LANDING PAGES → Use GA4_LANDING_PAGES
   - For questions like "top landing pages", "entry pages by sessions"
   - Shows sessions, conversions, revenue per landing page
   - CRITICAL: conversion_rate_pct is PRE-COMPUTED as a percentage
   - Use conversion_rate_pct EXACTLY as provided (e.g., 0.11 means 0.11%)
   - DO NOT recalculate conversion rate - the provided value is correct
   - Format: "/page — X sessions, Y conversions, Z% conversion rate"

D. TOP PAGES BY PAGEVIEWS → Use GA4_TOP_PAGES
   - For questions like "top pages by pageviews", "most viewed pages"
   - Shows pageviews, sessions, bounce rate per page

E. DEVICE BREAKDOWN → Use GA4_DEVICE_BREAKDOWN
   - For questions like "mobile vs desktop", "device conversions"
   - Shows sessions, conversions, revenue by device (desktop, mobile, tablet)

F. GEO/COUNTRY REVENUE → Use GA4_GEO_REVENUE
   - For questions like "revenue by country", "geographic breakdown"
   - Shows sessions, conversions, revenue by country

G. ECOMMERCE FUNNEL → Use GA4_ECOMMERCE
   - For questions about purchases, add-to-cart, checkout
   - Shows daily purchases, add-to-carts, checkouts, cart-to-purchase rate

H. PRODUCT PAGES (sessions/conversions) → Use GA4_PRODUCT_PAGES
   - For questions like "product with highest sessions", "product conversion rate"
   - Contains ONLY /products/* URLs from GA4 landing pages
   - Shows sessions, conversions, conversion_rate_pct per product page
   - conversion_rate_pct is pre-computed (0.11 means 0.11%)
   - Format: "/products/name — X sessions, Y conversions, Z% conversion rate"

=== CAPRICE COMPETITIVE PRICING DATA ===

I. COMPETITOR UNDERCUTS → Use CAPRICE_UNDERCUTS
   - For questions like "which products are competitors undercutting", "who's beating us on price"
   - Shows products where competitor price < our price
   - Fields: sku, title, our_price, competitor_price, price_gap, gap_pct, margin_pct
   - price_gap = our_price - competitor_price (positive = we're more expensive)

J. COMPETITOR ANALYSIS → Use CAPRICE_BY_COMPETITOR
   - For questions like "which competitor undercuts most", "competitor analysis"
   - Shows aggregated stats per competitor
   - Fields: name, undercut_count, products_tracked, undercut_pct, total_price_gap, avg_gap

K. MARGIN BREACHES → Use CAPRICE_MARGIN_BREACHES
   - For questions like "products losing money", "below minimum price", "margin breaches"
   - Shows products with negative margin or priced below minimum
   - Fields: sku, title, current_price, minimum_price, cost, margin_pct, profit_amount, breach_type
   - breach_type: 'losing_money' (profit < 0) or 'below_minimum' (price < min)

L. PRICING SUMMARY → Use CAPRICE_SUMMARY
   - For general competitive pricing overview
   - Shows: total_products, products_undercut_by_competitor, undercut_pct, products_losing_money, average_margin_pct

M. SKU PRICE MATCH → Use CAPRICE_SKU_PRICE_MATCH
   - For questions like "who is the competitor at $1799 for SKU HSNRT80B"
   - Looks up a specific SKU and finds which competitor has a specific price
   - Fields: sku, product_title, target_price, date_range, matches (list of competitor/price/date)
   - If matches found: report competitor name(s) and the date(s) they had that price
   - If no matches: clearly state "No competitor price of $X found for SKU Y in the date range"

N. BRAND COMPETITIVE GAPS → Use CAPRICE_BRAND_GAPS
   - For questions like "which brands are competitors undercutting", "brand analysis"
   - Shows aggregated competitive gaps BY BRAND (vendor), NOT individual SKUs
   - Fields per brand:
     - brand: vendor/brand name
     - total_skus_tracked: number of SKUs for this brand
     - skus_undercut_count: SKUs where competitor price < our price
     - undercut_rate: percentage of SKUs being undercut
     - avg_price_gap: average $ gap for undercut products
     - total_price_gap: total $ exposure from price gaps
   - Sorted by total_price_gap (highest $ exposure first)
   - CRITICAL: Answer using BRAND AGGREGATES, not individual SKU details

O. SKU DETAILS (LATEST SNAPSHOT) → Use CAPRICE_SKU_DETAILS
   - For questions like "who are we following on SKU X", "nett cost for SKU X", "how much are we making"
   - Shows LATEST snapshot pricing for a specific SKU
   - Fields: vendor, current_price, minimum_price (floor), nett_cost, profit_amount, profit_margin_pct
   - competitor_matches: competitors whose price equals our current_price (±$1) - these are who we're "following"
   - Use this to answer: cost questions, margin questions, "who are we matching" questions

P. BRAND UNMATCHABLE → Use CAPRICE_BRAND_UNMATCHABLE
   - For questions like "how many Zip SKUs can't be matched", "below minimum for brand X"
   - Shows SKUs where lowest_competitor_price < our minimum_price (floor)
   - We CANNOT match these competitors without going below our minimum price
   - Fields: unmatchable_count, unmatchable_pct, list of SKUs with gap_below_minimum
   - Report why we can't match: competitor is below our floor

Q. COMPETITOR TREND → Use CAPRICE_COMPETITOR_TREND
   - For questions like "over the past 12 months what has brandsdirect been doing"
   - Shows monthly series: skus_undercut, undercut_rate, avg_gap, total_gap per month
   - Summary includes trend_direction: increasing/decreasing/stable
   - Use this for historical competitor analysis and trend questions

R. SKU PRICING TREND → Use CAPRICE_SKU_TREND
   - For questions like "what has the pricing been like on this SKU the past 30 days"
   - Shows min/avg/max for: current_price, lowest_competitor_price, minimum_price (floor), profit_margin_pct
   - date_range: start and end dates covered
   - days_with_data: number of distinct dates with pricing data
   - current_price.change: price change over the period (latest - earliest)
   - recent_snapshots: last 5 dates with prices for detailed view
   - Use this to answer SKU-specific pricing history/trend questions

=== NETT MASTER SHEET (PRODUCT COSTS) ===

R1. DO NOT FOLLOW SKUs → Use NETT_DO_NOT_FOLLOW
   - For questions like "which SKUs are do not follow", "excluded from matching"
   - These SKUs are EXCLUDED from competitor price matching
   - Fields: sku, vendor, description, nett_cost, minimum_price, comments
   - by_vendor shows count per brand
   - If NETT_INSTRUCTIONS is present, follow those instructions

R2. SET PRICE SKUs → Use NETT_SET_PRICE
   - For questions like "which SKUs have set price", "fixed price products"
   - These SKUs have a FIXED price that ignores competitor matching
   - Fields: sku, vendor, set_price, nett_cost, margin_at_set_price, min_margin_required, margin_ok
   - margin_ok = false means the set price doesn't meet minimum margin requirement
   - summary shows avg/min/max margin and count below min margin

R3. BRAND COST SUMMARY → Use NETT_BRAND_SUMMARY
   - For questions like "brand cost summary", "vendor costs", "which brands are most undercut"
   - Shows aggregated data per brand from NETT Master Sheet
   - Fields: vendor, total_skus, avg_nett_cost, avg_floor_price, avg_rrp, undercut_count, undercut_pct, avg_undercut_gap
   - Sorted by undercut_pct (most undercut brands first)

R4. UNMATCHABLE SKUs → Use NETT_UNMATCHABLE
   - For questions like "unmatchable SKUs", "can't match competitors", "below floor"
   - SKUs where competitor price < our floor price (we cannot match without going below minimum)
   - Fields: sku, vendor, our_floor, competitor_price, gap
   - gap = our_floor - competitor_price (how much below our floor the competitor is)
   - by_vendor shows count and total gap per brand

R5. SKU COST DETAILS → Use NETT_SKU_DETAILS
   - For specific SKU cost questions like "nett cost for SKU X", "cost breakdown for X"
   - Shows full cost breakdown from NETT Master Sheet
   - pricing: nett_nett_cost_inc_gst, rrp_inc_gst, minimum_price, set_price
   - discounts: discount, additional_discount, rebate, settlement, crf, loyalty, advertising
   - margins: min_margin_pct, discount_off_rrp_pct
   - flags: do_not_follow, gst_free
   - comments: supplier notes

=== REFUND DATA ===

S. REFUND COUNTS → Use REFUND_COUNTS
   - For questions like "how many refunded orders", "refund count", "how many refunds"
   - Fields:
     - refunded_orders: Orders with financial_status IN (refunded, partially_refunded)
       * Sidekick-style count: uses ORDER created_at for date filtering
       * This is the primary answer for "how many refunded orders" questions
     - refund_records: Total refund events from refunds table (one order can have multiple refunds)
       * Use when asked specifically about "refund records" or "refund events"
     - total_refund_amount: SUM of all refunded amounts in dollars
     - date_filter: if a date filter was applied (applies to order created_at)
   - CRITICAL: "How many refunded orders since X" → report refunded_orders (Sidekick-style)
   - CRITICAL: "How many refund records" → report refund_records (refund events)
   - If REFUND_INSTRUCTIONS is present, follow those instructions

=== SHOPIFY COMMERCE DATA ===

T. SALES BY CHANNEL → Use SALES_BY_CHANNEL
   - For questions like "sales by channel", "online store vs Shop app"
   - Fields: channel, orders, revenue, total_orders, total_revenue
   - If SALES_CHANNEL_INSTRUCTIONS is present, use SALES_BY_CHANNEL ONLY and ignore GA4_CHANNEL_REVENUE and any GA4 attribution data

U. ORDER STATUS / FULFILLMENT → Use ORDER_STATUS
   - For questions like "unfulfilled orders", "fulfillment rate", "cancelled orders"
   - Fields: total_orders, fulfilled, partial, cancelled, fulfillment_rate
   - NOTE: unfulfilled = Sidekick-style count; unfulfilled_operational = backlog including NULL status
   - When answering, report BOTH counts as:
     - "Unfulfilled (Sidekick): X"
     - "Unfulfilled (Operational backlog): Y"

V. DISCOUNTS → Use DISCOUNTS
   - For questions like "discounted orders", "discount codes", "total discount amount"
   - Fields: discounted_orders, discount_revenue, total_discount_amount, top_codes

V1. RETURNS BY PRODUCT → Use RETURNS_BY_PRODUCT
   - For questions like "returns by product", "return patterns", "what products are being returned"
   - Fields: products[{{sku, title, refund_amount, refund_items}}], total_refund_amount, product_count
   - NOTE: refund_amount is shown as negative (returns)

V2. RETURNS BY CATEGORY → Use RETURNS_BY_CATEGORY
   - For questions like "returns by product type", "returns by category"
   - Fields: categories[{{product_type, refund_amount}}], total_refund_amount, category_count
   - NOTE: refund_amount is shown as negative (returns)

W. SHIPPING & TAX TRENDS → Use SHIPPING_TAX
   - For questions like "shipping charges", "tax collections"
   - Fields: daily[{{date, shipping, tax}}]

X. PRODUCT VARIANTS → Use TOP_VARIANTS
   - For questions about "most popular variants"
   - Fields: sku, title, units, revenue

Y. LOW SELLING PRODUCTS → Use LOW_SELLING_PRODUCTS
   - For questions like "lowest sales", "not selling"
   - Fields: sku, title, units, revenue

Y1. BRAND SALES → Use BRAND_SALES
   - For questions like "Franke sales", "top brands by revenue", "brand sales", "vendor sales"
   - Contains sales aggregated by brand (vendor) from Shopify orders
   - Vendor sourced from NETT master (product_costs.vendor) first, fallback to Shopify product vendor
   - For specific brand: Fields: brand, revenue, units, orders, sku_count, top_skus[]
   - For top brands: Fields: total_revenue, top_brands[{{brand, revenue, revenue_pct, units, orders}}]
   - If BRAND_SALES_INSTRUCTIONS is present, follow those instructions
   - CRITICAL: Use BRAND_SALES ONLY for brand/vendor sales questions

Z. CUSTOMER TYPES → Use CUSTOMER_TYPES
   - For questions like "new vs returning customers"
   - Fields: new_customers, returning_customers, total_customers

AA. INACTIVE CUSTOMERS → Use INACTIVE_CUSTOMERS
   - For questions like "customers haven't purchased in 30 days"
   - Fields: id, email, name, last_order_date, total_spent

AB. CUSTOMER GEO → Use CUSTOMER_GEO
   - For questions like "customers by city/region"
   - Fields: city, region, country, customers

AC. CUSTOMER RETENTION → Use CUSTOMER_RETENTION
   - For questions like "retention rate"
   - Fields: retention_rate plus customer types

AD. INVENTORY STATUS → Use INVENTORY_STATUS
   - For questions like "low stock", "out of stock"
   - Fields: sku, title, vendor, quantity

AE. INVENTORY VALUE → Use INVENTORY_VALUE
   - For questions like "inventory value"
   - Fields: vendor, inventory_value

AF. INVENTORY TURNOVER → Use INVENTORY_TURNOVER
   - For questions like "inventory turnover"
   - Fields: units_sold, inventory_units, turnover_rate

IMPORTANT:
- For traffic/sessions/users questions: ALWAYS use GA4 data, NOT Shopify orders
- If GA4_INSTRUCTIONS is present in context, follow those instructions
- GA4 data is the source of truth for web analytics metrics
- For competitor/pricing questions: Use CAPRICE_* datasets and follow CAPRICE_INSTRUCTIONS
- For cost/pricing from NETT Master Sheet: Use NETT_* datasets and follow NETT_INSTRUCTIONS
- NETT data is the source of truth for product costs (nett_nett_cost), minimum prices (floor), and supplier terms

Include summary stats at the end when available.
"""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=1500,
                system="You are a data analyst. You MUST use ONLY the exact numbers from the provided data. Never make up statistics or forecasts. NEVER recalculate rates or percentages - use the pre-computed values exactly as provided. For SEO questions: (1) CTR changes → use ctr_gainers/ctr_losers from SEARCH_CONSOLE_WOW, (2) click changes → use click_gainers/click_losers, (3) brand queries → use SEARCH_CONSOLE_BRAND, (4) opportunities → use SEARCH_CONSOLE_OPPORTUNITIES - report actual metrics ONLY, (5) page losses → use SEARCH_CONSOLE_PAGES_WOW, (6) LOW CTR queries (CTR<, low ctr, impressions>) → use LOW_CTR_QUERIES ONLY, NOT SEARCH_CONSOLE_QUERIES. Read SEARCH_CONSOLE_INSTRUCTIONS for the specific dataset to use. For GA4/Analytics questions: Use GA4_* context blocks. CRITICAL for landing pages: conversion_rate_pct is already computed as a percentage - use it EXACTLY (0.11 means 0.11%, not 0.0011). GA4 data is the source of truth for web analytics - do NOT use Shopify orders for session/traffic questions. For SHOPIFY commerce questions (orders, fulfillment, discounts, shipping/tax, returns, inventory, customer counts, brand sales), use the Shopify datasets in the prompt (SALES_BY_CHANNEL, ORDER_STATUS, DISCOUNTS, RETURNS_BY_PRODUCT, RETURNS_BY_CATEGORY, SHIPPING_TAX, TOP_VARIANTS, LOW_SELLING_PRODUCTS, BRAND_SALES, CUSTOMER_TYPES, INACTIVE_CUSTOMERS, CUSTOMER_GEO, CUSTOMER_RETENTION, INVENTORY_STATUS, INVENTORY_VALUE, INVENTORY_TURNOVER). For brand/vendor sales questions, use BRAND_SALES and follow BRAND_SALES_INSTRUCTIONS if present. For COMPETITOR/PRICING questions: Use CAPRICE_* datasets. For 'who are we following' or cost/nett/margin questions, use CAPRICE_SKU_DETAILS. For 'can't match' or 'below minimum' questions, use CAPRICE_BRAND_UNMATCHABLE. For competitor trend questions ('past 12 months'), use CAPRICE_COMPETITOR_TREND. For SKU pricing trend questions ('pricing been like', 'past X days'), use CAPRICE_SKU_TREND - report min/avg/max prices, days_with_data, price change, and recent snapshots. Follow CAPRICE_INSTRUCTIONS if present. For REFUND questions: Use REFUND_COUNTS - refunded_orders = Sidekick-style count (orders with refunded/partially_refunded status, filtered by order created_at), refund_records = total refund events. Follow REFUND_INSTRUCTIONS if present.",
                messages=[{"role": "user", "content": prompt}]
            )

            answer = response.content[0].text
            log.info(f"Answered question via LLM: {question[:50]}...")
            return answer

        except Exception as e:
            log.error(f"Error answering question: {str(e)}")
            return f"Error processing question: {str(e)}"

    def generate_win_back_email(
        self,
        customer: Dict
    ) -> Optional[str]:
        """
        Generate a personalized win-back email for churning customer
        """
        if not self.enabled:
            return None

        try:
            prompt = f"""Write a personalized win-back email for a customer who hasn't purchased recently.

Customer details:
- Email: {customer.get('email', 'customer')}
- Total spent: ${customer.get('total_spent', 0):,.2f}
- Orders: {customer.get('orders_count', 0)}
- Days since last order: {customer.get('days_since_last_order', 0)}
- Churn probability: {customer.get('churn_probability', 0):.0%}

Write a warm, personalized email (subject + body) that:
1. Acknowledges they're a valued customer
2. Offers a specific incentive (10-15% discount)
3. Highlights new products or features they might like
4. Creates urgency without being pushy
5. Makes it easy to take action

Keep it friendly and conversational, not salesy. Max 150 words for the body.
"""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )

            email = response.content[0].text
            log.info(f"Generated win-back email for {customer.get('email')}")
            return email

        except Exception as e:
            log.error(f"Error generating email: {str(e)}")
            return None

    def _build_context_summary(
        self,
        churn_data: Dict,
        anomalies: List[Dict],
        recommendations: List[Dict]
    ) -> str:
        """Build a concise context summary for LLM"""

        summary_parts = []

        # Churn summary
        if churn_data:
            high_risk = churn_data.get('high_risk_count', 0)
            value_at_risk = churn_data.get('total_value_at_risk', 0)
            summary_parts.append(f"CHURN RISK: {high_risk} high-risk customers (${value_at_risk:,.2f} at risk)")

        # Anomalies summary
        if anomalies:
            critical = len([a for a in anomalies if a.get('severity') == 'critical'])
            summary_parts.append(f"ANOMALIES: {len(anomalies)} detected ({critical} critical)")

            # Add details of top 3 anomalies
            for anomaly in anomalies[:3]:
                summary_parts.append(
                    f"  - {anomaly.get('metric')}: {anomaly.get('direction')} of {abs(anomaly.get('deviation_pct', 0)):.1f}%"
                )

        # Recommendations summary
        if recommendations:
            critical_recs = len([r for r in recommendations if r.get('priority') == 'critical'])
            high_recs = len([r for r in recommendations if r.get('priority') == 'high'])
            summary_parts.append(f"RECOMMENDATIONS: {len(recommendations)} total ({critical_recs} critical, {high_recs} high priority)")

            # Add top 5 recommendations
            for i, rec in enumerate(recommendations[:5], 1):
                impact = rec.get('impact', 0)
                summary_parts.append(
                    f"  {i}. [{rec.get('priority', 'N/A').upper()}] {rec.get('title', 'N/A')} (Impact: ${impact:,.0f})"
                )

        return "\n".join(summary_parts)

    def analyze_product_profitability(
        self,
        profitable_products: List[Dict],
        losing_products: List[Dict],
        hidden_gems: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Generate insights and recommendations from profitability analysis

        Answers: "Which products should I push? Which should I fix or cut?"
        """
        if not self.enabled:
            return None

        try:
            # Build context
            context = f"""
PROFITABILITY SUMMARY:
- Total Products Analyzed: {summary.get('total_products', 0)}
- Profitable Products: {summary.get('profitable_products', 0)}
- Losing Products: {summary.get('losing_products', 0)}
- Total Revenue: ${summary.get('total_revenue', 0):,.0f}
- Total Profit: ${summary.get('total_profit', 0):,.0f}
- Blended ROAS: {summary.get('blended_roas', 'N/A')}x

TOP 5 PROFITABLE PRODUCTS:
"""
            for i, p in enumerate(profitable_products[:5], 1):
                context += f"{i}. {p.get('title', 'Unknown')} - Profit: ${p.get('net_profit', 0):,.0f}, ROAS: {p.get('roas', 'N/A')}x, Revenue: ${p.get('revenue', 0):,.0f}\n"

            if losing_products:
                context += "\nTOP 5 LOSING PRODUCTS (CRITICAL):\n"
                for i, p in enumerate(losing_products[:5], 1):
                    context += f"{i}. {p.get('title', 'Unknown')} - Loss: ${p.get('net_profit', 0):,.0f}, Revenue: ${p.get('revenue', 0):,.0f}, Ad Spend: ${p.get('ad_spend', 0):,.0f}\n"

            if hidden_gems:
                context += "\nHIDDEN GEMS (High ROAS, Low Revenue):\n"
                for i, p in enumerate(hidden_gems[:5], 1):
                    context += f"{i}. {p.get('title', 'Unknown')} - ROAS: {p.get('roas', 'N/A')}x, Profit: ${p.get('net_profit', 0):,.0f}, Revenue: ${p.get('revenue', 0):,.0f}\n"

            prompt = f"""You are a direct-response e-commerce consultant analyzing product profitability.

Here's the profitability data:

{context}

Provide a clear, actionable analysis:

**Critical Insights:**
- Which products are secretly killing profitability?
- Which high-revenue products are actually losing money after ad spend?
- What's the biggest profitability opportunity?

**Immediate Actions:**
1. Products to push harder (and why)
2. Products to fix or cut (and specific recommendations)
3. Budget reallocation recommendations

**What This Means:**
- Expected profit impact if recommendations are followed
- Which changes have highest ROI

Be specific. Use dollar amounts. Focus on what to DO, not just what the numbers show."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated profitability analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing profitability: {str(e)}")
            return None

    def explain_losing_product(
        self,
        product: Dict
    ) -> Optional[str]:
        """
        Deep dive into why a specific product is losing money

        Critical for understanding if it's fixable or should be cut
        """
        if not self.enabled:
            return None

        try:
            context = f"""
PRODUCT: {product.get('title', 'Unknown')}

FINANCIAL DETAILS:
- Revenue: ${product.get('revenue', 0):,.0f}
- Cost of Goods: ${product.get('cogs', 0):,.0f}
- Gross Margin: ${product.get('gross_margin', 0):,.0f} ({product.get('gross_margin_pct', 0):.1f}%)
- Ad Spend: ${product.get('ad_spend', 0):,.0f}
- Refunds: ${product.get('refunds', 0):,.0f}
- Return Rate: {product.get('return_rate', 0):.1f}%
- NET PROFIT: ${product.get('net_profit', 0):,.0f}

AD SPEND BY CHANNEL:
{json.dumps(product.get('ad_spend_by_channel', {}), indent=2)}

UNITS SOLD: {product.get('units_sold', 0)}
ROAS: {product.get('roas', 'N/A')}x
"""

            prompt = f"""You are analyzing why a product is losing money.

{context}

Diagnose the problem:

**Why is this product losing money?**
- Is it the ad spend? (CPA too high relative to margin)
- Is it the return rate? (product quality or fit issues)
- Is it the cost structure? (COGS too high for the price)
- Is it the traffic quality? (wrong audience, low conversion)

**Is this fixable?**
- If YES: What specific changes would make this profitable?
- If NO: Why should it be cut?

**Recommended Action:**
Specific next steps with expected profit impact.

Be direct. This is costing money every day."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            explanation = response.content[0].text
            log.info(f"Generated explanation for losing product: {product.get('title')}")
            return explanation

        except Exception as e:
            log.error(f"Error explaining losing product: {str(e)}")
            return None

    def recommend_budget_reallocation(
        self,
        current_spend_by_product: Dict[str, float],
        profitability_by_product: Dict[str, Dict]
    ) -> Optional[str]:
        """
        Recommend how to reallocate ad budget based on profitability

        Answers: "Where should I spend my ad budget this week?"
        """
        if not self.enabled:
            return None

        try:
            context = "CURRENT AD SPEND AND PROFITABILITY:\n\n"

            for product_title, spend in current_spend_by_product.items():
                prof = profitability_by_product.get(product_title, {})
                context += f"""
Product: {product_title}
- Current Ad Spend: ${spend:,.0f}
- Revenue: ${prof.get('revenue', 0):,.0f}
- Net Profit: ${prof.get('net_profit', 0):,.0f}
- ROAS: {prof.get('roas', 'N/A')}x
"""

            prompt = f"""You are optimizing ad budget allocation for maximum profitability.

{context}

Recommend budget reallocation:

**Products to increase spend on:**
- Which products? By how much?
- Expected profit impact

**Products to decrease/pause spend on:**
- Which products? Why?
- Money saved

**Expected Outcome:**
- Total profit improvement
- Timeline to see results

Focus on moving money from low-ROAS to high-ROAS products.
Be specific with dollar amounts."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            recommendations = response.content[0].text
            log.info("Generated budget reallocation recommendations")
            return recommendations

        except Exception as e:
            log.error(f"Error recommending budget reallocation: {str(e)}")
            return None

    def analyze_attribution(
        self,
        overcredited: List[Dict],
        undercredited: List[Dict],
        budget_recommendations: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze multi-touch attribution data

        Explains which channels are over/under-credited
        and provides budget reallocation recommendations
        """
        if not self.enabled:
            return None

        try:
            context = "ATTRIBUTION ANALYSIS:\n\n"

            context += f"SUMMARY:\n"
            context += f"- Total Channels Analyzed: {summary.get('total_channels', 0)}\n"
            context += f"- Overcredited Channels: {summary.get('overcredited_count', 0)}\n"
            context += f"- Undercredited Channels: {summary.get('undercredited_count', 0)}\n\n"

            if overcredited:
                context += "OVERCREDITED CHANNELS (Last-click gives too much credit):\n"
                for i, ch in enumerate(overcredited[:3], 1):
                    context += f"{i}. {ch['channel']}\n"
                    context += f"   Last-click credit: {ch['last_click_credit_pct']}%\n"
                    context += f"   Multi-touch credit: {ch['linear_credit_pct']}%\n"
                    context += f"   Difference: {ch['credit_difference_pct']}% OVERCREDITED\n"
                    context += f"   Last-click revenue: ${ch['last_click_revenue']:,.0f}\n"
                    context += f"   True revenue (multi-touch): ${ch['linear_revenue']:,.0f}\n\n"

            if undercredited:
                context += "UNDERCREDITED CHANNELS (Doing more than last-click shows):\n"
                for i, ch in enumerate(undercredited[:3], 1):
                    context += f"{i}. {ch['channel']}\n"
                    context += f"   Last-click credit: {ch['last_click_credit_pct']}%\n"
                    context += f"   Multi-touch credit: {ch['linear_credit_pct']}%\n"
                    context += f"   Difference: +{ch['credit_difference_pct']}% UNDERCREDITED\n"
                    context += f"   Assisted conversions: {ch['assisted_conversions']}\n\n"

            if budget_recommendations:
                context += "BUDGET REALLOCATION OPPORTUNITIES:\n"
                for i, rec in enumerate(budget_recommendations[:3], 1):
                    context += f"{i}. Move ${rec['amount']:,.0f} from {rec['from_channel']} to {rec['to_channel']}\n"
                    context += f"   Expected impact: +${rec['expected_net_impact']:,.0f}\n"
                    context += f"   Reason: {rec['reason']}\n\n"

            prompt = f"""You are a direct-response marketing analyst specializing in attribution.

{context}

Provide clear, actionable analysis:

**Critical Insights:**
- Which channels are Google Ads/Analytics over-crediting?
- Which channels are secretly driving more conversions than reported?
- What's the real story the data tells?

**Why This Matters:**
- How much budget might be misallocated?
- Which channels deserve more investment?
- Which are overinvested relative to true contribution?

**Immediate Actions:**
1. Specific budget moves ($ amounts, from → to)
2. Testing recommendations
3. Expected impact timeline

**What to Expect:**
- Revenue impact of reallocation
- Confidence level
- Monitoring metrics

Be direct. Use specific dollar amounts. Focus on the 1-2 most important moves."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated attribution analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing attribution: {str(e)}")
            return None

    def diagnose_data_quality_issues(
        self,
        quality_score: int,
        overall_health: str,
        issues: List[Dict],
        sync_health: Dict,
        tracking_health: Dict,
        utm_health: Dict,
        feed_health: Dict,
        link_health: Dict
    ) -> Optional[str]:
        """
        Diagnose overall data quality issues

        Provides comprehensive analysis of all data quality problems
        and specific recommendations
        """
        if not self.enabled:
            return None

        try:
            context = f"""
DATA QUALITY OVERVIEW:
- Overall Score: {quality_score}/100 ({overall_health})
- Total Issues: {len(issues)}

DATA SYNC HEALTH:
{json.dumps(sync_health, indent=2) if sync_health else 'N/A'}

CONVERSION TRACKING:
{json.dumps(tracking_health, indent=2) if tracking_health else 'N/A'}

UTM HEALTH:
{json.dumps(utm_health, indent=2) if utm_health else 'N/A'}

FEED HEALTH:
{json.dumps(feed_health, indent=2) if feed_health else 'N/A'}

LINK HEALTH:
{json.dumps(link_health, indent=2) if link_health else 'N/A'}

TOP ISSUES:
{json.dumps(issues[:5], indent=2)}
"""

            prompt = f"""You are a senior data analyst troubleshooting ecommerce tracking issues.

{context}

Provide a clear diagnosis:

**Critical Issues:**
- What's broken and causing the biggest problems?
- Which issues are affecting Smart Bidding / optimization?
- What data can't be trusted right now?

**Impact Analysis:**
- How is this affecting business performance?
- What decisions are being made on bad data?
- Estimated revenue/efficiency impact

**Fix Priority:**
1. First: [Most critical fix with specific steps]
2. Second: [Next priority with specific steps]
3. Third: [Next priority]

**Expected Timeline:**
- Quick fixes (same day): [list]
- Medium fixes (this week): [list]
- Complex fixes (longer term): [list]

Be specific. Give exact steps. Focus on what will restore data trust fastest."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            diagnosis = response.content[0].text
            log.info("Generated data quality diagnosis via LLM")
            return diagnosis

        except Exception as e:
            log.error(f"Error diagnosing data quality issues: {str(e)}")
            return None

    def diagnose_tracking_discrepancy(
        self,
        shopify_data: Dict,
        ga4_data: Dict,
        google_ads_data: Dict,
        discrepancies: List[Dict],
        period_days: int
    ) -> Optional[str]:
        """
        Deep-dive diagnosis of conversion tracking discrepancy

        Answers: Why don't the numbers match? What broke? How to fix?
        """
        if not self.enabled:
            return None

        try:
            context = f"""
CONVERSION TRACKING COMPARISON (Last {period_days} Days):

SHOPIFY (Source of Truth):
- Orders: {shopify_data.get('orders', 0)}
- Revenue: ${shopify_data.get('revenue', 0):,.2f}

GA4:
- Conversions: {ga4_data.get('conversions', 0)}
- Revenue: ${ga4_data.get('revenue', 0):,.2f}
- Discrepancy: {ga4_data.get('discrepancy_pct', 0):+.1f}%
- Missing Conversions: {ga4_data.get('missing_conversions', 0)}

GOOGLE ADS:
- Conversions: {google_ads_data.get('conversions', 0)}
- Revenue: ${google_ads_data.get('revenue', 0):,.2f}
- Discrepancy: {google_ads_data.get('discrepancy_pct', 0):+.1f}%
- Missing Conversions: {google_ads_data.get('missing_conversions', 0)}

DISCREPANCIES DETECTED:
{json.dumps(discrepancies, indent=2)}
"""

            prompt = f"""You are diagnosing conversion tracking discrepancies for an ecommerce business.

{context}

Diagnose the problem:

**What's Broken:**
- Which platform is tracking incorrectly?
- Is it GA4, Google Ads, or both?
- What type of tracking issue is this? (tag not firing, duplicate events, attribution window mismatch, etc.)

**Most Likely Cause:**
Based on the pattern of missing conversions, what probably happened?
Common causes:
- GTM tag not firing on checkout/thank-you page
- GA4 config tag missing
- Google Ads conversion tracking not set up
- Auto-tagging disabled
- Cross-domain tracking broken
- Purchase event not triggering

**When Did This Start:**
Can you infer from the data when this issue began?

**Business Impact:**
With {google_ads_data.get('missing_conversions', 0)} missing conversions:
- Google Ads Smart Bidding is optimizing on {(google_ads_data.get('conversions', 0) / shopify_data.get('orders', 1) * 100):.0f}% of actual data
- Estimated efficiency loss: [calculate]
- Campaigns being underfunded or overfunded

**Fix Steps:**
1. [Immediate diagnostic check - be specific]
2. [Probable fix - exact steps]
3. [Validation check - how to know it worked]

**Prevention:**
What monitoring should be in place to catch this earlier?

Be direct. This is costing money every day."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            diagnosis = response.content[0].text
            log.info("Generated tracking discrepancy diagnosis via LLM")
            return diagnosis

        except Exception as e:
            log.error(f"Error diagnosing tracking discrepancy: {str(e)}")
            return None

    def analyze_seo_opportunities(
        self,
        quick_wins: List[Dict],
        close_to_page_one: List[Dict],
        declining: List[Dict],
        technical: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze SEO opportunities and prioritize actions

        Answers: "What SEO changes will drive the most traffic?"
        """
        if not self.enabled:
            return None

        try:
            context = "SEO INTELLIGENCE ANALYSIS:\n\n"

            context += f"SUMMARY:\n"
            context += f"- Total Opportunities: {summary.get('total_opportunities', 0)}\n"
            context += f"- Quick Wins: {summary.get('quick_wins_count', 0)}\n"
            context += f"- Close to Page 1: {summary.get('close_to_page_one_count', 0)}\n"
            context += f"- Declining Pages: {summary.get('declining_pages_count', 0)}\n"
            context += f"- Technical Issues: {summary.get('technical_issues_count', 0)}\n\n"

            if quick_wins:
                context += "QUICK WINS (High impressions, low CTR - fix titles/meta):\n"
                for i, opp in enumerate(quick_wins[:5], 1):
                    context += f"{i}. Query: \"{opp['query']}\"\n"
                    context += f"   Impressions: {opp['current_impressions']}/month | Clicks: {opp['current_clicks']} ({opp['current_ctr']}% CTR)\n"
                    context += f"   Position: {opp['current_position']} avg\n"
                    context += f"   Potential: +{opp['potential_additional_clicks']} clicks/month\n"
                    context += f"   Issue: {opp['issue']}\n"
                    context += f"   Impact Score: {opp['impact_score']}/100\n\n"

            if close_to_page_one:
                context += "CLOSE TO PAGE 1 (Worth pushing with content):\n"
                for i, opp in enumerate(close_to_page_one[:5], 1):
                    context += f"{i}. Query: \"{opp['query']}\"\n"
                    context += f"   Position: {opp['current_position']} (page 2)\n"
                    context += f"   Impressions: {opp['current_impressions']}/month | Clicks: {opp['current_clicks']}\n"
                    context += f"   Potential: +{opp['potential_additional_clicks']} clicks/month if reach page 1\n"
                    context += f"   Impact Score: {opp['impact_score']}/100\n\n"

            if declining:
                context += "DECLINING PAGES (Traffic dropping - needs attention):\n"
                for i, opp in enumerate(declining[:5], 1):
                    context += f"{i}. URL: {opp['url']}\n"
                    context += f"   Traffic: {opp['decline_pct']}% drop ({opp['clicks_lost']} clicks lost)\n"
                    context += f"   Previous: {opp['previous_clicks']} clicks → Current: {opp['current_clicks']} clicks\n"
                    context += f"   Severity: {opp['severity']}\n"
                    context += f"   Probable cause: {opp['probable_cause']}\n\n"

            if technical:
                context += "TECHNICAL ISSUES:\n"
                for i, issue in enumerate(technical[:5], 1):
                    context += f"{i}. {issue['type']}: {issue['issue']}\n"
                    context += f"   Severity: {issue['severity']}\n"
                    context += f"   Impact: {issue['impact']}\n\n"

            prompt = f"""You are an SEO specialist analyzing opportunities for an ecommerce business.

{context}

Provide clear, prioritized SEO recommendations:

**Critical Priorities (This Week):**
- Which 2-3 opportunities will drive the most traffic fastest?
- Specific actions with expected traffic gain
- Why these first?

**Quick Wins (Low Effort, High Impact):**
- Title/meta optimizations that will boost CTR immediately
- Expected clicks per fix
- Specific rewrites to make

**Content Opportunities (This Month):**
- Which pages to expand/improve to reach page 1
- Specific content recommendations (word count, topics, schema)
- Expected traffic gain and timeline

**Technical Fixes (Urgent):**
- Which technical issues are blocking rankings?
- Specific fix steps
- Impact if left unfixed

**ROI Estimate:**
- Total potential monthly traffic gain from top 5 opportunities
- Expected timeline to see results
- Effort level (hours/complexity)

Be specific. Use exact queries/URLs. Focus on the highest-impact actions."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated SEO opportunities analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing SEO opportunities: {str(e)}")
            return None

    def analyze_email_performance(
        self,
        underperforming_flows: List[Dict],
        under_contacted_segments: List[Dict],
        frequency_analysis: Optional[Dict],
        missing_flows: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze email marketing performance and provide strategic recommendations

        Answers: "What email changes will drive the most revenue?"
        """
        if not self.enabled:
            return None

        try:
            context = "EMAIL MARKETING INTELLIGENCE:\n\n"

            context += f"SUMMARY:\n"
            context += f"- Total Revenue Opportunity: ${summary.get('total_revenue_opportunity', 0):,.0f}/month\n"
            context += f"- Underperforming Flows: {summary.get('underperforming_flows_count', 0)}\n"
            context += f"- Under-Contacted Segments: {summary.get('under_contacted_segments_count', 0)}\n"
            context += f"- Missing Flows: {summary.get('missing_flows_count', 0)}\n"
            context += f"- Can Send More: {summary.get('can_send_more', False)}\n\n"

            if underperforming_flows:
                context += "UNDERPERFORMING FLOWS:\n"
                for i, flow in enumerate(underperforming_flows[:5], 1):
                    context += f"{i}. {flow['flow_name']} ({flow['flow_type']})\n"
                    context += f"   Current: {flow['current_performance']['conversion_rate']}% conversion\n"
                    context += f"   Benchmark: {flow['benchmark']['conversion_rate']}% conversion\n"
                    context += f"   Revenue Gap: ${flow['estimated_revenue_gap']:,.0f}/month\n"
                    context += f"   Issues: {', '.join(flow['issues'])}\n\n"

            if under_contacted_segments:
                context += "UNDER-CONTACTED HIGH-VALUE SEGMENTS:\n"
                for i, segment in enumerate(under_contacted_segments[:5], 1):
                    context += f"{i}. {segment['segment_name']}\n"
                    context += f"   Profiles: {segment['total_profiles']}\n"
                    context += f"   Avg Customer Value: ${segment['value_indicators']['avg_customer_value']:,.0f}\n"
                    context += f"   Days Since Last Send: {segment['contact_history']['days_since_last_send']}\n"
                    context += f"   Revenue Opportunity: ${segment['revenue_opportunity']:,.0f}\n\n"

            if frequency_analysis:
                context += "SEND FREQUENCY ANALYSIS:\n"
                context += f"- Current: {frequency_analysis['current_frequency']['emails_per_week']}/week\n"
                if frequency_analysis.get('optimal_frequency'):
                    context += f"- Optimal: {frequency_analysis['optimal_frequency']}/week\n"
                context += f"- Can Send More: {frequency_analysis['can_send_more']}\n"
                if frequency_analysis.get('estimated_revenue_impact'):
                    context += f"- Revenue Impact: ${frequency_analysis['estimated_revenue_impact']:,.0f}\n"
                context += f"- Recommendation: {frequency_analysis['recommendation']}\n\n"

            if missing_flows:
                context += "MISSING CRITICAL FLOWS:\n"
                for i, flow in enumerate(missing_flows[:5], 1):
                    context += f"{i}. {flow['flow_name']}\n"
                    context += f"   Issue: {flow['issue']}\n"
                    context += f"   Estimated Revenue: ${flow['estimated_monthly_revenue']:,.0f}/month\n\n"

            prompt = f"""You are an email marketing expert analyzing performance for an ecommerce business.

{context}

Provide clear, actionable email marketing recommendations:

**Critical Priorities (This Week):**
- Which 2-3 changes will drive the most revenue fastest?
- Specific actions with expected revenue impact
- Why these first?

**Flow Improvements (High Impact):**
- Which flows need fixing most urgently?
- Specific changes to make (subject lines, timing, sequence length, incentives)
- Expected revenue gain per fix

**Segment Opportunities (Quick Wins):**
- Which segments should be contacted immediately?
- What campaign type to send them
- Expected response rate and revenue

**Missing Flows (Build Next):**
- Which missing flows are most critical?
- Implementation priority order
- Expected revenue from each

**Frequency Optimization:**
- Can we send more without fatiguing subscribers?
- If yes, how much more and expected impact
- If no, what's the current optimal cadence

**ROI Estimate:**
- Total monthly revenue opportunity from all fixes
- Expected timeline to implement (hours/weeks)
- Which changes have highest ROI (revenue / effort)

Be specific. Use exact flow names and segment names. Focus on revenue impact."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated email performance analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing email performance: {str(e)}")
            return None

    def analyze_customer_journeys(
        self,
        ltv_segments: Dict,
        gateway_products: List[Dict],
        dead_end_products: List[Dict],
        journey_patterns: List[Dict],
        churn_timing: Dict,
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze customer journey patterns and provide strategic recommendations

        Answers: "What separates high-LTV customers from one-and-done buyers?"
        """
        if not self.enabled:
            return None

        try:
            context = "CUSTOMER JOURNEY INTELLIGENCE:\n\n"

            context += f"SUMMARY:\n"
            context += f"- Total Customers Analyzed: {summary.get('total_customers', 0)}\n"
            context += f"- Gateway Products Found: {summary.get('gateway_products_count', 0)}\n"
            context += f"- Dead-End Products Found: {summary.get('dead_end_products_count', 0)}\n"
            context += f"- Journey Patterns Identified: {summary.get('patterns_identified', 0)}\n"
            context += f"- Customers At Churn Risk: {summary.get('customers_at_risk', 0)}\n\n"

            # LTV Segments
            if ltv_segments and 'segments' in ltv_segments:
                context += "LTV SEGMENTATION:\n"
                top_20 = ltv_segments['segments'].get('top_20', {})
                middle_60 = ltv_segments['segments'].get('middle_60', {})
                bottom_20 = ltv_segments['segments'].get('bottom_20', {})

                if top_20:
                    context += f"\nTop 20% (High LTV):\n"
                    context += f"- Customers: {top_20.get('customer_count', 0)}\n"
                    context += f"- Avg LTV: ${top_20.get('avg_ltv', 0):,.0f}\n"
                    context += f"- Avg Orders: {top_20.get('avg_orders', 0):.1f}\n"
                    context += f"- Repeat Rate: {top_20.get('repeat_customer_rate', 0):.1f}%\n"
                    if top_20.get('avg_days_to_second_order'):
                        context += f"- Days to 2nd Order: {top_20['avg_days_to_second_order']:.0f}\n"

                if bottom_20:
                    context += f"\nBottom 20% (Low LTV):\n"
                    context += f"- Customers: {bottom_20.get('customer_count', 0)}\n"
                    context += f"- Avg LTV: ${bottom_20.get('avg_ltv', 0):,.0f}\n"
                    context += f"- Avg Orders: {bottom_20.get('avg_orders', 0):.1f}\n"
                    if bottom_20.get('avg_days_to_second_order'):
                        context += f"- Days to 2nd Order: {bottom_20['avg_days_to_second_order']:.0f}\n"

                if 'key_differences' in ltv_segments:
                    context += f"\nKEY DIFFERENCES:\n"
                    for key, value in ltv_segments['key_differences'].items():
                        context += f"- {key}: {value}\n"

            # Gateway Products
            if gateway_products:
                context += "\nGATEWAY PRODUCTS (Create Repeat Customers):\n"
                for i, product in enumerate(gateway_products[:5], 1):
                    context += f"\n{i}. {product['product_title']}\n"
                    context += f"   Repeat Rate: {product['metrics']['repeat_purchase_rate']}% ({product['metrics']['repeat_rate_vs_average']})\n"
                    context += f"   LTV: ${product['metrics']['avg_ltv']:,.0f} ({product['metrics']['ltv_vs_average']})\n"
                    context += f"   Currently Promoted: Featured={product['current_promotion']['is_featured']}, Ads={product['current_promotion']['is_in_ads']}\n"
                    context += f"   Opportunity: ${product['opportunity']['estimated_ltv_gain']:,.0f} LTV gain\n"

            # Dead-End Products
            if dead_end_products:
                context += "\nDEAD-END PRODUCTS (One-and-Done):\n"
                for i, product in enumerate(dead_end_products[:5], 1):
                    context += f"\n{i}. {product['product_title']}\n"
                    context += f"   One-Time Rate: {product['metrics']['one_time_rate']}% ({product['metrics']['one_time_rate_vs_average']})\n"
                    if product['metrics'].get('return_rate'):
                        context += f"   Return Rate: {product['metrics']['return_rate']}%\n"
                    context += f"   Currently Promoted: Featured={product['current_promotion']['is_featured']}, Ads={product['current_promotion']['is_in_ads']}\n"
                    context += f"   Problem Severity: {product['problem_severity']['severity']}\n"
                    context += f"   Estimated LTV Lost: ${product['problem_severity']['estimated_ltv_lost']:,.0f}\n"

            # Journey Patterns
            if journey_patterns:
                context += "\nJOURNEY PATTERNS:\n"
                for i, pattern in enumerate(journey_patterns[:3], 1):
                    context += f"\n{i}. {pattern['pattern_name']} ({pattern['pattern_type']})\n"
                    context += f"   Customers: {pattern['prevalence']['customer_count']}\n"
                    context += f"   Avg LTV: ${pattern['outcomes']['avg_ltv']:,.0f}\n"
                    if pattern['vs_baseline'].get('ltv_difference'):
                        context += f"   vs Baseline: {pattern['vs_baseline']['ltv_difference']}\n"

            # Churn Timing
            if churn_timing:
                context += "\nCHURN RISK TIMING:\n"
                context += f"- Customers At Risk: {churn_timing.get('customers_at_risk', 0)}\n"
                context += f"- Total LTV At Risk: ${churn_timing.get('total_ltv_at_risk', 0):,.0f}\n\n"

                if 'by_segment' in churn_timing:
                    for segment_name, segment_data in churn_timing['by_segment'].items():
                        context += f"{segment_name.replace('_', ' ').title()}:\n"
                        context += f"- Avg Days Between Purchases: {segment_data['timing_metrics']['avg_days_between_purchases']:.0f}\n"
                        context += f"- At-Risk Threshold: {segment_data['risk_thresholds']['at_risk_days']} days\n"
                        if segment_data.get('reactivation_window'):
                            context += f"- Optimal Reactivation: Days {segment_data['reactivation_window']['optimal_start_day']}-{segment_data['reactivation_window']['optimal_end_day']}\n"

            prompt = f"""You are a customer analytics expert analyzing journey patterns for an ecommerce business.

{context}

Provide clear, actionable customer journey insights:

**What Makes High-LTV Customers Different:**
- Specific behaviors from day 1 that predict repeat purchases
- Key differences in first purchase, timing, engagement
- Why these patterns matter

**Gateway Products (Promote These):**
- Which products create repeat customers when purchased first?
- What's the LTV multiplier for each?
- Where should these be featured (homepage, ads, email)?
- Expected impact of promoting gateway products

**Dead-End Products (Stop Promoting These):**
- Which products attract one-time buyers?
- Why don't customers come back after these purchases?
- What's currently being spent promoting these?
- Expected savings/LTV improvement from deprioritizing

**Journey Patterns to Encourage:**
- What path do high-LTV customers typically take?
- How to guide more customers onto this path
- Specific touchpoints to optimize

**Churn Prevention Strategy:**
- When do customers become at-risk?
- Optimal timing for win-back campaigns
- How many customers need reactivation now?
- Expected recovery rate and revenue

**Immediate Action Plan:**
1. [Most critical change with expected LTV impact]
2. [Second priority with expected impact]
3. [Third priority]

**Expected Business Impact:**
- Total LTV improvement from implementing all recommendations
- Which changes have highest ROI
- Timeline to see results

Be specific. Use exact product names. Focus on LTV impact."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated customer journey analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing customer journeys: {str(e)}")
            return None

    def analyze_user_behavior(
        self,
        high_friction_pages: List[Dict],
        checkout_funnel: List[Dict],
        mobile_issues: List[Dict],
        rage_click_pages: List[Dict],
        session_patterns: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze user behavior patterns and UX friction points

        Answers: "Where are users getting stuck? What's frustrating them?"
        """
        if not self.enabled:
            return None

        try:
            context = "USER BEHAVIOR INTELLIGENCE:\n\n"

            context += f"SUMMARY:\n"
            context += f"- High Friction Pages: {summary.get('high_friction_pages_count', 0)}\n"
            context += f"- Checkout Steps Analyzed: {summary.get('checkout_steps_analyzed', 0)}\n"
            context += f"- Mobile Issues: {summary.get('mobile_issues_count', 0)}\n"
            context += f"- Rage Click Pages: {summary.get('rage_click_pages_count', 0)}\n"
            context += f"- Total Revenue Impact: ${summary.get('total_estimated_revenue_impact', 0):,.0f}/month\n\n"

            # High Friction Pages
            if high_friction_pages:
                context += "HIGH-FRICTION PAGES:\n"
                for i, page in enumerate(high_friction_pages[:5], 1):
                    context += f"\n{i}. {page['page_path']}\n"
                    context += f"   Traffic: {page['traffic']['monthly_sessions']}/month\n"
                    context += f"   Conversion: {page['conversion']['conversion_rate']}% (avg: {page['conversion']['site_average']}%)\n"

                    if page['friction_signals']['rage_clicks'] > 0:
                        context += f"   Rage Clicks: {page['friction_signals']['rage_clicks']}\n"

                    if page['engagement'].get('median_scroll_depth'):
                        context += f"   Scroll Depth: {page['engagement']['median_scroll_depth']}%\n"

                    if page['friction_elements']:
                        context += f"   Friction Elements: {len(page['friction_elements'])} problematic elements\n"
                        top_element = page['friction_elements'][0] if page['friction_elements'] else None
                        if top_element:
                            context += f"      - {top_element.get('element')}: {top_element.get('click_count')} clicks, {top_element.get('issue')}\n"

                    context += f"   Revenue Lost: ${page['revenue_impact']['estimated_revenue_lost']:,.0f}/month\n"

            # Checkout Funnel
            if checkout_funnel:
                context += "\nCHECKOUT FUNNEL:\n"
                for step in checkout_funnel:
                    status = "✓" if step['metrics']['completion_rate'] and step['metrics']['completion_rate'] > 70 else "⚠️"
                    context += f"\n{step['step_name']}: {step['metrics']['completion_rate']}% {status}\n"

                    if step['is_biggest_leak']:
                        context += f"   ⚠️ BIGGEST LEAK - {step['metrics']['drop_off_rate']}% drop-off\n"

                    if step['friction_signals']['rage_clicks'] > 0:
                        context += f"   Rage Clicks: {step['friction_signals']['rage_clicks']}\n"

                    if step['friction_signals']['stuck_sessions'] > 0:
                        context += f"   Stuck Sessions: {step['friction_signals']['stuck_sessions']}\n"

                    if step['friction_signals']['page_reloads'] > 0:
                        context += f"   Page Reloads: {step['friction_signals']['page_reloads']}\n"

                    if step['issues']:
                        context += f"   Issues: {', '.join(step['issues'])}\n"

                    context += f"   Revenue Impact: ${step['revenue_impact']['estimated_revenue_lost']:,.0f}/month\n"

            # Mobile Issues
            if mobile_issues:
                context += "\nMOBILE ISSUES:\n"
                for i, issue in enumerate(mobile_issues[:5], 1):
                    context += f"\n{i}. {issue['page_path']}\n"
                    context += f"   Mobile: {issue['conversion_comparison']['mobile_conversion']}% vs Desktop: {issue['conversion_comparison']['desktop_conversion']}%\n"
                    context += f"   Gap: {issue['conversion_comparison']['gap']}% points\n"

                    if issue['mobile_friction']['rage_clicks'] > 0:
                        context += f"   Mobile Rage Clicks: {issue['mobile_friction']['rage_clicks']}\n"

                    if issue['mobile_specific_problems']:
                        context += f"   Mobile Problems:\n"
                        for problem in issue['mobile_specific_problems'][:3]:
                            context += f"      - {problem.get('issue')}\n"

                    context += f"   Revenue Lost: ${issue['revenue_impact']['estimated_revenue_lost']:,.0f}/month\n"

            # Rage Clicks
            if rage_click_pages:
                context += "\nRAGE CLICK PAGES (User Frustration):\n"
                for i, page in enumerate(rage_click_pages[:3], 1):
                    context += f"\n{i}. {page['page_path']}\n"
                    context += f"   Total Rage Clicks: {page['rage_clicks']['total_rage_clicks']}\n"
                    context += f"   Sessions with Rage: {page['rage_clicks']['sessions_with_rage']}\n"

                    if page.get('top_frustration_element'):
                        elem = page['top_frustration_element']
                        context += f"   Top Element: {elem.get('element')} ({elem.get('click_count')} clicks, {elem.get('issue')})\n"

                    context += f"   Diagnosis: {page.get('diagnosis', 'Unknown')}\n"

            # Session Patterns
            if session_patterns:
                context += "\nSESSION PATTERNS:\n"
                for i, pattern in enumerate(session_patterns[:3], 1):
                    context += f"\n{i}. {pattern['pattern_name']} ({pattern['pattern_type']})\n"
                    context += f"   Sessions: {pattern['prevalence']['sessions_with_pattern']}\n"

                    if pattern['outcomes'].get('conversion_rate'):
                        context += f"   Conversion: {pattern['outcomes']['conversion_rate']}%\n"

                    if pattern.get('event_sequence'):
                        context += f"   Sequence: {len(pattern['event_sequence'])} events\n"

            prompt = f"""You are a UX optimization expert analyzing user behavior data for an ecommerce business.

{context}

Provide clear, actionable UX improvement recommendations:

**CRITICAL PRIORITIES (This Week):**
- Which 2-3 fixes will drive the most revenue fastest?
- Specific implementation steps for each
- Expected revenue impact

**HIGH-FRICTION PAGES:**
- What's causing the friction on each page?
- Root cause diagnosis (not just symptoms)
- Specific fixes (move X above fold, make Y clickable, etc.)
- Expected conversion lift

**CHECKOUT FUNNEL:**
- Biggest leak and why users are dropping off
- What friction signals tell us (rage clicks = what's broken?)
- Specific fix for biggest leak
- Expected completion rate improvement

**MOBILE ISSUES:**
- Which pages have the worst mobile experience?
- Specific mobile UX problems (touch targets, layout, etc.)
- Priority order for mobile fixes
- Expected mobile conversion improvement

**RAGE CLICKS (Quick Wins):**
- What elements are users trying to click?
- Why aren't they working?
- Simple fixes (< 1 hour each)

**ROI ESTIMATE:**
- Total monthly revenue opportunity
- Which fixes have highest ROI (impact/effort)
- Implementation timeline
- Quick wins you can do this week

Be specific with element names, page paths, and dollar amounts.
Focus on root causes, not symptoms.
Prioritize by revenue impact and ease of implementation."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated user behavior analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing user behavior: {str(e)}")
            return None

    def analyze_ad_spend(
        self,
        campaigns: List[Dict],
        scaling_opportunities: List[Dict],
        waste_identified: List[Dict],
        budget_reallocations: List[Dict],
        product_performance: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze ad spend optimization opportunities

        Answers: "Where am I wasting ad spend? Where should I scale?"
        """
        if not self.enabled:
            return None

        try:
            context = "AD SPEND OPTIMIZATION INTELLIGENCE:\n\n"

            context += f"SUMMARY:\n"
            context += f"- Total Campaigns: {summary.get('total_campaigns', 0)}\n"
            context += f"- Total Spend: ${summary.get('total_spend', 0):,.0f}/month\n"
            context += f"- Total Waste Identified: ${summary.get('total_waste_identified', 0):,.0f}/month\n"
            context += f"- Scaling Opportunities: {summary.get('scaling_opportunities_count', 0)}\n"
            context += f"- Total Scaling Opportunity: +${summary.get('total_scaling_opportunity', 0):,.0f}/month\n\n"

            # Campaign Performance
            if campaigns:
                context += "CAMPAIGN PERFORMANCE (True ROAS vs Google ROAS):\n"
                for i, campaign in enumerate(campaigns[:10], 1):
                    context += f"\n{i}. {campaign['campaign_name']} ({campaign['campaign_type']})\n"
                    context += f"   Spend: ${campaign['spend']:,.0f}/month\n"
                    context += f"   Google ROAS: {campaign['roas_difference']['google_roas']}x\n"
                    context += f"   True ROAS: {campaign['roas_difference']['true_roas']}x\n"

                    if campaign['roas_difference']['inflated_by']:
                        context += f"   Google inflated by: {campaign['roas_difference']['inflated_by']}x\n"

                    context += f"   Profit: ${campaign['true_metrics']['profit']:,.0f}\n"

                    if campaign['budget_status']['is_capped']:
                        context += f"   Budget-Capped: Runs out at {campaign['budget_status']['cap_time']}\n"

                    if campaign['indicators']['is_wasting_budget']:
                        context += f"   ⚠️ WASTING BUDGET\n"

            # Scaling Opportunities
            if scaling_opportunities:
                context += "\nSCALING OPPORTUNITIES:\n"
                for i, opp in enumerate(scaling_opportunities[:5], 1):
                    context += f"\n{i}. {opp['campaign_name']}\n"
                    context += f"   Current: ${opp['current_performance']['monthly_spend']:,.0f}/month, {opp['current_performance']['true_roas']}x ROAS\n"
                    context += f"   Budget Capped: {opp['budget_constraint']['is_capped']}\n"
                    if opp['budget_constraint']['caps_at']:
                        context += f"   Caps at: {opp['budget_constraint']['caps_at']}\n"
                    context += f"   Recommendation: Increase to ${opp['recommendation']['recommended_monthly_budget']:,.0f}\n"
                    context += f"   Expected profit increase: +${opp['expected_impact']['additional_profit_per_month']:,.0f}/month\n"

            # Waste Identified
            if waste_identified:
                context += "\nWASTE IDENTIFIED:\n"
                for i, waste in enumerate(waste_identified[:5], 1):
                    context += f"\n{i}. {waste['waste_type'].replace('_', ' ').title()}\n"
                    context += f"   Campaign: {waste['affected']['campaign_name']}\n" if waste['affected'].get('campaign_name') else ""
                    context += f"   Product: {waste['affected']['product_title']}\n" if waste['affected'].get('product_title') else ""
                    context += f"   Monthly Waste: ${waste['waste_metrics']['monthly_waste']:,.0f}\n"
                    context += f"   Severity: {waste['waste_metrics']['severity']}\n"
                    context += f"   Issue: {waste['description']}\n"
                    context += f"   Fix: {waste['recommendation']['action']}\n"
                    context += f"   Expected Savings: ${waste['recommendation']['expected_savings']:,.0f}/month\n"

            # Budget Reallocations
            if budget_reallocations:
                context += "\nBUDGET REALLOCATION OPPORTUNITIES:\n"
                for i, realloc in enumerate(budget_reallocations[:3], 1):
                    context += f"\n{i}. {realloc['optimization_name']}\n"
                    if realloc['from_campaign'].get('name'):
                        context += f"   From: {realloc['from_campaign']['name']} (reduce ${realloc['from_campaign']['budget_reduction']:,.0f})\n"
                    if realloc['to_campaign'].get('name'):
                        context += f"   To: {realloc['to_campaign']['name']} (add ${realloc['to_campaign']['budget_increase']:,.0f})\n"
                    context += f"   Expected additional profit: +${realloc['expected_impact']['additional_profit']:,.0f}/month\n"
                    context += f"   Same total spend\n"

            # Product Performance
            if product_performance:
                profitable = [p for p in product_performance if p['indicators']['is_profitable']]
                unprofitable = [p for p in product_performance if p['indicators']['is_losing_money']]

                if unprofitable:
                    context += "\nPRODUCTS LOSING MONEY ON ADS:\n"
                    for i, product in enumerate(unprofitable[:5], 1):
                        context += f"\n{i}. {product['product_title']}\n"
                        context += f"   Ad Spend: ${product['ad_spend']['total_spend']:,.0f}\n"
                        context += f"   Revenue: ${product['revenue']['ad_revenue']:,.0f}\n"
                        context += f"   Net Profit: ${product['profitability']['net_profit']:,.0f}\n"
                        context += f"   Profit ROAS: {product['roas']['profit_roas']}x\n"
                        context += f"   Recommendation: {product['recommendation']['action']}\n"

            prompt = f"""You are a Google Ads optimization expert analyzing ad spend for an ecommerce business.

{context}

Provide clear, actionable ad spend optimization recommendations:

**TRUE ROAS ANALYSIS:**
- How much is Google's ROAS inflated compared to reality?
- Which campaigns look good in Google but are actually unprofitable?
- Why the difference? (product costs, margin reality)

**CRITICAL PRIORITIES (This Week):**
- Top 3 changes for maximum profit impact
- Specific budget numbers for each
- Expected profit increase

**SCALING OPPORTUNITIES:**
- Which campaigns should get more budget?
- How much to increase (specific $ amounts)
- Why they're budget-capped and losing impression share
- Expected profit from scaling

**WASTE TO CUT:**
- Brand cannibalization: How much organic traffic are you paying for?
- Below-margin products: Which products lose money on every sale?
- Zero-conversion spend: What's not working at all?
- Specific $ amounts to cut from each

**BUDGET REALLOCATION:**
- Best reallocation moves (Campaign A → Campaign B)
- Expected profit impact of each move
- Same total spend, better results

**PRODUCT EXCLUSIONS:**
- Which products should be excluded from Shopping/PMax?
- Why they're unprofitable (margin too low, high CPA)
- Expected savings from exclusion

**ROI ESTIMATE:**
- Total monthly profit opportunity
- Breakdown: Waste to cut + Scaling to add
- Implementation priority order
- Quick wins (< 1 hour to implement)

Be specific with campaign names, dollar amounts, and ROAS numbers.
Focus on profit, not just revenue.
Prioritize by dollars of impact."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated ad spend optimization analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing ad spend: {str(e)}")
            return None

    def analyze_pricing_impact(self, pricing_data: Dict) -> Optional[str]:
        """
        Analyze pricing competitiveness and its impact on sales.

        Answers: "Are we losing sales due to price?", "Which SKUs are price-sensitive?",
        "How much revenue is at risk from competitor undercutting?"
        """
        if not self.enabled:
            return None

        try:
            sku_data = pricing_data.get('sku_sensitivity', {})
            brand_data = pricing_data.get('brand_impact', {})
            unmatchable = pricing_data.get('unmatchable_risk', {})

            context = "PRICING IMPACT INTELLIGENCE:\n\n"

            # SKU sensitivity summary
            context += f"SKU PRICING SENSITIVITY:\n"
            context += f"- Total SKUs Analyzed: {sku_data.get('total_skus_analyzed', 0)}\n"
            context += f"- Price-Sensitive SKUs: {sku_data.get('price_sensitive_count', 0)}\n"
            context += f"- Analysis Period: {sku_data.get('analysis_period_days', 30)} days\n"
            context += f"- Decline Threshold: {sku_data.get('decline_threshold_pct', 10)}%\n\n"

            sensitive_skus = [s for s in sku_data.get('skus', []) if s.get('price_sensitive')]
            if sensitive_skus:
                context += "TOP PRICE-SENSITIVE SKUs (undercut + declining sales):\n"
                for i, s in enumerate(sensitive_skus[:15], 1):
                    context += f"\n{i}. {s['title']} (SKU: {s['sku']})\n"
                    context += f"   Brand: {s['vendor']}\n"
                    context += f"   Our Price: ${s['current_price']:,.2f}\n"
                    context += f"   Cheapest Competitor: ${s['lowest_competitor_price']:,.2f} ({s['cheapest_competitor']})\n"
                    context += f"   Price Gap: ${s['price_gap']:,.2f} ({s['price_gap_pct']}% higher)\n"
                    context += f"   Units (current 30d): {s['units_30d']} | Prior 30d: {s['units_prior_30d']}\n"
                    context += f"   Unit Change: {s['pct_change_30d']}%\n"
                    context += f"   Revenue (30d): ${s['revenue_30d']:,.2f}\n"

            # Brand impact
            brands = brand_data.get('brands', [])
            if brands:
                context += "\nBRAND PRICING IMPACT:\n"
                for i, b in enumerate(brands[:10], 1):
                    context += f"\n{i}. {b['brand']}\n"
                    context += f"   Total SKUs: {b['total_skus']} | Undercut: {b['undercut_skus']}\n"
                    context += f"   Avg Price Gap: ${b['avg_price_gap']:,.2f}\n"
                    context += f"   Units Decline: {b['pct_units_decline']}%\n"
                    context += f"   Revenue at Risk: ${b['revenue_at_risk']:,.2f}\n"
                    context += f"   Price-Sensitive SKUs: {b['price_sensitive_skus']}\n"

            # Unmatchable
            context += f"\nUNMATCHABLE REVENUE RISK:\n"
            context += f"- Unmatchable SKUs (competitor below our floor): {unmatchable.get('total_unmatchable_skus', 0)}\n"
            context += f"- Total Revenue at Risk: ${unmatchable.get('total_revenue_at_risk', 0):,.2f}\n"
            context += f"- Orders Affected (30d): {unmatchable.get('total_orders_affected', 0)}\n\n"

            unmatchable_skus = unmatchable.get('skus', [])
            if unmatchable_skus:
                context += "TOP UNMATCHABLE SKUs:\n"
                for i, s in enumerate(unmatchable_skus[:10], 1):
                    context += f"\n{i}. {s['title']} (SKU: {s['sku']})\n"
                    context += f"   Our Price: ${s['our_price']:,.2f} | Our Floor: ${s['our_floor']:,.2f}\n"
                    context += f"   Competitor: ${s['competitor_price']:,.2f} ({s['cheapest_competitor']})\n"
                    context += f"   Gap Below Floor: ${s['gap_below_floor']:,.2f}\n"
                    context += f"   Revenue at Risk: ${s['revenue_at_risk']:,.2f}\n"

            prompt = f"""You are a pricing strategy analyst for an e-commerce business in the appliances/bathroom industry.

{context}

Provide a clear, actionable pricing impact analysis:

**EXECUTIVE SUMMARY:**
- Are we losing sales due to pricing? Quantify the impact.
- How much revenue is at risk from competitor undercutting?

**MOST AT-RISK SKUs:**
- Which specific products are most impacted?
- What is the sales trend telling us?
- Prioritise by revenue impact.

**BRAND-LEVEL INSIGHTS:**
- Which brands have the biggest pricing gap problem?
- Are certain brands more price-sensitive than others?
- Revenue at risk by brand.

**UNMATCHABLE PRODUCTS:**
- How significant is the revenue risk from products we cannot match?
- Are there strategic options (negotiate cost, accept margin hit, differentiate)?

**RECOMMENDED ACTIONS:**
1. Immediate price adjustments (SKUs to reprice now)
2. Brand-level strategy changes
3. Supplier negotiation opportunities (where cost reduction would help)
4. SKUs to watch but not act on yet

Be specific with SKU names, dollar amounts, and percentages.
Focus on revenue impact and actionable recommendations."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated pricing impact analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing pricing impact: {str(e)}")
            return None

    def generate_weekly_brief(
        self,
        priorities: List[Dict],
        working_well: List[Dict],
        watch_list: List[Dict],
        trends: Dict,
        data_quality_score: int,
        total_impact: float
    ) -> Optional[str]:
        """
        Generate executive summary for weekly brief

        Answers: "What should I focus on this week?"
        """
        if not self.enabled:
            return None

        try:
            context = "WEEKLY STRATEGIC BRIEF:\n\n"

            context += f"DATA QUALITY: {data_quality_score}/100 "
            if data_quality_score >= 90:
                context += "✓ (Insights are reliable)\n"
            elif data_quality_score >= 70:
                context += "⚠ (Insights are mostly reliable)\n"
            else:
                context += "✗ (Data quality issues - insights may be unreliable)\n"

            context += "\n"

            # Top priorities
            context += "TOP PRIORITIES:\n"
            for i, priority in enumerate(priorities[:5], 1):
                context += f"\n#{i} — {priority['title'].upper()}\n"
                context += f"   Source: {priority['source_module'].replace('_', ' ').title()} Module\n"
                context += f"   Impact: +${priority['impact']:,.0f}/{priority.get('impact_timeframe', 'month')}\n"
                context += f"   Effort: {priority['effort']['hours']:.1f} hours ({priority['effort']['level']})\n"
                context += f"   Confidence: {priority['confidence'].title()}\n"
                context += f"   Action: {priority['action']}\n"

            context += f"\nTOTAL OPPORTUNITY: +${total_impact:,.0f}\n"

            # What's working
            if working_well:
                context += "\nWHAT'S WORKING (don't touch):\n"
                for item in working_well[:5]:
                    context += f"- {item['description']}\n"

            # Watch list
            if watch_list:
                context += "\nWATCH LIST (monitor, not urgent):\n"
                for item in watch_list[:5]:
                    context += f"- {item.get('description', 'Item to monitor')}\n"

            # Trends
            if trends:
                context += "\nVS LAST WEEK:\n"
                for impl in trends.get('implemented', []):
                    context += f"✅ Implemented: {impl.get('title', impl)}\n"
                for pend in trends.get('pending', [])[:3]:
                    context += f"⏳ Still pending: {pend}\n"
                for impr in trends.get('improved', [])[:3]:
                    context += f"📈 Improved: {impr}\n"
                for decl in trends.get('declined', [])[:3]:
                    context += f"📉 Declined: {decl}\n"

            prompt = f"""You are a strategic business advisor creating an executive summary for a weekly strategic brief.

{context}

Create a compelling, action-oriented weekly brief that answers: "What should I focus on this week?"

Format:

**EXECUTIVE SUMMARY:**
[2-3 sentence high-level overview of the week's priorities and overall business health]

**TOP 3 PRIORITIES THIS WEEK:**

For each priority:
- Why it matters (business context)
- Specific action to take
- Expected outcome
- Implementation note (any gotchas or dependencies)

**STRATEGIC FOCUS:**
[One paragraph on the overarching theme - is this a revenue optimization week? A cost-cutting week? A UX improvement week?]

**WHAT TO PROTECT:**
[1-2 sentences on what's working well that should NOT be changed]

**EMERGING CONCERNS:**
[1-2 sentences on watch list items and when to escalate]

**WEEK-OVER-WEEK PROGRESS:**
[Brief commentary on trends - what's improving, what needs attention]

**RECOMMENDED APPROACH:**
[Concrete advice on implementation order and timeline - "Start with X on Monday, Y on Wednesday" etc.]

Be concise, specific, and actionable. Use exact dollar amounts. Prioritize by ROI (impact/effort).
This is what a CEO would read Monday morning to know where to focus."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            summary = response.content[0].text
            log.info("Generated weekly brief executive summary via LLM")
            return summary

        except Exception as e:
            log.error(f"Error generating weekly brief: {str(e)}")
            return None

    def analyze_content_gaps(
        self,
        content_gaps: List[Dict],
        merchandising_gaps: List[Dict],
        content_opportunities: List[Dict],
        underperforming_content: List[Dict],
        category_health: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze content and merchandising gaps

        Returns strategic recommendations for:
        - Content creation priorities
        - Merchandising improvements
        - Quick wins
        - Long-term content strategy
        """
        if not self.enabled:
            return None

        try:
            # Build context
            context = "CONTENT & MERCHANDISING GAP ANALYSIS:\n\n"

            # Summary
            context += f"**OVERVIEW:**\n"
            context += f"- Total Gaps: {summary.get('total_gaps', 0)}\n"
            context += f"- Critical Gaps: {summary.get('critical_gaps', 0)}\n"
            context += f"- Content Opportunities: {summary.get('opportunities_count', 0)}\n"
            context += f"- Total Revenue Opportunity: ${summary.get('total_revenue_opportunity', 0):,.0f}/month\n"
            context += f"- Average Content Health Score: {summary.get('avg_content_health_score', 0)}/100\n\n"

            # Top content gaps
            context += "**TOP CONTENT GAPS:**\n"
            for gap in content_gaps[:5]:
                context += f"\n{gap['gap_type']} - {gap['product_title']}\n"
                context += f"  Severity: {gap['gap_severity']}\n"
                context += f"  Current: {gap['current_state']}\n"
                context += f"  Impact: ${gap['impact']['estimated_revenue_impact']:.0f}/month\n"
                context += f"  Effort: {gap['effort']['hours']} hours\n"

            # Top merchandising gaps
            context += "\n\n**TOP MERCHANDISING GAPS:**\n"
            for gap in merchandising_gaps[:5]:
                context += f"\n{gap['gap_type']} - {gap['product_title']}\n"
                context += f"  {gap['description']}\n"
                context += f"  Impact: ${gap['impact']['estimated_impact']:.0f}/month\n"
                context += f"  Effort: {gap['effort']['level']} ({gap['effort']['hours']} hours)\n"

            # Top content opportunities
            context += "\n\n**CONTENT OPPORTUNITIES:**\n"
            for opp in content_opportunities[:3]:
                context += f"\n{opp['opportunity_type']} - {opp['topic']}\n"
                context += f"  Target: {opp['target_audience']}\n"
                context += f"  Search Volume: {opp['opportunity_metrics']['search_volume']}/month\n"
                context += f"  Est. Revenue: ${opp['opportunity_metrics']['estimated_monthly_revenue']:.0f}/month\n"
                context += f"  Effort: {opp['effort']['hours']} hours\n"

            # Underperforming content
            context += "\n\n**UNDERPERFORMING CONTENT (High Traffic, Low Conversion):**\n"
            for page in underperforming_content[:3]:
                context += f"\n{page['page_title']} ({page['page_url']})\n"
                context += f"  Traffic: {page['performance']['monthly_sessions']:,} sessions/month\n"
                context += f"  Conversion: {page['performance']['conversion_rate']:.1%} (vs {page['optimization_potential']['benchmark_conversion_rate']:.1%} benchmark)\n"
                context += f"  Issues: {', '.join(page['issues_identified'][:3])}\n"
                context += f"  Revenue Gain if Optimized: ${page['optimization_potential']['estimated_revenue_gain']:.0f}/month\n"

            # Category health
            context += "\n\n**CATEGORY HEALTH:**\n"
            for cat in category_health[:3]:
                context += f"\n{cat['category_name']} ({cat['product_count']} products)\n"
                context += f"  Overall Health: {cat['health_scores']['overall_health_score']}/100\n"
                context += f"  Gaps: {cat['gaps']['total_gaps']} total ({cat['gaps']['critical_gaps']} critical)\n"
                context += f"  Revenue Opportunity: ${cat['opportunity']['revenue_opportunity']:.0f}/month\n"
                context += f"  Top Priorities: {cat['top_priorities'][0] if cat['top_priorities'] else 'None'}\n"

            prompt = f"""You are a content strategy and merchandising expert analyzing an e-commerce site's content gaps.

{context}

Provide strategic recommendations covering:

1. **TOP 3 CONTENT PRIORITIES:** What content to create/fix first and why (consider impact, effort, and traffic potential)

2. **MERCHANDISING QUICK WINS:** Low-effort improvements with high impact (cross-sells, categorization, etc.)

3. **UNDERPERFORMING CONTENT STRATEGY:** How to fix high-traffic pages that don't convert

4. **CONTENT OPPORTUNITIES:** Which new content to create (guides, videos, etc.) and why

5. **CATEGORY-SPECIFIC RECOMMENDATIONS:** Focus areas for each category

6. **EXPECTED ROI:** Estimate total revenue impact if top priorities are implemented

Be specific about:
- Which products/pages to focus on first
- What the content should include
- Why each recommendation matters
- Expected revenue impact

Prioritize by: (revenue impact / effort hours). Quick wins that take 30 minutes but generate $2,000/month should rank higher than 20-hour projects generating $3,000/month."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated content gap analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing content gaps: {str(e)}")
            return None

    def analyze_code_health(
        self,
        repo_name: str,
        overall_health_score: int,
        quality_metrics: Dict,
        theme_health: Dict,
        security_issues: Dict,
        technical_debt: Dict,
        commit_analysis: Dict,
        dependency_status: Dict,
        priorities: List[Dict]
    ) -> Optional[str]:
        """
        Analyze code and theme health

        Returns strategic recommendations for:
        - Security fixes
        - Technical debt reduction
        - Performance optimization
        - Code quality improvements
        """
        if not self.enabled:
            return None

        try:
            # Build context
            context = f"CODE HEALTH ANALYSIS FOR: {repo_name}\n\n"

            # Overall score
            context += f"**OVERALL HEALTH SCORE:** {overall_health_score}/100\n\n"

            # Security issues
            context += f"**SECURITY VULNERABILITIES:**\n"
            context += f"- Total: {security_issues.get('total_vulnerabilities', 0)}\n"
            context += f"- Critical: {security_issues.get('critical', 0)}\n"
            context += f"- High: {security_issues.get('high', 0)}\n"
            context += f"- Medium: {security_issues.get('medium', 0)}\n\n"

            if security_issues.get('issues'):
                context += "Top Security Issues:\n"
                for issue in security_issues['issues'][:3]:
                    context += f"\n{issue['severity'].upper()}: {issue['title']}\n"
                    context += f"  Type: {issue['vulnerability_type']}\n"
                    context += f"  Description: {issue['description']}\n"
                    context += f"  Recommendation: {issue['recommendation']}\n"

            # Technical debt
            context += f"\n\n**TECHNICAL DEBT:**\n"
            context += f"- Total Items: {technical_debt.get('total_debt_items', 0)}\n"
            context += f"- High Priority: {technical_debt.get('high_priority', 0)}\n"
            context += f"- Estimated Effort: {technical_debt.get('estimated_total_effort_hours', 0)} hours\n\n"

            if technical_debt.get('items'):
                context += "Top Technical Debt:\n"
                for item in technical_debt['items'][:5]:
                    context += f"\n{item['severity'].upper()}: {item['title']}\n"
                    context += f"  Type: {item['debt_type']}\n"
                    context += f"  File: {item.get('file_path', 'N/A')}\n"
                    context += f"  Impact: {item.get('business_impact', 'N/A')} (business), {item.get('technical_impact', 'N/A')} (technical)\n"
                    context += f"  Effort: {item.get('estimated_effort_hours', 'N/A')} hours\n"
                    context += f"  Recommendation: {item['recommendation']}\n"

            # Code quality
            context += f"\n\n**CODE QUALITY:**\n"
            context += f"- Overall Score: {quality_metrics.get('overall_score', 0)}/100\n"
            context += f"- File Size Issues: {len(quality_metrics.get('file_size_issues', []))}\n"
            context += f"- Complexity Issues: {len(quality_metrics.get('complexity_issues', []))}\n"
            context += f"- Code Duplication: {quality_metrics.get('code_duplication', {}).get('duplication_percentage', 0)}%\n\n"

            # Theme health (if Shopify theme)
            if theme_health:
                context += f"**THEME HEALTH:**\n"
                context += f"- Overall Score: {theme_health.get('overall_score', 0)}/100\n"
                context += f"- Liquid Quality: {theme_health.get('liquid_quality', {}).get('score', 0)}/100\n"
                context += f"- Performance: {theme_health.get('performance', {}).get('score', 0)}/100\n"
                context += f"- Accessibility: {theme_health.get('accessibility', {}).get('score', 0)}/100\n"
                context += f"- SEO: {theme_health.get('seo', {}).get('score', 0)}/100\n\n"

            # Dependencies
            context += f"\n**DEPENDENCIES:**\n"
            context += f"- Total Dependencies: {dependency_status.get('total_dependencies', 0)}\n"
            context += f"- Outdated: {dependency_status.get('outdated_dependencies', 0)}\n"
            context += f"- Deprecated: {dependency_status.get('deprecated_dependencies', 0)}\n"
            context += f"- Vulnerable: {dependency_status.get('vulnerable_dependencies', 0)}\n\n"

            # Commit activity
            context += f"**RECENT ACTIVITY:**\n"
            context += f"- Commits (30 days): {commit_analysis.get('total_commits_last_30_days', 0)}\n"
            context += f"- Active Contributors: {commit_analysis.get('active_contributors_last_30_days', 0)}\n"
            context += f"- High Churn Files: {len(commit_analysis.get('code_churn', {}).get('high_churn_files', []))}\n\n"

            # Priorities
            context += "**TOP PRIORITIES:**\n"
            for i, priority in enumerate(priorities[:5], 1):
                context += f"\n{i}. {priority['title']} ({priority['priority']} priority)\n"
                context += f"   Category: {priority['category']}\n"
                context += f"   Description: {priority['description']}\n"
                context += f"   Effort: {priority.get('effort_hours', 'N/A')} hours\n"

            prompt = f"""You are a senior software engineer and code quality expert analyzing a Shopify theme repository.

{context}

Provide strategic recommendations covering:

1. **CRITICAL SECURITY FIXES:** What security issues must be fixed immediately and why (prioritize CVE vulnerabilities, XSS, exposed secrets)

2. **TECHNICAL DEBT STRATEGY:** Which debt to tackle first (balance impact vs effort, focus on items blocking future development)

3. **PERFORMANCE OPTIMIZATION:** Quick wins to improve page speed and bundle size

4. **CODE QUALITY IMPROVEMENTS:** Refactoring priorities (large files, complex functions, code duplication)

5. **DEPENDENCY MANAGEMENT:** Update strategy for outdated/vulnerable packages (which to update first, breaking changes to watch for)

6. **THEME-SPECIFIC RECOMMENDATIONS:** Shopify best practices (deprecated Liquid tags, accessibility, SEO)

7. **IMPLEMENTATION PLAN:** Concrete steps to improve health score from {overall_health_score}/100 to 90+

Be specific about:
- Which files to refactor first and why
- Which dependencies to update (with version numbers)
- Estimated time for each fix
- Business impact of NOT fixing critical issues
- Quick wins (< 2 hours) vs longer-term improvements

Prioritize by: (business impact × technical impact) / effort hours."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info(f"Generated code health analysis for {repo_name} via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing code health: {str(e)}")
            return None

    def analyze_404_health(
        self,
        not_found_errors: List[Dict],
        revenue_impact: Dict,
        redirect_issues: Dict,
        redirect_chains: List[Dict],
        broken_links: List[Dict],
        summary: Dict
    ) -> Optional[str]:
        """
        Analyze 404 errors and redirect health

        Returns strategic recommendations for:
        - Which 404s to fix first
        - Redirect creation strategy
        - Internal link fixes
        - SEO impact
        """
        if not self.enabled:
            return None

        try:
            # Build context
            context = "404 & REDIRECT HEALTH ANALYSIS:\n\n"

            # Summary
            context += f"**OVERVIEW:**\n"
            context += f"- Total 404 Errors: {summary.get('total_404_errors', 0)}\n"
            context += f"- High-Traffic 404s: {summary.get('high_traffic_404s', 0)}\n"
            context += f"- Total Monthly Revenue Loss: ${summary.get('total_monthly_revenue_loss', 0):,.0f}\n"
            context += f"- Total 404 Sessions: {summary.get('total_404_sessions', 0):,}/month\n"
            context += f"- Redirect Issues: {summary.get('redirects_with_issues', 0)}\n"
            context += f"- Redirect Chains: {summary.get('redirect_chains', 0)}\n"
            context += f"- Broken Internal Links: {summary.get('broken_internal_links', 0)}\n\n"

            # Top 404 errors
            context += "**TOP 404 ERRORS (by revenue impact):**\n"
            for error in not_found_errors[:5]:
                context += f"\n{error['requested_url']}\n"
                context += f"  Type: {error['url_type']}\n"
                context += f"  Traffic: {error['traffic']['total_hits']:,} hits ({error['traffic']['unique_visitors']:,} unique visitors)\n"
                context += f"  Revenue Loss: ${error['revenue_impact']['estimated_monthly_revenue_loss']:.0f}/month\n"
                context += f"  Likely Cause: {error['likely_cause']}\n"
                context += f"  Recommended Fix: {error['recommended_action']} → {error.get('redirect_to_url', 'N/A')}\n"

            # Revenue impact
            context += f"\n\n**REVENUE IMPACT:**\n"
            context += f"- Total Lost Revenue: ${revenue_impact.get('total_lost_revenue_monthly', 0):,.0f}/month\n"
            context += f"- High-Impact 404s (>$500/month): {revenue_impact.get('high_impact_404s_count', 0)}\n\n"

            # Redirect issues
            context += f"**REDIRECT ISSUES:**\n"
            if redirect_issues.get('broken_redirects'):
                context += f"\nBroken Redirects ({len(redirect_issues['broken_redirects'])}):\n"
                for redirect in redirect_issues['broken_redirects'][:3]:
                    context += f"  {redirect['source_url']} → {redirect['destination_url']} (returns {redirect['destination_status_code']})\n"

            if redirect_chains:
                context += f"\nRedirect Chains ({len(redirect_chains)}):\n"
                for chain in redirect_chains[:3]:
                    context += f"  {chain['initial_url']} → ... ({chain['chain_length']} hops) → {chain['final_url']}\n"
                    if chain['ends_in_404']:
                        context += f"    WARNING: Chain ends in 404!\n"

            # Broken internal links
            if broken_links:
                context += f"\n\n**BROKEN INTERNAL LINKS:**\n"
                for link in broken_links[:5]:
                    context += f"\n{link['source_page']} → {link['broken_link']}\n"
                    context += f"  Traffic: {link['source_page_traffic']:,} sessions/month to source page\n"
                    context += f"  Est. Clicks: {link['estimated_monthly_clicks']} clicks to broken link\n"
                    context += f"  Priority: {link['priority']}\n"

            prompt = f"""You are an SEO and website optimization expert analyzing 404 errors and redirect health.

{context}

Provide strategic recommendations covering:

1. **TOP 3 PRIORITIES:** Which 404s to fix first (prioritize by revenue impact and traffic)

2. **REDIRECT STRATEGY:** Recommended URL mappings (be specific - which URLs to redirect where and why)

3. **INTERNAL LINK FIXES:** Which broken internal links to fix first (high-traffic pages first)

4. **REDIRECT CHAIN FIXES:** How to simplify redirect chains (create direct redirects)

5. **SEO IMPACT:** How these 404s and redirects affect SEO rankings and user experience

6. **QUICK WINS:** Low-effort fixes that prevent immediate revenue loss

7. **ROI ESTIMATE:** Expected revenue recovery if top priorities are fixed

Be specific about:
- Exact URL redirects to create (source → destination)
- Which pages need internal link updates
- Expected revenue recovery per fix
- Implementation time (most redirects take 5 minutes)

Prioritize by revenue impact. A 404 causing $2,000/month loss should be fixed before one causing $100/month."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=settings.llm_max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )

            analysis = response.content[0].text
            log.info("Generated 404 & redirect health analysis via LLM")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing 404 health: {str(e)}")
            return None

    def generate_seo_blog_post(
        self,
        query: str,
        page_url: Optional[str],
        opportunity_type: str,
        seo_data: Dict,
        product_context: List[Dict],
        site_url: str
    ) -> Optional[Dict]:
        """
        Generate a full SEO-optimized blog post draft targeting an underperforming query.

        Returns dict with: title, meta_description, slug, content_html, outline,
        target_keywords, internal_links, word_count
        """
        if not self.enabled:
            return None

        try:
            context = f'TARGET QUERY: "{query}"\n'
            context += f"OPPORTUNITY TYPE: {opportunity_type}\n"
            context += f"SITE URL: {site_url}\n\n"

            context += "SEO DATA:\n"
            context += f"- Current Position: {seo_data.get('position', 'N/A')}\n"
            context += f"- Impressions: {seo_data.get('impressions', 0)}/month\n"
            context += f"- Clicks: {seo_data.get('clicks', 0)}\n"
            context += f"- Click Gap: {seo_data.get('click_gap', 0)}\n"
            context += f"- CTR: {seo_data.get('actual_ctr', 'N/A')}%\n"

            if page_url:
                context += f"\nEXISTING PAGE: {page_url}\n"
                context += "(This page needs content expansion/refresh)\n"
            else:
                context += "\nNO EXISTING PAGE — this is NEW content to create.\n"

            if seo_data.get("related_queries"):
                context += "\nRELATED QUERIES (also target these):\n"
                for rq in seo_data["related_queries"][:10]:
                    context += f'- "{rq["query"]}" (pos {rq.get("position", "?")}, {rq.get("impressions", 0)} imp)\n'

            if product_context:
                context += "\nRELEVANT PRODUCTS TO REFERENCE:\n"
                for p in product_context[:8]:
                    context += f"- {p['title']} ({p.get('vendor', '')})"
                    if p.get("handle"):
                        context += f" — /products/{p['handle']}"
                    context += "\n"

            prompt = f"""You are an expert SEO content writer for an Australian e-commerce site that sells bathroom, kitchen and plumbing products.

{context}

Write a COMPLETE blog post (1200-1800 words) optimized for the target query.

Return your response in this EXACT format:

TITLE: [SEO-optimized title, 55-60 chars, include target keyword]

META_DESCRIPTION: [Compelling meta description, 150-155 chars, include target keyword]

SLUG: [url-friendly-slug]

TARGET_KEYWORDS: [comma-separated list of 5-8 keywords to target]

OUTLINE:
H2: [First section heading]
  H3: [Subsection if needed]
H2: [Second section heading]
H2: [Third section heading]
  H3: [Subsection]
H2: [FAQ or Conclusion]

INTERNAL_LINKS:
- "anchor text" -> /products/handle (reason: relevant product)
- "anchor text" -> /collections/category (reason: category page)

CONTENT:
[Full blog post in HTML format with <h2>, <h3>, <p>, <ul>, <ol>, <strong> tags.
Naturally integrate the target keyword 3-5 times.
Include a FAQ section with 3-4 common questions.
Reference relevant products with links where natural.
Write for Australian audience (favour, colour, etc.).
Include specific, helpful information — not generic filler.
End with a clear call-to-action.]

Requirements:
1. Title must include the primary keyword naturally
2. Use H2/H3 hierarchy for clear structure
3. Include internal links to relevant product pages
4. Write naturally — avoid keyword stuffing
5. Include FAQ schema-ready questions
6. Australian English spelling
7. Actionable, helpful content that answers searcher intent
8. End with CTA linking to relevant product category"""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )

            raw = response.content[0].text
            parsed = self._parse_blog_response(raw)
            parsed["generation_tokens"] = (
                response.usage.output_tokens if hasattr(response, "usage") else None
            )
            parsed["llm_model"] = settings.llm_model

            log.info(f"Generated blog draft for query: {query}")
            return parsed

        except Exception as e:
            log.error(f"Error generating blog post: {str(e)}")
            return None

    def _parse_blog_response(self, raw: str) -> Dict:
        """Parse structured blog generation response into dict."""
        result = {
            "title": "",
            "meta_description": "",
            "slug": "",
            "content_html": "",
            "outline": [],
            "target_keywords": [],
            "internal_links": [],
            "word_count": 0,
            "estimated_reading_time": 1,
        }

        title_match = re.search(r"TITLE:\s*(.+)", raw)
        if title_match:
            result["title"] = title_match.group(1).strip()

        meta_match = re.search(r"META_DESCRIPTION:\s*(.+)", raw)
        if meta_match:
            result["meta_description"] = meta_match.group(1).strip()[:300]

        slug_match = re.search(r"SLUG:\s*(.+)", raw)
        if slug_match:
            result["slug"] = slug_match.group(1).strip()

        kw_match = re.search(r"TARGET_KEYWORDS:\s*(.+)", raw)
        if kw_match:
            result["target_keywords"] = [
                k.strip() for k in kw_match.group(1).split(",")
            ]

        # Extract outline section
        outline_match = re.search(
            r"OUTLINE:\s*\n((?:.*H[23]:.*\n?)+)", raw
        )
        if outline_match:
            current_h2 = None
            for line in outline_match.group(1).strip().split("\n"):
                line = line.strip()
                if line.startswith("H2:"):
                    current_h2 = {
                        "heading": line[3:].strip(),
                        "subheadings": [],
                    }
                    result["outline"].append(current_h2)
                elif line.startswith("H3:") and current_h2:
                    current_h2["subheadings"].append(line[3:].strip())

        # Extract internal links
        links_match = re.search(
            r"INTERNAL_LINKS:\s*\n((?:.*->.*\n?)+)", raw
        )
        if links_match:
            for line in links_match.group(1).strip().split("\n"):
                link_match = re.match(
                    r'-\s*"(.+?)"\s*->\s*(\S+)\s*(?:\(reason:\s*(.+?)\))?',
                    line,
                )
                if link_match:
                    result["internal_links"].append(
                        {
                            "text": link_match.group(1),
                            "url": link_match.group(2),
                            "context": link_match.group(3) or "",
                        }
                    )

        # Extract content (everything after CONTENT:)
        content_match = re.search(r"CONTENT:\s*\n([\s\S]+)", raw)
        if content_match:
            result["content_html"] = content_match.group(1).strip()
        else:
            result["content_html"] = raw

        # Word count (strip HTML tags)
        text_only = re.sub(r"<[^>]+>", "", result["content_html"])
        result["word_count"] = len(text_only.split())
        result["estimated_reading_time"] = max(1, result["word_count"] // 250)

        return result

    def suggest_blog_topics(
        self,
        underperformers: List[Dict],
        limit: int = 5,
    ) -> Optional[List[Dict]]:
        """
        Given SEO underperformers, suggest the best blog post topics.

        Returns list of dicts with: query, rationale, suggested_angle, priority
        """
        if not self.enabled or not underperformers:
            return None

        try:
            context = "SEO UNDERPERFORMERS (sorted by priority):\n\n"
            for i, u in enumerate(underperformers[:20], 1):
                context += f'{i}. "{u["query"]}" — pos {u.get("position", "?")}, '
                context += f'{u.get("impressions", 0)} imp, {u.get("clicks", 0)} clicks, '
                context += f'click gap: {u.get("click_gap", 0)}, '
                context += f'decay: {u.get("content_decay", False)}, '
                context += f'fix: {u.get("fix_first", "N/A")}\n'
                if u.get("page"):
                    context += f'   Page: {u["page"]}\n'

            prompt = f"""You are an SEO content strategist for an Australian e-commerce site selling bathroom, kitchen and plumbing products.

{context}

From these underperforming queries, pick the TOP {limit} that would benefit MOST from a new or refreshed blog post.

For each, return in this EXACT format:

QUERY: "exact query"
RATIONALE: Why blog content helps this query rank better
ANGLE: Specific blog topic/title angle
PRIORITY: high or medium
---

Only recommend blog posts where long-form content would actually help rank better.
Skip queries where a simple title/meta fix is sufficient.
Focus on queries where content depth, FAQs, buying guides, or how-to content would close the click gap.
Consider the searcher's intent — informational queries benefit most from blog content."""

            response = self.client.messages.create(
                model=settings.llm_model,
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )

            raw = response.content[0].text
            suggestions = self._parse_topic_suggestions(raw)
            log.info(f"Generated {len(suggestions)} blog topic suggestions")
            return suggestions[:limit]

        except Exception as e:
            log.error(f"Error suggesting blog topics: {str(e)}")
            return None

    def _parse_topic_suggestions(self, raw: str) -> List[Dict]:
        """Parse topic suggestion response into list of dicts."""
        suggestions = []
        blocks = re.split(r"\n---\n?", raw)

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            query_match = re.search(r'QUERY:\s*"?(.+?)"?\s*$', block, re.MULTILINE)
            rationale_match = re.search(r"RATIONALE:\s*(.+)", block)
            angle_match = re.search(r"ANGLE:\s*(.+)", block)
            priority_match = re.search(r"PRIORITY:\s*(.+)", block)

            if query_match:
                suggestions.append(
                    {
                        "query": query_match.group(1).strip(),
                        "rationale": rationale_match.group(1).strip()
                        if rationale_match
                        else "",
                        "suggested_angle": angle_match.group(1).strip()
                        if angle_match
                        else "",
                        "priority": priority_match.group(1).strip().lower()
                        if priority_match
                        else "medium",
                    }
                )

        return suggestions

    def is_available(self) -> bool:
        """Check if LLM service is available"""
        return self.enabled
