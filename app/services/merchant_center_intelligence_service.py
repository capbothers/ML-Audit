"""
Merchant Center Intelligence Service

Product visibility, feed health, issue breakdown, category risk,
GTIN coverage, and revenue-at-risk analysis.

Two data modes:
 - GMC tables (populated after /sync/merchant-center runs daily)
 - Feed Readiness from Shopify + ProductCost (works immediately)
"""
import logging
from datetime import datetime, timedelta, date
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import func, case, desc, and_, or_, distinct

from app.models.merchant_center_data import (
    MerchantCenterProductStatus,
    MerchantCenterDisapproval,
    MerchantCenterAccountStatus,
)
from app.models.data_quality import MerchantCenterHealth
from app.models.shopify import (
    ShopifyProduct,
    ShopifyInventory,
    ShopifyOrderItem,
)
from app.models.product_cost import ProductCost
from app.models.competitive_pricing import CompetitivePricing

logger = logging.getLogger(__name__)


class MerchantCenterIntelligenceService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Dashboard orchestrator
    # ------------------------------------------------------------------

    def get_dashboard(self):
        """Return the complete payload for all tabs."""
        try:
            has_gmc = self._has_gmc_data()
            snapshot = self._get_latest_snapshot_date()

            # GMC-powered sections (empty-safe)
            executive_kpis = self._compute_executive_kpis(snapshot)
            eligibility = self._compute_eligibility_breakdown(snapshot)
            issue_breakdown = self._compute_issue_breakdown(snapshot)
            category_risk = self._compute_category_risk(snapshot)
            at_risk_products = self._compute_at_risk_products(snapshot)
            approval_trend = self._compute_approval_trend()

            # Feed Readiness sections (always work from Shopify data)
            feed_readiness = self._compute_feed_readiness()
            gtin_coverage = self._compute_gtin_coverage()
            price_drift = self._compute_price_drift()
            availability_health = self._compute_availability_health()

            health_score = self._compute_feed_health_score(
                executive_kpis, feed_readiness, gtin_coverage
            )
            pulse = self._compute_pulse(
                executive_kpis, health_score, has_gmc, feed_readiness
            )

            return {
                "has_gmc_data": has_gmc,
                "estimates_only": not has_gmc,
                "snapshot_date": str(snapshot) if snapshot else None,
                "pulse": pulse,
                "health_score": health_score,
                "executive_kpis": executive_kpis,
                "eligibility_breakdown": eligibility,
                "issue_breakdown": issue_breakdown,
                "category_risk": category_risk,
                "at_risk_products": at_risk_products,
                "approval_trend": approval_trend,
                "feed_readiness": feed_readiness,
                "gtin_coverage": gtin_coverage,
                "price_drift": price_drift,
                "availability_health": availability_health,
            }
        except Exception as e:
            logger.error(f"Dashboard generation failed: {e}")
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _has_gmc_data(self):
        """Check if any GMC data exists."""
        count = (
            self.db.query(func.count(MerchantCenterAccountStatus.id)).scalar()
        )
        return (count or 0) > 0

    def _get_latest_snapshot_date(self):
        """Get most recent GMC snapshot date."""
        row = (
            self.db.query(func.max(MerchantCenterAccountStatus.snapshot_date))
            .scalar()
        )
        return row

    def _get_active_product_universe(self):
        """Single source of truth for active product counts and subquery."""
        active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
            ShopifyProduct.status == 'active'
        ).subquery()
        total = (
            self.db.query(func.count(ShopifyProduct.id))
            .filter(ShopifyProduct.status == 'active')
            .scalar()
        ) or 0
        return active_pids, total

    def _count_gtin_in_active_universe(self, active_pids):
        """Count active-product SKUs that have / are missing GTIN in ProductCost.

        Joins ProductCost → ShopifyInventory (UPPER match) → active product filter.
        Returns (with_gtin, missing_gtin, matched_total).
        """
        matched = (
            self.db.query(
                func.count(ProductCost.id).label("total"),
                func.sum(
                    case(
                        (and_(ProductCost.ean.isnot(None), ProductCost.ean != ""), 1),
                        else_=0,
                    )
                ).label("with_gtin"),
            )
            .join(
                ShopifyInventory,
                func.upper(ProductCost.vendor_sku) == func.upper(ShopifyInventory.sku),
            )
            .filter(ShopifyInventory.shopify_product_id.in_(active_pids))
            .first()
        )
        matched_total = int(matched.total or 0) if matched else 0
        with_gtin = int(matched.with_gtin or 0) if matched else 0
        missing_gtin = matched_total - with_gtin
        return with_gtin, missing_gtin, matched_total

    def _get_product_revenue_map(self):
        """Build product_id/title → 30d revenue map from ShopifyOrderItem."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=30)
            rows = (
                self.db.query(
                    ShopifyOrderItem.title,
                    func.sum(ShopifyOrderItem.total_price).label("revenue"),
                    func.sum(ShopifyOrderItem.quantity).label("units"),
                )
                .filter(ShopifyOrderItem.order_date >= cutoff)
                .group_by(ShopifyOrderItem.title)
                .all()
            )
            rev_map = {}
            for r in rows:
                if r.title:
                    rev_map[r.title.lower()] = {
                        "revenue": float(r.revenue or 0),
                        "units": int(r.units or 0),
                    }
            return rev_map
        except Exception as e:
            logger.error(f"Revenue map failed: {e}")
            return {}

    # ------------------------------------------------------------------
    # Estimated KPIs from Shopify + CompetitivePricing (no GMC required)
    # ------------------------------------------------------------------

    def _compute_estimated_kpis(self):
        """Estimate executive KPIs from Shopify catalog + CompetitivePricing when GMC data is empty.

        All counts are based on the active Shopify product universe.
        """
        try:
            active_pids, total = self._get_active_product_universe()

            # Products with pricing issues (below minimum, losing money)
            latest_pricing = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            price_issues = 0
            losing_money = 0
            below_min = 0
            est_risk = 0
            if latest_pricing:
                below_min = (
                    self.db.query(func.count(CompetitivePricing.id))
                    .filter(CompetitivePricing.pricing_date == latest_pricing)
                    .filter(CompetitivePricing.is_below_minimum == True)
                    .scalar()
                ) or 0
                losing_money = (
                    self.db.query(func.count(CompetitivePricing.id))
                    .filter(CompetitivePricing.pricing_date == latest_pricing)
                    .filter(CompetitivePricing.is_losing_money == True)
                    .scalar()
                ) or 0
                price_issues = below_min + losing_money

                # Revenue at risk from below-minimum products
                risk_rows = (
                    self.db.query(
                        CompetitivePricing.title,
                        CompetitivePricing.current_price,
                    )
                    .filter(CompetitivePricing.pricing_date == latest_pricing)
                    .filter(CompetitivePricing.is_below_minimum == True)
                    .all()
                )
                rev_map = self._get_product_revenue_map()
                est_risk = sum(
                    rev_map.get((r.title or "").lower(), {}).get("revenue", 0)
                    for r in risk_rows
                )

            # Out of stock = can't serve on Shopping (active + deny policy only)
            out_of_stock = (
                self.db.query(func.count(ShopifyInventory.id))
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_policy != "continue",
                    ShopifyInventory.inventory_quantity <= 0,
                )
                .scalar()
            ) or 0

            # Missing GTIN — scoped to active product universe
            _with_gtin, missing_gtin, _matched = self._count_gtin_in_active_universe(active_pids)
            missing_gtin = min(missing_gtin, total)  # cap at universe size

            # Missing product type
            missing_product_type = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .filter(or_(ShopifyProduct.product_type.is_(None), ShopifyProduct.product_type == ""))
                .scalar()
            ) or 0

            # ---- Distinct products with any issue ----
            # Collect shopify_product_id sets for each issue category
            issue_pids = set()

            # Price issues (below_min, losing_money) → via variant_id join
            if latest_pricing:
                price_pid_rows = (
                    self.db.query(ShopifyInventory.shopify_product_id)
                    .join(
                        CompetitivePricing,
                        CompetitivePricing.variant_id == ShopifyInventory.shopify_variant_id,
                    )
                    .filter(
                        CompetitivePricing.pricing_date == latest_pricing,
                        or_(
                            CompetitivePricing.is_below_minimum == True,
                            CompetitivePricing.is_losing_money == True,
                        ),
                    )
                    .distinct()
                    .all()
                )
                issue_pids.update(r[0] for r in price_pid_rows if r[0])

            # Out of stock
            oos_pid_rows = (
                self.db.query(ShopifyInventory.shopify_product_id)
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_policy != "continue",
                    ShopifyInventory.inventory_quantity <= 0,
                )
                .distinct()
                .all()
            )
            issue_pids.update(r[0] for r in oos_pid_rows if r[0])

            # Missing GTIN → via ProductCost sku join
            missing_gtin_pid_rows = (
                self.db.query(ShopifyInventory.shopify_product_id)
                .join(
                    ProductCost,
                    func.upper(ProductCost.vendor_sku) == func.upper(ShopifyInventory.sku),
                )
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    or_(ProductCost.ean.is_(None), ProductCost.ean == ""),
                )
                .distinct()
                .all()
            )
            issue_pids.update(r[0] for r in missing_gtin_pid_rows if r[0])

            # Missing product type
            missing_type_pid_rows = (
                self.db.query(ShopifyProduct.shopify_product_id)
                .filter(
                    ShopifyProduct.status == "active",
                    or_(ShopifyProduct.product_type.is_(None), ShopifyProduct.product_type == ""),
                )
                .all()
            )
            issue_pids.update(r[0] for r in missing_type_pid_rows if r[0])

            products_with_issues = min(len(issue_pids), total)

            # Eligible rate = products without any issue / total
            eligible_rate = round((total - products_with_issues) / max(total, 1) * 100, 1)

            return {
                "total_products": total,
                "active_products": total,
                "disapproved_products": below_min,
                "pending_products": 0,
                "expiring_products": out_of_stock,
                "products_with_issues": products_with_issues,
                "approval_rate": round((total - below_min) / max(total, 1) * 100, 1),
                "eligible_rate": eligible_rate,
                "approval_rate_estimated": True,
                "est_revenue_at_risk": round(est_risk, 2),
                "missing_gtin_count": missing_gtin,
                "missing_product_type_count": missing_product_type,
                "price_issue_count": price_issues,
                "estimates_only": True,
            }
        except Exception as e:
            logger.error(f"Estimated KPIs failed: {e}")
            return {k: 0 for k in [
                "total_products", "active_products", "disapproved_products",
                "pending_products", "expiring_products", "products_with_issues",
                "approval_rate", "eligible_rate", "est_revenue_at_risk",
                "missing_gtin_count", "missing_product_type_count", "price_issue_count",
            ]}

    def _compute_estimated_issues(self):
        """Derive feed issues from CompetitivePricing + Shopify data when GMC is empty.

        All counts scoped to the active Shopify product universe.
        """
        try:
            active_pids, total_active = self._get_active_product_universe()

            latest = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            if not latest:
                return []

            rev_map = self._get_product_revenue_map()
            results = []

            # 1. Below minimum price
            below_min = (
                self.db.query(func.count(CompetitivePricing.id))
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_below_minimum == True)
                .scalar()
            ) or 0
            if below_min > 0:
                titles = (
                    self.db.query(CompetitivePricing.title)
                    .filter(CompetitivePricing.pricing_date == latest)
                    .filter(CompetitivePricing.is_below_minimum == True)
                    .distinct()
                    .all()
                )
                risk = sum(rev_map.get((t.title or "").lower(), {}).get("revenue", 0) for t in titles)
                results.append({
                    "issue_code": "price_below_minimum",
                    "description": "Price below minimum advertised price",
                    "severity": "disapproved",
                    "products": below_min,
                    "est_revenue_risk": round(risk, 2),
                    "color": "#b5342a",
                })

            # 2. Losing money (negative margin)
            losing = (
                self.db.query(func.count(CompetitivePricing.id))
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_losing_money == True)
                .scalar()
            ) or 0
            if losing > 0:
                titles = (
                    self.db.query(CompetitivePricing.title)
                    .filter(CompetitivePricing.pricing_date == latest)
                    .filter(CompetitivePricing.is_losing_money == True)
                    .distinct()
                    .all()
                )
                risk = sum(rev_map.get((t.title or "").lower(), {}).get("revenue", 0) for t in titles)
                results.append({
                    "issue_code": "negative_margin",
                    "description": "Product selling at a loss (negative margin)",
                    "severity": "disapproved",
                    "products": losing,
                    "est_revenue_risk": round(risk, 2),
                    "color": "#b5342a",
                })

            # 3. Above RRP
            above_rrp = (
                self.db.query(func.count(CompetitivePricing.id))
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_above_rrp == True)
                .scalar()
            ) or 0
            if above_rrp > 0:
                results.append({
                    "issue_code": "price_above_rrp",
                    "description": "Price above recommended retail price",
                    "severity": "demoted",
                    "products": above_rrp,
                    "est_revenue_risk": 0,
                    "color": "#c49a4a",
                })

            # 4. Out of stock products (only deny-policy, active products)
            oos = (
                self.db.query(func.count(ShopifyInventory.id))
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_policy != "continue",
                    ShopifyInventory.inventory_quantity <= 0,
                )
                .scalar()
            ) or 0
            if oos > 0:
                results.append({
                    "issue_code": "out_of_stock",
                    "description": "Product out of stock (cannot serve on Shopping)",
                    "severity": "disapproved",
                    "products": oos,
                    "est_revenue_risk": 0,
                    "color": "#b5342a",
                })

            # 5. Missing GTIN/EAN — scoped to active product universe
            _with_gtin, missing_gtin, _matched = self._count_gtin_in_active_universe(active_pids)
            missing_gtin = min(missing_gtin, total_active)
            if missing_gtin > 0:
                results.append({
                    "issue_code": "missing_gtin",
                    "description": "Missing GTIN / EAN barcode (required for Shopping)",
                    "severity": "demoted",
                    "products": missing_gtin,
                    "est_revenue_risk": 0,
                    "color": "#c49a4a",
                })

            # 6. Missing product type
            missing_type = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .filter(or_(ShopifyProduct.product_type.is_(None), ShopifyProduct.product_type == ""))
                .scalar()
            ) or 0
            if missing_type > 0:
                results.append({
                    "issue_code": "missing_product_type",
                    "description": "Missing product type / category (affects Shopping targeting)",
                    "severity": "demoted",
                    "products": missing_type,
                    "est_revenue_risk": 0,
                    "color": "#c49a4a",
                })

            # 7. Oversold (negative inventory) - active products only
            oversold = (
                self.db.query(func.count(ShopifyInventory.id))
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity < 0,
                )
                .scalar()
            ) or 0
            if oversold > 0:
                results.append({
                    "issue_code": "oversold",
                    "description": "Oversold product (negative inventory quantity)",
                    "severity": "disapproved",
                    "products": oversold,
                    "est_revenue_risk": 0,
                    "color": "#b5342a",
                })

            return sorted(results, key=lambda x: x["products"], reverse=True)
        except Exception as e:
            logger.error(f"Estimated issues failed: {e}")
            return []

    def _compute_estimated_eligibility(self):
        """Estimate eligibility breakdown from Shopify data when GMC is empty."""
        try:
            total_active = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .scalar()
            ) or 0

            latest = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            below_min = 0
            if latest:
                below_min = (
                    self.db.query(func.count(distinct(CompetitivePricing.title)))
                    .filter(CompetitivePricing.pricing_date == latest)
                    .filter(CompetitivePricing.is_below_minimum == True)
                    .scalar()
                ) or 0

            active_pids_elig = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == "active"
            ).subquery()
            oos = (
                self.db.query(func.count(ShopifyInventory.id))
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids_elig),
                    ShopifyInventory.inventory_policy != "continue",
                    ShopifyInventory.inventory_quantity <= 0,
                )
                .scalar()
            ) or 0

            # Estimate: eligible = active - OOS - price violations
            eligible = max(0, total_active - below_min - oos)
            total = total_active

            results = []
            if eligible > 0:
                results.append({
                    "status": "eligible",
                    "count": eligible,
                    "pct": round(eligible / max(total, 1) * 100, 1),
                    "est_revenue_risk": 0,
                    "color": "#1a7a3a",
                })
            if below_min > 0:
                results.append({
                    "status": "price violation",
                    "count": below_min,
                    "pct": round(below_min / max(total, 1) * 100, 1),
                    "est_revenue_risk": 0,
                    "color": "#b5342a",
                })
            if oos > 0:
                results.append({
                    "status": "out of stock",
                    "count": oos,
                    "pct": round(oos / max(total, 1) * 100, 1),
                    "est_revenue_risk": 0,
                    "color": "#c49a4a",
                })

            return results
        except Exception as e:
            logger.error(f"Estimated eligibility failed: {e}")
            return []

    def _compute_estimated_category_risk(self):
        """Brand/vendor risk from CompetitivePricing + ShopifyInventory when GMC is empty."""
        try:
            latest = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            if not latest:
                return {"by_vendor": [], "by_type": []}

            rows = (
                self.db.query(
                    CompetitivePricing.vendor,
                    func.count().label("total"),
                    func.sum(
                        case(
                            (CompetitivePricing.is_below_minimum == True, 1),
                            else_=0,
                        )
                    ).label("below_min"),
                    func.sum(
                        case(
                            (CompetitivePricing.is_losing_money == True, 1),
                            else_=0,
                        )
                    ).label("losing"),
                    func.sum(
                        case(
                            (CompetitivePricing.is_above_rrp == True, 1),
                            else_=0,
                        )
                    ).label("above_rrp"),
                )
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.vendor.isnot(None))
                .group_by(CompetitivePricing.vendor)
                .order_by(desc("total"))
                .limit(25)
                .all()
            )

            by_vendor = []
            for r in rows:
                total = r.total or 1
                issues = int(r.below_min or 0) + int(r.losing or 0)
                ok = total - issues
                by_vendor.append({
                    "name": r.vendor,
                    "total": r.total,
                    "active_pct": round(ok / total * 100, 1),
                    "issues": issues,
                    "below_min": int(r.below_min or 0),
                    "losing": int(r.losing or 0),
                    "above_rrp": int(r.above_rrp or 0),
                })

            by_vendor.sort(key=lambda x: x["issues"], reverse=True)
            return {"by_vendor": by_vendor[:20], "by_type": []}
        except Exception as e:
            logger.error(f"Estimated category risk failed: {e}")
            return {"by_vendor": [], "by_type": []}

    def _compute_estimated_at_risk_products(self, limit=50):
        """Products at risk from pricing violations when GMC is empty."""
        try:
            latest = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            if not latest:
                return []

            rev_map = self._get_product_revenue_map()

            # Get products below minimum price (sorted by how far below)
            drifts = (
                self.db.query(
                    CompetitivePricing.variant_sku,
                    CompetitivePricing.title,
                    CompetitivePricing.vendor,
                    CompetitivePricing.current_price,
                    CompetitivePricing.minimum_price,
                    CompetitivePricing.profit_margin_pct,
                    CompetitivePricing.is_losing_money,
                )
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_below_minimum == True)
                .order_by(CompetitivePricing.profit_margin_pct.asc())
                .limit(limit)
                .all()
            )

            results = []
            for d in drifts:
                rev_info = rev_map.get((d.title or "").lower(), {})
                revenue = rev_info.get("revenue", 0)
                issue = "negative_margin" if d.is_losing_money else "price_below_minimum"
                results.append({
                    "product_id": d.variant_sku or "",
                    "offer_id": d.variant_sku or "",
                    "title": d.title or "",
                    "status": "disapproved" if d.is_losing_money else "demoted",
                    "issue_count": 1,
                    "top_issue": issue,
                    "days_affected": 0,
                    "est_revenue_loss": round(revenue, 2),
                    "monthly_revenue": round(revenue, 2),
                    "issues": [{
                        "code": issue,
                        "severity": "disapproved" if d.is_losing_money else "demoted",
                        "days_affected": 0,
                    }],
                })

            results.sort(key=lambda x: x["est_revenue_loss"], reverse=True)
            return results[:limit]
        except Exception as e:
            logger.error(f"Estimated at-risk products failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Feed Health Score (0-100)
    # ------------------------------------------------------------------

    def _compute_feed_health_score(self, kpis, readiness, gtin):
        """
        Composite score weighted:
          Active % (40%) + Issue severity (30%) + Feed readiness (30%)
        """
        try:
            # Active % component (from GMC or readiness)
            total = kpis.get("total_products", 0)
            active = kpis.get("active_products", 0)
            if total > 0:
                active_score = (active / total) * 100
            else:
                # Fall back to readiness
                active_score = readiness.get("completeness_score", 70)

            # Issue severity component
            disapproved = kpis.get("disapproved_products", 0)
            if total > 0:
                issue_score = max(0, 100 - (disapproved / total * 200))
            else:
                issue_score = 80  # default when no GMC data

            # Feed readiness component
            readiness_score = readiness.get("completeness_score", 70)

            score = round(active_score * 0.4 + issue_score * 0.3 + readiness_score * 0.3)
            return max(0, min(100, score))
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Pulse narrative
    # ------------------------------------------------------------------

    def _compute_pulse(self, kpis, health_score, has_gmc, readiness):
        """Generate narrative + status chip."""
        try:
            if health_score >= 80:
                status = "Healthy"
            elif health_score >= 60:
                status = "Needs Attention"
            elif health_score >= 40:
                status = "At Risk"
            else:
                status = "Critical"

            if has_gmc:
                total = kpis.get("total_products", 0)
                active = kpis.get("active_products", 0)
                disapproved = kpis.get("disapproved_products", 0)
                revenue_risk = kpis.get("est_revenue_at_risk", 0)

                narrative = (
                    f"{active:,} of {total:,} products active in Google Shopping"
                )
                if disapproved > 0:
                    narrative += f" — {disapproved:,} disapproved"
                if revenue_risk > 0:
                    narrative += f", est. ${revenue_risk:,.0f} revenue at risk"

                pro_narrative = (
                    f"Feed health score: {health_score}/100. "
                    f"{total:,} products in feed: {active:,} active ({kpis.get('approval_rate', 0):.1f}%), "
                    f"{disapproved:,} disapproved, {kpis.get('pending_products', 0):,} pending. "
                    f"Issues affecting {kpis.get('products_with_issues', 0):,} products. "
                    f"Est. revenue at risk: ${revenue_risk:,.0f}."
                )
            else:
                active_catalog = readiness.get("active_products", 0)
                gtin_pct = readiness.get("gtin_coverage_pct", 0)
                price_issues = kpis.get("price_issue_count", 0)
                revenue_risk = kpis.get("est_revenue_at_risk", 0)
                missing_gtin = kpis.get("missing_gtin_count", 0)

                parts = [f"{active_catalog:,} active products (estimated)"]
                if price_issues > 0:
                    parts.append(f"{price_issues:,} with pricing violations")
                if missing_gtin > 0:
                    parts.append(f"{missing_gtin:,} missing GTIN")
                if revenue_risk > 0:
                    parts.append(f"est. ${revenue_risk:,.0f} revenue at risk")
                parts.append(f"{gtin_pct}% GTIN coverage")
                narrative = " — ".join(parts)

                pro_narrative = (
                    f"Feed health score: {health_score}/100. "
                    f"Active catalog: {active_catalog:,} products. "
                    f"Pricing violations: {price_issues:,} products below minimum. "
                    f"GTIN coverage: {gtin_pct}% of active products. "
                    f"Missing GTIN: {missing_gtin:,}. "
                    f"Revenue at risk from price violations: ${revenue_risk:,.0f}. "
                    f"GMC data not connected — using Shopify + Cost + Caprice estimates."
                )

            return {
                "narrative": narrative,
                "status": status,
                "pro_narrative": pro_narrative,
            }
        except Exception as e:
            logger.error(f"Pulse failed: {e}")
            return {"narrative": "Loading…", "status": "Loading", "pro_narrative": ""}

    # ------------------------------------------------------------------
    # Executive KPIs (GMC)
    # ------------------------------------------------------------------

    def _compute_executive_kpis(self, snapshot):
        """KPI values from latest GMC snapshot, or estimated from Shopify/CompetitivePricing."""
        try:
            if not snapshot:
                return self._compute_estimated_kpis()

            acct = (
                self.db.query(MerchantCenterAccountStatus)
                .filter(MerchantCenterAccountStatus.snapshot_date == snapshot)
                .first()
            )
            if not acct:
                return {k: 0 for k in [
                    "total_products", "active_products", "disapproved_products",
                    "pending_products", "expiring_products", "products_with_issues",
                    "approval_rate", "eligible_rate", "est_revenue_at_risk",
                    "missing_gtin_count", "price_issue_count",
                ]}

            # Count products with issues
            issue_products = (
                self.db.query(func.count(distinct(MerchantCenterProductStatus.product_id)))
                .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                .filter(MerchantCenterProductStatus.has_issues == True)
                .scalar()
            ) or 0

            # Count specific issue types
            missing_gtin = (
                self.db.query(func.count(distinct(MerchantCenterDisapproval.product_id)))
                .filter(MerchantCenterDisapproval.snapshot_date == snapshot)
                .filter(MerchantCenterDisapproval.issue_code.ilike("%gtin%"))
                .scalar()
            ) or 0

            price_issues = (
                self.db.query(func.count(distinct(MerchantCenterDisapproval.product_id)))
                .filter(MerchantCenterDisapproval.snapshot_date == snapshot)
                .filter(
                    or_(
                        MerchantCenterDisapproval.issue_code.ilike("%price%"),
                        MerchantCenterDisapproval.issue_code.ilike("%cost%"),
                    )
                )
                .scalar()
            ) or 0

            # Estimate revenue at risk
            rev_map = self._get_product_revenue_map()
            disapproved_titles = (
                self.db.query(MerchantCenterProductStatus.title)
                .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                .filter(MerchantCenterProductStatus.approval_status != "approved")
                .all()
            )
            est_risk = sum(
                rev_map.get((t.title or "").lower(), {}).get("revenue", 0)
                for t in disapproved_titles
            )

            total_gmc = acct.total_products or 0
            eligible_rate = round(
                (total_gmc - issue_products) / max(total_gmc, 1) * 100, 1
            )

            return {
                "total_products": total_gmc,
                "active_products": acct.approved_count or 0,
                "disapproved_products": acct.disapproved_count or 0,
                "pending_products": acct.pending_count or 0,
                "expiring_products": acct.expiring_count or 0,
                "products_with_issues": issue_products,
                "approval_rate": float(acct.approval_rate or 0),
                "eligible_rate": eligible_rate,
                "est_revenue_at_risk": round(est_risk, 2),
                "missing_gtin_count": missing_gtin,
                "price_issue_count": price_issues,
            }
        except Exception as e:
            logger.error(f"Executive KPIs failed: {e}")
            return {k: 0 for k in [
                "total_products", "active_products", "disapproved_products",
                "pending_products", "expiring_products", "products_with_issues",
                "approval_rate", "eligible_rate", "est_revenue_at_risk",
                "missing_gtin_count", "price_issue_count",
            ]}

    # ------------------------------------------------------------------
    # Eligibility breakdown
    # ------------------------------------------------------------------

    def _compute_eligibility_breakdown(self, snapshot):
        """Status breakdown: active, disapproved, pending, expiring."""
        try:
            if not snapshot:
                return self._compute_estimated_eligibility()

            rows = (
                self.db.query(
                    MerchantCenterProductStatus.approval_status,
                    func.count().label("count"),
                )
                .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                .group_by(MerchantCenterProductStatus.approval_status)
                .all()
            )
            total = sum(r.count for r in rows) or 1

            rev_map = self._get_product_revenue_map()

            results = []
            for r in rows:
                # Estimate revenue at risk for non-approved
                if r.approval_status != "approved":
                    titles = (
                        self.db.query(MerchantCenterProductStatus.title)
                        .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                        .filter(MerchantCenterProductStatus.approval_status == r.approval_status)
                        .all()
                    )
                    risk = sum(
                        rev_map.get((t.title or "").lower(), {}).get("revenue", 0)
                        for t in titles
                    )
                else:
                    risk = 0

                color_map = {
                    "approved": "#1a7a3a",
                    "disapproved": "#b5342a",
                    "pending": "#c49a4a",
                    "expiring": "#e88c3a",
                }
                results.append({
                    "status": r.approval_status,
                    "count": r.count,
                    "pct": round(r.count / total * 100, 1),
                    "est_revenue_risk": round(risk, 2),
                    "color": color_map.get(r.approval_status, "#6b7280"),
                })

            return sorted(results, key=lambda x: x["count"], reverse=True)
        except Exception as e:
            logger.error(f"Eligibility breakdown failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Issue breakdown
    # ------------------------------------------------------------------

    def _compute_issue_breakdown(self, snapshot):
        """Top issues by product count and estimated revenue impact."""
        try:
            if not snapshot:
                return self._compute_estimated_issues()

            rows = (
                self.db.query(
                    MerchantCenterDisapproval.issue_code,
                    MerchantCenterDisapproval.issue_description,
                    MerchantCenterDisapproval.issue_severity,
                    func.count(distinct(MerchantCenterDisapproval.product_id)).label("products"),
                )
                .filter(MerchantCenterDisapproval.snapshot_date == snapshot)
                .filter(MerchantCenterDisapproval.is_resolved == False)
                .group_by(
                    MerchantCenterDisapproval.issue_code,
                    MerchantCenterDisapproval.issue_description,
                    MerchantCenterDisapproval.issue_severity,
                )
                .order_by(desc("products"))
                .limit(20)
                .all()
            )

            rev_map = self._get_product_revenue_map()
            results = []
            for r in rows:
                # Get titles affected by this issue
                titles = (
                    self.db.query(MerchantCenterDisapproval.title)
                    .filter(MerchantCenterDisapproval.snapshot_date == snapshot)
                    .filter(MerchantCenterDisapproval.issue_code == r.issue_code)
                    .distinct()
                    .all()
                )
                risk = sum(
                    rev_map.get((t.title or "").lower(), {}).get("revenue", 0)
                    for t in titles
                )

                severity_color = {
                    "disapproved": "#b5342a",
                    "demoted": "#c49a4a",
                    "unaffected": "#6b7280",
                }
                results.append({
                    "issue_code": r.issue_code,
                    "description": r.issue_description or r.issue_code,
                    "severity": r.issue_severity or "unknown",
                    "products": r.products,
                    "est_revenue_risk": round(risk, 2),
                    "color": severity_color.get(r.issue_severity, "#6b7280"),
                })

            return sorted(results, key=lambda x: x["est_revenue_risk"], reverse=True)
        except Exception as e:
            logger.error(f"Issue breakdown failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Category / brand risk
    # ------------------------------------------------------------------

    def _compute_category_risk(self, snapshot):
        """Category and brand risk from GMC disapprovals + Shopify product data."""
        try:
            if not snapshot:
                return self._compute_estimated_category_risk()

            # Get vendors of disapproved products by joining with ShopifyInventory
            disapproved = (
                self.db.query(
                    MerchantCenterProductStatus.product_id,
                    MerchantCenterProductStatus.title,
                    MerchantCenterProductStatus.approval_status,
                    MerchantCenterProductStatus.has_issues,
                )
                .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                .all()
            )

            # Build a title → vendor lookup from ShopifyInventory (active products only)
            active_pids_vendor = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()
            vendor_map = {}
            inv_rows = (
                self.db.query(ShopifyInventory.title, ShopifyInventory.vendor)
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids_vendor),
                    ShopifyInventory.vendor.isnot(None),
                )
                .all()
            )
            for r in inv_rows:
                if r.title:
                    vendor_map[r.title.lower()] = r.vendor

            # Aggregate by vendor
            vendor_stats = defaultdict(lambda: {"total": 0, "active": 0, "issues": 0})
            for p in disapproved:
                vendor = vendor_map.get((p.title or "").lower(), "Unknown")
                vendor_stats[vendor]["total"] += 1
                if p.approval_status == "approved":
                    vendor_stats[vendor]["active"] += 1
                if p.has_issues:
                    vendor_stats[vendor]["issues"] += 1

            rev_map = self._get_product_revenue_map()
            by_vendor = []
            for vendor, stats in vendor_stats.items():
                total = stats["total"] or 1
                by_vendor.append({
                    "name": vendor,
                    "total": stats["total"],
                    "active_pct": round(stats["active"] / total * 100, 1),
                    "issues": stats["issues"],
                })

            by_vendor.sort(key=lambda x: x["issues"], reverse=True)

            return {"by_vendor": by_vendor[:20], "by_type": []}
        except Exception as e:
            logger.error(f"Category risk failed: {e}")
            return {"by_vendor": [], "by_type": []}

    # ------------------------------------------------------------------
    # At-risk products
    # ------------------------------------------------------------------

    def _compute_at_risk_products(self, snapshot, limit=50):
        """Top products by revenue risk — disapproved or with critical issues."""
        try:
            if not snapshot:
                return self._compute_estimated_at_risk_products(limit)

            products = (
                self.db.query(
                    MerchantCenterProductStatus.product_id,
                    MerchantCenterProductStatus.offer_id,
                    MerchantCenterProductStatus.title,
                    MerchantCenterProductStatus.approval_status,
                    MerchantCenterProductStatus.issue_count,
                    MerchantCenterProductStatus.critical_issue_count,
                )
                .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                .filter(
                    or_(
                        MerchantCenterProductStatus.approval_status != "approved",
                        MerchantCenterProductStatus.critical_issue_count > 0,
                    )
                )
                .all()
            )

            if not products:
                return []

            rev_map = self._get_product_revenue_map()

            # Get issue details for these products
            issue_map = defaultdict(list)
            issues = (
                self.db.query(
                    MerchantCenterDisapproval.product_id,
                    MerchantCenterDisapproval.issue_code,
                    MerchantCenterDisapproval.issue_severity,
                    MerchantCenterDisapproval.first_seen_date,
                )
                .filter(MerchantCenterDisapproval.snapshot_date == snapshot)
                .filter(MerchantCenterDisapproval.is_resolved == False)
                .all()
            )
            for iss in issues:
                days_affected = 0
                if iss.first_seen_date:
                    days_affected = (date.today() - iss.first_seen_date).days
                issue_map[iss.product_id].append({
                    "code": iss.issue_code,
                    "severity": iss.issue_severity,
                    "days_affected": days_affected,
                })

            results = []
            for p in products:
                rev_info = rev_map.get((p.title or "").lower(), {})
                revenue = rev_info.get("revenue", 0)
                prod_issues = issue_map.get(p.product_id, [])
                top_issue = prod_issues[0]["code"] if prod_issues else "unknown"
                max_days = max((i["days_affected"] for i in prod_issues), default=0)

                results.append({
                    "product_id": p.product_id,
                    "offer_id": p.offer_id,
                    "title": p.title or "",
                    "status": p.approval_status,
                    "issue_count": p.issue_count or 0,
                    "top_issue": top_issue,
                    "days_affected": max_days,
                    "est_revenue_loss": round(revenue / 30 * max_days, 2) if max_days > 0 else round(revenue, 2),
                    "monthly_revenue": round(revenue, 2),
                    "issues": prod_issues[:3],
                })

            results.sort(key=lambda x: x["est_revenue_loss"], reverse=True)
            return results[:limit]
        except Exception as e:
            logger.error(f"At-risk products failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Approval trend (historical)
    # ------------------------------------------------------------------

    def _compute_approval_trend(self, days=30):
        """Approval rate over time from account status snapshots."""
        try:
            cutoff = date.today() - timedelta(days=days)
            rows = (
                self.db.query(
                    MerchantCenterAccountStatus.snapshot_date,
                    MerchantCenterAccountStatus.total_products,
                    MerchantCenterAccountStatus.approved_count,
                    MerchantCenterAccountStatus.disapproved_count,
                    MerchantCenterAccountStatus.pending_count,
                    MerchantCenterAccountStatus.approval_rate,
                )
                .filter(MerchantCenterAccountStatus.snapshot_date >= cutoff)
                .order_by(MerchantCenterAccountStatus.snapshot_date)
                .all()
            )

            return [
                {
                    "date": str(r.snapshot_date),
                    "total": r.total_products or 0,
                    "approved": r.approved_count or 0,
                    "disapproved": r.disapproved_count or 0,
                    "pending": r.pending_count or 0,
                    "approval_rate": round(float(r.approval_rate or 0), 1),
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Approval trend failed: {e}")
            return []

    # ==================================================================
    # FEED READINESS (works from Shopify data — always available)
    # ==================================================================

    def _compute_feed_readiness(self):
        """
        Catalog completeness analysis from ShopifyProduct.
        Works without GMC data.  All counts based on active products only.
        """
        try:
            active_pids, active = self._get_active_product_universe()

            # Products with title
            with_title = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .filter(ShopifyProduct.title.isnot(None))
                .filter(ShopifyProduct.title != "")
                .scalar()
            ) or 0

            # Products with description
            with_desc = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .filter(ShopifyProduct.body_html.isnot(None))
                .filter(ShopifyProduct.body_html != "")
                .scalar()
            ) or 0

            # Products with vendor
            with_vendor = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .filter(ShopifyProduct.vendor.isnot(None))
                .filter(ShopifyProduct.vendor != "")
                .scalar()
            ) or 0

            # Products with product type
            with_type = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .filter(ShopifyProduct.product_type.isnot(None))
                .filter(ShopifyProduct.product_type != "")
                .scalar()
            ) or 0

            # Products with images
            with_images = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(ShopifyProduct.status == "active")
                .filter(ShopifyProduct.images.isnot(None))
                .scalar()
            ) or 0

            # GTIN/EAN — scoped to active product universe
            with_gtin, _missing, matched = self._count_gtin_in_active_universe(active_pids)
            # Express GTIN coverage as % of active products
            gtin_pct = min(round(with_gtin / max(active, 1) * 100, 1), 100.0)

            a = active or 1
            completeness = round(
                (with_title / a * 20) +
                (with_desc / a * 20) +
                (with_vendor / a * 20) +
                (with_type / a * 20) +
                (min(gtin_pct, 100) / 100 * 20),
            1)

            fields = [
                {"field": "Title", "count": with_title, "pct": round(with_title / a * 100, 1), "status": "ok" if with_title / a > 0.95 else "warning"},
                {"field": "Description", "count": with_desc, "pct": round(with_desc / a * 100, 1), "status": "ok" if with_desc / a > 0.8 else "warning"},
                {"field": "Vendor / Brand", "count": with_vendor, "pct": round(with_vendor / a * 100, 1), "status": "ok" if with_vendor / a > 0.9 else "warning"},
                {"field": "Product Type", "count": with_type, "pct": round(with_type / a * 100, 1), "status": "ok" if with_type / a > 0.8 else "warning"},
                {"field": "Images", "count": with_images, "pct": round(with_images / a * 100, 1), "status": "ok" if with_images / a > 0.9 else "warning"},
                {"field": "GTIN / EAN", "count": with_gtin, "pct": gtin_pct, "status": "ok" if gtin_pct > 80 else "critical" if gtin_pct < 30 else "warning"},
            ]

            return {
                "total_products": active,
                "active_products": active,
                "gtin_coverage_pct": gtin_pct,
                "completeness_score": round(completeness),
                "fields": fields,
            }
        except Exception as e:
            logger.error(f"Feed readiness failed: {e}")
            return {
                "total_products": 0, "active_products": 0,
                "gtin_coverage_pct": 0, "completeness_score": 0, "fields": [],
            }

    # ------------------------------------------------------------------
    # GTIN coverage
    # ------------------------------------------------------------------

    def _compute_gtin_coverage(self):
        """GTIN/EAN coverage from ProductCost by vendor — scoped to active products.

        Joins ProductCost → ShopifyInventory (SKU match) → active product filter.
        """
        try:
            active_pids, _total = self._get_active_product_universe()

            rows = (
                self.db.query(
                    ProductCost.vendor,
                    func.count(ProductCost.id).label("total"),
                    func.sum(
                        case(
                            (and_(ProductCost.ean.isnot(None), ProductCost.ean != ""), 1),
                            else_=0,
                        )
                    ).label("with_gtin"),
                )
                .join(
                    ShopifyInventory,
                    func.upper(ProductCost.vendor_sku) == func.upper(ShopifyInventory.sku),
                )
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ProductCost.vendor.isnot(None),
                )
                .group_by(ProductCost.vendor)
                .order_by(desc("total"))
                .limit(25)
                .all()
            )

            results = []
            for r in rows:
                total = r.total or 1
                with_gtin = int(r.with_gtin or 0)
                pct = round(with_gtin / total * 100, 1)
                results.append({
                    "vendor": r.vendor,
                    "total": r.total,
                    "with_gtin": with_gtin,
                    "missing_gtin": r.total - with_gtin,
                    "coverage_pct": pct,
                    "status": "ok" if pct >= 80 else "critical" if pct < 30 else "warning",
                })

            return sorted(results, key=lambda x: x["total"], reverse=True)
        except Exception as e:
            logger.error(f"GTIN coverage failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Price drift
    # ------------------------------------------------------------------

    def _compute_price_drift(self):
        """Price consistency analysis from CompetitivePricing."""
        try:
            # Get latest pricing date
            latest = (
                self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            )
            if not latest:
                return {"summary": {}, "top_drifts": []}

            # Count price issues
            total = (
                self.db.query(func.count(CompetitivePricing.id))
                .filter(CompetitivePricing.pricing_date == latest)
                .scalar()
            ) or 1

            below_min = (
                self.db.query(func.count(CompetitivePricing.id))
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_below_minimum == True)
                .scalar()
            ) or 0

            above_rrp = (
                self.db.query(func.count(CompetitivePricing.id))
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_above_rrp == True)
                .scalar()
            ) or 0

            losing_money = (
                self.db.query(func.count(CompetitivePricing.id))
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_losing_money == True)
                .scalar()
            ) or 0

            # Top price drifts (biggest gap between current and minimum)
            drifts = (
                self.db.query(
                    CompetitivePricing.variant_sku,
                    CompetitivePricing.title,
                    CompetitivePricing.vendor,
                    CompetitivePricing.current_price,
                    CompetitivePricing.minimum_price,
                    CompetitivePricing.rrp,
                    CompetitivePricing.lowest_competitor_price,
                    CompetitivePricing.profit_margin_pct,
                )
                .filter(CompetitivePricing.pricing_date == latest)
                .filter(CompetitivePricing.is_below_minimum == True)
                .order_by(CompetitivePricing.price_vs_minimum)
                .limit(20)
                .all()
            )

            return {
                "pricing_date": str(latest),
                "summary": {
                    "total_tracked": total,
                    "below_minimum": below_min,
                    "above_rrp": above_rrp,
                    "losing_money": losing_money,
                    "below_min_pct": round(below_min / total * 100, 1),
                },
                "top_drifts": [
                    {
                        "sku": d.variant_sku or "",
                        "title": (d.title or "")[:60],
                        "vendor": d.vendor or "",
                        "current_price": float(d.current_price or 0),
                        "minimum_price": float(d.minimum_price or 0),
                        "rrp": float(d.rrp or 0),
                        "lowest_competitor": float(d.lowest_competitor_price or 0),
                        "margin_pct": float(d.profit_margin_pct or 0),
                    }
                    for d in drifts
                ],
            }
        except Exception as e:
            logger.error(f"Price drift failed: {e}")
            return {"summary": {}, "top_drifts": []}

    # ------------------------------------------------------------------
    # Availability health
    # ------------------------------------------------------------------

    def _compute_availability_health(self):
        """Inventory availability from ShopifyInventory (deny-policy, active products only)."""
        try:
            # Only count products that actually block purchase when OOS
            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == "active"
            ).subquery()
            base_q = self.db.query(ShopifyInventory).filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.inventory_policy != "continue",
            )

            total = base_q.count() or 0

            in_stock = (
                base_q.filter(ShopifyInventory.inventory_quantity > 0).count()
            ) or 0

            out_of_stock = (
                base_q.filter(ShopifyInventory.inventory_quantity <= 0).count()
            ) or 0

            oversold = (
                base_q.filter(ShopifyInventory.inventory_quantity < 0).count()
            ) or 0

            # By vendor
            vendor_rows = (
                self.db.query(
                    ShopifyInventory.vendor,
                    func.count().label("total"),
                    func.sum(
                        case(
                            (ShopifyInventory.inventory_quantity > 0, 1),
                            else_=0,
                        )
                    ).label("in_stock"),
                )
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_policy != "continue",
                    ShopifyInventory.vendor.isnot(None),
                )
                .group_by(ShopifyInventory.vendor)
                .order_by(desc("total"))
                .limit(20)
                .all()
            )

            by_vendor = []
            for v in vendor_rows:
                t = v.total or 1
                s = int(v.in_stock or 0)
                by_vendor.append({
                    "vendor": v.vendor,
                    "total": v.total,
                    "in_stock": s,
                    "out_of_stock": v.total - s,
                    "in_stock_pct": round(s / t * 100, 1),
                })

            return {
                "total_skus": total,
                "in_stock": in_stock,
                "out_of_stock": out_of_stock,
                "oversold": oversold,
                "in_stock_pct": round(in_stock / (total or 1) * 100, 1),
                "by_vendor": by_vendor,
            }
        except Exception as e:
            logger.error(f"Availability health failed: {e}")
            return {"total_skus": 0, "in_stock": 0, "out_of_stock": 0, "oversold": 0, "in_stock_pct": 0, "by_vendor": []}

    # ------------------------------------------------------------------
    # Product detail (drill-down)
    # ------------------------------------------------------------------

    def get_product_detail(self, product_id: str):
        """Full product drill-down."""
        try:
            snapshot = self._get_latest_snapshot_date()
            if not snapshot:
                return None

            product = (
                self.db.query(MerchantCenterProductStatus)
                .filter(MerchantCenterProductStatus.product_id == product_id)
                .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                .first()
            )
            if not product:
                return None

            issues = (
                self.db.query(MerchantCenterDisapproval)
                .filter(MerchantCenterDisapproval.product_id == product_id)
                .filter(MerchantCenterDisapproval.snapshot_date == snapshot)
                .filter(MerchantCenterDisapproval.is_resolved == False)
                .all()
            )

            # Status history
            history = (
                self.db.query(
                    MerchantCenterProductStatus.snapshot_date,
                    MerchantCenterProductStatus.approval_status,
                    MerchantCenterProductStatus.issue_count,
                )
                .filter(MerchantCenterProductStatus.product_id == product_id)
                .order_by(desc(MerchantCenterProductStatus.snapshot_date))
                .limit(30)
                .all()
            )

            rev_map = self._get_product_revenue_map()
            rev_info = rev_map.get((product.title or "").lower(), {})

            return {
                "product_id": product.product_id,
                "offer_id": product.offer_id,
                "title": product.title,
                "status": product.approval_status,
                "issue_count": product.issue_count,
                "critical_issues": product.critical_issue_count,
                "monthly_revenue": rev_info.get("revenue", 0),
                "monthly_units": rev_info.get("units", 0),
                "issues": [
                    {
                        "code": i.issue_code,
                        "severity": i.issue_severity,
                        "description": i.issue_description,
                        "detail": i.issue_detail,
                        "attribute": i.issue_attribute,
                        "first_seen": str(i.first_seen_date) if i.first_seen_date else None,
                        "days_affected": (date.today() - i.first_seen_date).days if i.first_seen_date else 0,
                    }
                    for i in issues
                ],
                "history": [
                    {
                        "date": str(h.snapshot_date),
                        "status": h.approval_status,
                        "issues": h.issue_count,
                    }
                    for h in history
                ],
            }
        except Exception as e:
            logger.error(f"Product detail failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search_products(self, query: str, limit: int = 20):
        """Search GMC products by title or product_id."""
        try:
            snapshot = self._get_latest_snapshot_date()
            if not snapshot:
                return []

            pattern = f"%{query}%"
            rows = (
                self.db.query(
                    MerchantCenterProductStatus.product_id,
                    MerchantCenterProductStatus.title,
                    MerchantCenterProductStatus.approval_status,
                    MerchantCenterProductStatus.has_issues,
                    MerchantCenterProductStatus.issue_count,
                )
                .filter(MerchantCenterProductStatus.snapshot_date == snapshot)
                .filter(
                    or_(
                        MerchantCenterProductStatus.title.ilike(pattern),
                        MerchantCenterProductStatus.product_id.ilike(pattern),
                        MerchantCenterProductStatus.offer_id.ilike(pattern),
                    )
                )
                .limit(limit)
                .all()
            )

            return [
                {
                    "product_id": r.product_id,
                    "title": r.title or "",
                    "status": r.approval_status,
                    "has_issues": r.has_issues,
                    "issue_count": r.issue_count or 0,
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Product search failed: {e}")
            return []
