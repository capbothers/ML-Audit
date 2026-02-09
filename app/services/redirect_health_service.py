"""
404 & Redirect Intelligence Service

Queries real database tables for 404 errors, redirect issues,
broken links, and revenue impact.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List, Dict
from datetime import datetime, timedelta

from app.models.redirect_health import (
    NotFoundError, RedirectRule, RedirectChain, BrokenLink
)
from app.utils.logger import log


class RedirectHealthService:
    """Service for analyzing 404 errors and redirects"""

    def __init__(self, db: Session):
        self.db = db
        self.high_traffic_404_threshold = 100
        self.site_avg_conversion_rate = 0.025
        self.site_avg_order_value = 85.00

    async def analyze_404_health(self, days: int = 30) -> Dict:
        """Complete 404 and redirect analysis from real data."""
        try:
            not_found_errors = await self.get_404_errors(days)
            revenue_impact = await self.calculate_revenue_impact(not_found_errors)
            redirect_issues = await self.analyze_redirects()
            redirect_chains = await self.detect_redirect_chains()
            broken_links = await self.find_broken_internal_links()

            recommendations = self._generate_recommendations(
                not_found_errors, redirect_issues, redirect_chains, broken_links
            )
            summary = self._calculate_summary(
                not_found_errors, revenue_impact, redirect_issues,
                redirect_chains, broken_links
            )

            return {
                "not_found_errors": not_found_errors,
                "revenue_impact": revenue_impact,
                "redirect_issues": redirect_issues,
                "redirect_chains": redirect_chains,
                "broken_links": broken_links,
                "recommendations": recommendations,
                "summary": summary,
            }
        except Exception as e:
            log.error(f"Error analyzing 404 health: {str(e)}")
            raise

    async def get_404_errors(self, days: int = 30) -> List[Dict]:
        """Get 404 errors from NotFoundError table, sorted by revenue impact."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            rows = (
                self.db.query(NotFoundError)
                .filter(
                    NotFoundError.status == "active",
                    NotFoundError.last_seen >= cutoff,
                )
                .order_by(desc(NotFoundError.estimated_monthly_revenue_loss))
                .limit(20)
                .all()
            )

            errors = []
            for r in rows:
                errors.append({
                    "requested_url": r.requested_url,
                    "url_type": r.url_type or "other",
                    "traffic": {
                        "total_hits": r.total_hits or 0,
                        "unique_visitors": r.unique_visitors or 0,
                        "estimated_monthly_sessions": r.estimated_monthly_sessions or r.total_hits or 0,
                    },
                    "referrers": {
                        "top_referrers": r.top_referrers or [],
                        "external_links_count": r.external_links_count or 0,
                        "internal_links_count": r.internal_links_count or 0,
                    },
                    "user_behavior": {
                        "bounce_rate_after_404": r.bounce_rate_after_404 or 0,
                        "avg_session_duration_before_404": r.avg_session_duration_before_404 or 0,
                    },
                    "revenue_impact": {
                        "estimated_conversion_rate": r.estimated_conversion_rate or self.site_avg_conversion_rate,
                        "estimated_monthly_revenue_loss": float(r.estimated_monthly_revenue_loss or 0),
                        "confidence_level": "high" if (r.total_hits or 0) >= self.high_traffic_404_threshold else "medium" if (r.total_hits or 0) >= 30 else "low",
                    },
                    "likely_cause": r.likely_cause or "unknown",
                    "recommended_action": r.recommended_action or "create_redirect",
                    "redirect_to_url": r.redirect_to_url or "",
                    "status": r.status or "active",
                })
            return errors

        except Exception as e:
            log.error(f"Error getting 404 errors: {str(e)}")
            return []

    async def calculate_revenue_impact(self, not_found_errors: List[Dict]) -> Dict:
        """Calculate total revenue impact from 404s."""
        total_lost_revenue = sum(
            e["revenue_impact"]["estimated_monthly_revenue_loss"]
            for e in not_found_errors
        )
        total_sessions = sum(
            e["traffic"]["estimated_monthly_sessions"]
            for e in not_found_errors
        )
        high_impact = [
            e for e in not_found_errors
            if e["revenue_impact"]["estimated_monthly_revenue_loss"] >= 500
        ]
        return {
            "total_lost_revenue_monthly": float(total_lost_revenue),
            "total_404_sessions_monthly": total_sessions,
            "high_impact_404s_count": len(high_impact),
            "top_revenue_losses": not_found_errors[:5],
        }

    async def analyze_redirects(self) -> Dict:
        """Analyze redirect health from RedirectRule table."""
        try:
            total = self.db.query(func.count(RedirectRule.id)).filter(
                RedirectRule.is_active == True
            ).scalar() or 0

            broken = (
                self.db.query(RedirectRule)
                .filter(
                    RedirectRule.is_active == True,
                    RedirectRule.destination_exists == False,
                )
                .order_by(desc(RedirectRule.total_hits))
                .limit(10)
                .all()
            )

            chains = (
                self.db.query(RedirectRule)
                .filter(
                    RedirectRule.is_active == True,
                    RedirectRule.is_in_chain == True,
                )
                .order_by(desc(RedirectRule.total_hits))
                .limit(10)
                .all()
            )

            broken_list = [{
                "source_url": r.source_url,
                "destination_url": r.destination_url,
                "redirect_type": r.redirect_type,
                "destination_status_code": r.destination_status_code,
                "total_hits": r.total_hits or 0,
                "issue": "Redirect points to " + str(r.destination_status_code or 404),
                "recommendation": f"Update redirect destination for {r.source_url}",
            } for r in broken]

            chain_list = [{
                "source_url": r.source_url,
                "destination_url": r.destination_url,
                "final_destination": r.final_destination_url or r.destination_url,
                "chain_length": r.chain_length or 2,
                "total_redirect_time_ms": int((r.avg_redirect_time_ms or 200) * (r.chain_length or 2)),
                "issue": f"Redirect chain ({r.chain_length or 2} hops)",
                "recommendation": f"Create direct 301 from {r.source_url} to {r.final_destination_url or r.destination_url}",
            } for r in chains]

            issues_count = len(broken_list) + len(chain_list)

            return {
                "total_redirects": total,
                "redirects_with_issues": issues_count,
                "broken_redirects": broken_list,
                "redirect_chains": chain_list,
                "temporary_redirects": [],
                "slow_redirects": [],
            }

        except Exception as e:
            log.error(f"Error analyzing redirects: {str(e)}")
            return {
                "total_redirects": 0, "redirects_with_issues": 0,
                "broken_redirects": [], "redirect_chains": [],
                "temporary_redirects": [], "slow_redirects": [],
            }

    async def detect_redirect_chains(self) -> List[Dict]:
        """Detect multi-hop redirect chains from RedirectChain table."""
        try:
            rows = (
                self.db.query(RedirectChain)
                .filter(RedirectChain.status == "active")
                .order_by(desc(RedirectChain.chain_length))
                .limit(10)
                .all()
            )
            return [{
                "initial_url": r.initial_url,
                "final_url": r.final_url,
                "chain_length": r.chain_length,
                "chain_path": r.chain_path or [],
                "total_redirect_time_ms": r.total_redirect_time_ms or 0,
                "severity": r.severity,
                "contains_302": r.contains_302,
                "ends_in_404": r.ends_in_404,
                "recommended_fix": r.recommended_fix or f"Create direct redirect from {r.initial_url} to {r.final_url}",
                "status": r.status,
            } for r in rows]

        except Exception as e:
            log.error(f"Error detecting redirect chains: {str(e)}")
            return []

    async def find_broken_internal_links(self) -> List[Dict]:
        """Find broken internal links from BrokenLink table."""
        try:
            rows = (
                self.db.query(BrokenLink)
                .filter(BrokenLink.status == "active")
                .order_by(desc(BrokenLink.source_page_traffic))
                .limit(10)
                .all()
            )
            return [{
                "source_page": r.source_page,
                "broken_link": r.broken_link,
                "link_text": r.link_text or "",
                "link_type": r.link_type or "text_link",
                "source_page_traffic": r.source_page_traffic or 0,
                "estimated_monthly_clicks": r.estimated_monthly_clicks or 0,
                "priority": r.priority,
                "recommended_fix": r.recommended_fix or "update_link",
                "suggested_replacement": r.suggested_replacement or "",
                "status": r.status,
            } for r in rows]

        except Exception as e:
            log.error(f"Error finding broken internal links: {str(e)}")
            return []

    def _generate_recommendations(
        self, not_found_errors, redirect_issues, redirect_chains, broken_links
    ) -> List[Dict]:
        """Generate prioritized recommendations from real data."""
        recommendations = []

        for error in not_found_errors:
            loss = error["revenue_impact"]["estimated_monthly_revenue_loss"]
            if loss >= 500:
                recommendations.append({
                    "priority": "high",
                    "type": "create_redirect",
                    "title": f"Create redirect for {error['requested_url']}",
                    "description": f"404 error causing ${loss:.0f}/month revenue loss",
                    "action": f"Create 301 redirect: {error['requested_url']} → {error['redirect_to_url']}",
                    "impact": f"${loss:.0f}/month",
                    "effort": "Low (5 minutes)",
                })

        for redirect in redirect_issues.get("broken_redirects", []):
            recommendations.append({
                "priority": "high",
                "type": "fix_redirect",
                "title": f"Fix broken redirect: {redirect['source_url']}",
                "description": f"Redirect points to {redirect.get('destination_status_code', 404)}",
                "action": redirect["recommendation"],
                "impact": f"{redirect['total_hits']} monthly sessions affected",
                "effort": "Low (5 minutes)",
            })

        for chain in redirect_chains:
            if chain["chain_length"] >= 3:
                recommendations.append({
                    "priority": "medium",
                    "type": "fix_redirect_chain",
                    "title": f"Fix redirect chain: {chain['initial_url']}",
                    "description": f"{chain['chain_length']}-hop chain adds {chain.get('total_redirect_time_ms', 0)}ms latency",
                    "action": chain.get("recommended_fix", "Simplify redirect chain"),
                    "impact": "Improve SEO and reduce latency",
                    "effort": "Low (5 minutes)",
                })

        for link in broken_links:
            if link["priority"] == "high":
                recommendations.append({
                    "priority": "high",
                    "type": "fix_internal_link",
                    "title": f"Fix broken link on {link['source_page']}",
                    "description": f"High-traffic page ({link['source_page_traffic']:,} sessions/month) has broken link",
                    "action": f"Update link from {link['broken_link']} to {link['suggested_replacement']}",
                    "impact": f"{link['estimated_monthly_clicks']} clicks to 404 prevented",
                    "effort": "Low (5 minutes)",
                })

        priority_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda x: priority_order.get(x["priority"], 3))
        return recommendations[:15]

    def _calculate_summary(
        self, not_found_errors, revenue_impact, redirect_issues, redirect_chains, broken_links
    ) -> Dict:
        return {
            "total_404_errors": len(not_found_errors),
            "high_traffic_404s": len([
                e for e in not_found_errors
                if e["traffic"]["estimated_monthly_sessions"] >= self.high_traffic_404_threshold
            ]),
            "total_monthly_revenue_loss": revenue_impact.get("total_lost_revenue_monthly", 0),
            "total_404_sessions": revenue_impact.get("total_404_sessions_monthly", 0),
            "total_redirects": redirect_issues.get("total_redirects", 0),
            "redirects_with_issues": redirect_issues.get("redirects_with_issues", 0),
            "broken_redirects": len(redirect_issues.get("broken_redirects", [])),
            "redirect_chains": len(redirect_chains),
            "broken_internal_links": len(broken_links),
            "high_priority_broken_links": len([l for l in broken_links if l["priority"] == "high"]),
        }

    async def get_404_dashboard(self, days: int = 30) -> Dict:
        """Get complete 404 & redirect dashboard."""
        analysis = await self.analyze_404_health(days=days)

        rev = analysis["revenue_impact"]
        return {
            "overview": {
                "total_404_errors": analysis["summary"]["total_404_errors"],
                "high_traffic_404s": analysis["summary"]["high_traffic_404s"],
                "total_revenue_loss": analysis["summary"]["total_monthly_revenue_loss"],
                "total_redirect_issues": analysis["summary"]["redirects_with_issues"],
            },
            "top_priorities": analysis["recommendations"][:5],
            "404_summary": {
                "count": len(analysis["not_found_errors"]),
                "top_errors": analysis["not_found_errors"][:5],
            },
            "revenue_impact_summary": {
                "total_lost_revenue": rev.get("total_lost_revenue_monthly", 0),
                "total_sessions": rev.get("total_404_sessions_monthly", 0),
                "high_impact_404s": rev.get("high_impact_404s_count", 0),
            },
            "redirect_issues_summary": {
                "total_redirects": analysis["redirect_issues"].get("total_redirects", 0),
                "issues_count": analysis["redirect_issues"].get("redirects_with_issues", 0),
                "broken_redirects": len(analysis["redirect_issues"].get("broken_redirects", [])),
                "redirect_chains": len(analysis["redirect_chains"]),
            },
            "broken_links_summary": {
                "count": len(analysis["broken_links"]),
                "high_priority": analysis["summary"]["high_priority_broken_links"],
                "top_links": analysis["broken_links"][:5],
            },
        }
