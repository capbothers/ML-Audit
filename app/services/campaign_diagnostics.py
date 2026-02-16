"""
Campaign Diagnostics Service — per-campaign working/not-working/actions.

Scoping rules:
  - Campaign-linked:  search terms, auction/IS, feed (via product-ads bridge)
  - Site-wide (labelled): device gaps, LP health fallback
  - LP can be campaign-scoped when URL matches campaign brand keywords

Each issue includes impact: $ at risk and/or % conversions affected.
"""
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from app.models.google_ads_data import (
    GoogleAdsSearchTerm, GoogleAdsCampaign, GoogleAdsProductPerformance,
)
from app.models.ga4_data import GA4LandingPage, GA4DeviceBreakdown
from app.models.merchant_center_data import MerchantCenterDisapproval
from app.models.seo import CoreWebVitals
from app.utils.logger import log


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _truncate_url(url: str, max_len: int = 55) -> str:
    if not url or len(url) <= max_len:
        return url or ''
    return url[:max_len - 3] + '...'


def _extract_brand_keywords(campaign_name: str) -> List[str]:
    """Pull brand/product keywords from campaign name for LP matching."""
    if not campaign_name:
        return []
    # Strip common prefixes: PM-AU, PM-SYD, PM1, PMAX, etc.
    name = campaign_name
    for prefix in ('PM-AU ', 'PM-SYD ', 'PM1 ', 'PMAX ', 'PM ', 'AI Max '):
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    # Split into words, keep only meaningful brand tokens (≥3 chars)
    words = [w.lower() for w in name.split() if len(w) >= 3]
    # Filter out generic words
    generic = {'campaign', 'search', 'shopping', 'free', 'listings',
               'non', 'sydney', 'old', 'all', 'hardware', 'zombie',
               'demand', 'gen', 'local', 'store', 'visits', 'max'}
    return [w for w in words if w not in generic]


def _normalize_mc_product_id(mc_pid: str) -> str:
    """Convert MC product_id to comparable format with ads product_item_id.

    MC:  online:en:AU:shopify_AU_<id>_<variant>
    Ads: shopify_au_<id>_<variant>
    """
    if not mc_pid:
        return ''
    parts = mc_pid.split(':')
    if len(parts) >= 4:
        # Take everything after 'online:en:AU:'
        return parts[-1].lower()
    return mc_pid.lower()


class CampaignDiagnosticsService:
    def __init__(self, db: Session):
        self.db = db

    def diagnose_all(
        self,
        campaigns: List[Dict],
        period_start: date,
        period_end: date,
    ) -> Dict[str, Dict]:
        """
        Batch diagnostics for all campaigns.

        Returns: {campaign_id: {working: [...], not_working: [...], actions: [],
                                 has_blockers: bool}}
        """
        campaign_ids = [c['campaign_id'] for c in campaigns]

        # Batch-fetch all data sources once
        search_terms_by_cid = self._batch_search_terms(campaign_ids, period_start, period_end)
        product_revenue_by_cid = self._batch_product_revenue(campaign_ids, period_start, period_end)
        lp_health = self._get_lp_health(period_start, period_end)
        feed_issues = self._get_feed_issues()
        device_gaps = self._get_device_gaps(period_start, period_end)
        auction_data = self._batch_auction_data(campaign_ids, period_start, period_end)

        # Build bridge: MC product → campaign_ids (via product perf table)
        mc_to_campaigns = self._build_product_campaign_bridge(campaign_ids, period_start, period_end)

        results = {}
        for campaign in campaigns:
            cid = campaign['campaign_id']
            cname = campaign.get('campaign_name', '')
            brand_keywords = _extract_brand_keywords(cname)
            campaign_spend = campaign.get('spend', 0)
            campaign_conversions = campaign.get('true_metrics', {}).get('conversions', 0)

            diag: Dict = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

            self._analyze_search_terms(diag, search_terms_by_cid.get(cid, []), campaign_spend)
            self._analyze_landing_pages(diag, lp_health, brand_keywords, cname)
            self._analyze_feed(
                diag, feed_issues, cid, mc_to_campaigns,
                product_revenue_by_cid.get(cid, {}), campaign_spend,
                brand_keywords,
            )
            self._analyze_device(diag, device_gaps, campaign_conversions)
            self._analyze_auction(diag, auction_data.get(cid, {}), campaign_spend)

            results[cid] = diag

        return results

    # ------------------------------------------------------------------
    # Batch data fetchers
    # ------------------------------------------------------------------

    def _batch_search_terms(
        self, campaign_ids: List[str], start: date, end: date
    ) -> Dict[str, List[Dict]]:
        """Fetch search term data grouped by campaign_id."""
        try:
            rows = (
                self.db.query(
                    GoogleAdsSearchTerm.campaign_id,
                    GoogleAdsSearchTerm.search_term,
                    func.sum(GoogleAdsSearchTerm.clicks).label('clicks'),
                    func.sum(GoogleAdsSearchTerm.impressions).label('impressions'),
                    func.sum(GoogleAdsSearchTerm.cost_micros).label('cost_micros'),
                    func.sum(GoogleAdsSearchTerm.conversions).label('conversions'),
                    func.sum(GoogleAdsSearchTerm.conversions_value).label('conv_value'),
                )
                .filter(
                    GoogleAdsSearchTerm.campaign_id.in_(campaign_ids),
                    GoogleAdsSearchTerm.date >= start,
                    GoogleAdsSearchTerm.date <= end,
                )
                .group_by(
                    GoogleAdsSearchTerm.campaign_id,
                    GoogleAdsSearchTerm.search_term,
                )
                .all()
            )
        except Exception as e:
            log.warning(f"Search terms fetch failed: {e}")
            return {}

        by_cid: Dict[str, List[Dict]] = defaultdict(list)
        for r in rows:
            by_cid[r.campaign_id].append({
                'term': r.search_term,
                'clicks': r.clicks or 0,
                'impressions': r.impressions or 0,
                'cost': (r.cost_micros or 0) / 1_000_000,
                'conversions': r.conversions or 0,
                'conv_value': r.conv_value or 0,
            })
        return dict(by_cid)

    def _batch_product_revenue(
        self, campaign_ids: List[str], start: date, end: date,
    ) -> Dict[str, Dict[str, float]]:
        """Revenue per product_item_id per campaign_id."""
        try:
            rows = (
                self.db.query(
                    GoogleAdsProductPerformance.campaign_id,
                    GoogleAdsProductPerformance.product_item_id,
                    GoogleAdsProductPerformance.product_title,
                    func.sum(GoogleAdsProductPerformance.conversions_value).label('rev'),
                    func.sum(GoogleAdsProductPerformance.cost_micros).label('cost'),
                    func.sum(GoogleAdsProductPerformance.conversions).label('convs'),
                )
                .filter(
                    GoogleAdsProductPerformance.campaign_id.in_(campaign_ids),
                    GoogleAdsProductPerformance.date >= start,
                    GoogleAdsProductPerformance.date <= end,
                )
                .group_by(
                    GoogleAdsProductPerformance.campaign_id,
                    GoogleAdsProductPerformance.product_item_id,
                    GoogleAdsProductPerformance.product_title,
                )
                .all()
            )
        except Exception as e:
            log.warning(f"Product revenue fetch failed: {e}")
            return {}

        by_cid: Dict[str, Dict] = defaultdict(dict)
        for r in rows:
            pid_norm = r.product_item_id.lower() if r.product_item_id else ''
            by_cid[r.campaign_id][pid_norm] = {
                'title': r.product_title,
                'revenue': float(r.rev or 0),
                'cost': float(r.cost or 0) / 1_000_000,
                'conversions': float(r.convs or 0),
            }
        return dict(by_cid)

    def _build_product_campaign_bridge(
        self, campaign_ids: List[str], start: date, end: date,
    ) -> Dict[str, Set[str]]:
        """Map normalized product_item_id → set of campaign_ids that advertise it."""
        try:
            rows = (
                self.db.query(
                    GoogleAdsProductPerformance.campaign_id,
                    GoogleAdsProductPerformance.product_item_id,
                )
                .filter(
                    GoogleAdsProductPerformance.campaign_id.in_(campaign_ids),
                    GoogleAdsProductPerformance.date >= start,
                    GoogleAdsProductPerformance.date <= end,
                )
                .distinct()
                .all()
            )
        except Exception as e:
            log.warning(f"Product bridge build failed: {e}")
            return {}

        bridge: Dict[str, Set[str]] = defaultdict(set)
        for r in rows:
            pid_norm = r.product_item_id.lower() if r.product_item_id else ''
            if pid_norm:
                bridge[pid_norm].add(r.campaign_id)
        return dict(bridge)

    def _get_lp_health(self, start: date, end: date) -> Dict:
        """Landing page CVR/bounce for google/cpc traffic, period-over-period."""
        mid = start + (end - start) // 2
        try:
            def _period(s, e):
                rows = (
                    self.db.query(
                        GA4LandingPage.landing_page,
                        func.sum(GA4LandingPage.sessions).label('sessions'),
                        func.sum(GA4LandingPage.conversions).label('conversions'),
                        func.avg(GA4LandingPage.bounce_rate).label('bounce'),
                    )
                    .filter(
                        GA4LandingPage.session_source == 'google',
                        GA4LandingPage.session_medium == 'cpc',
                        GA4LandingPage.date >= s,
                        GA4LandingPage.date <= e,
                    )
                    .group_by(GA4LandingPage.landing_page)
                    .having(func.sum(GA4LandingPage.sessions) >= 10)
                    .all()
                )
                return {r.landing_page: {
                    'sessions': r.sessions or 0,
                    'conversions': r.conversions or 0,
                    'cvr': (r.conversions or 0) / (r.sessions or 1),
                    'bounce': float(r.bounce or 0),
                } for r in rows}

            prev = _period(start, mid - timedelta(days=1))
            curr = _period(mid, end)

            # Total paid sessions for % impact calc
            total_paid_sessions = sum(d['sessions'] for d in curr.values())

            # CWV data for top pages
            cwv = {}
            try:
                cwv_rows = (
                    self.db.query(
                        CoreWebVitals.url,
                        CoreWebVitals.lcp_value,
                        CoreWebVitals.lcp_status,
                        CoreWebVitals.overall_status,
                        CoreWebVitals.device,
                    )
                    .order_by(CoreWebVitals.checked_at.desc())
                    .limit(200)
                    .all()
                )
                for r in cwv_rows:
                    if r.url not in cwv:
                        cwv[r.url] = {
                            'lcp': r.lcp_value,
                            'lcp_status': r.lcp_status,
                            'overall': r.overall_status,
                            'device': r.device,
                        }
            except Exception:
                pass

            return {
                'prev': prev, 'curr': curr, 'cwv': cwv,
                'total_sessions': total_paid_sessions,
            }
        except Exception as e:
            log.warning(f"LP health fetch failed: {e}")
            return {'prev': {}, 'curr': {}, 'cwv': {}, 'total_sessions': 0}

    def _get_feed_issues(self) -> Dict:
        """Active MC disapprovals — deduplicated by product_id + issue_code."""
        try:
            rows = (
                self.db.query(
                    MerchantCenterDisapproval.product_id,
                    MerchantCenterDisapproval.offer_id,
                    MerchantCenterDisapproval.title,
                    MerchantCenterDisapproval.issue_code,
                    MerchantCenterDisapproval.issue_severity,
                    MerchantCenterDisapproval.issue_description,
                )
                .filter(
                    MerchantCenterDisapproval.is_resolved == False,
                    MerchantCenterDisapproval.issue_severity == 'disapproved',
                )
                .all()
            )
            # Deduplicate: one entry per (product_id, issue_code)
            seen = set()
            issues = []
            for r in rows:
                key = (r.product_id, r.issue_code)
                if key in seen:
                    continue
                seen.add(key)
                issues.append({
                    'product_id': r.product_id,
                    'offer_id': r.offer_id,
                    'title': r.title,
                    'issue_code': r.issue_code,
                    'description': r.issue_description,
                    'normalized_pid': _normalize_mc_product_id(r.product_id),
                })
            return {'disapproved': issues, 'count': len(issues)}
        except Exception as e:
            log.warning(f"Feed issues fetch failed: {e}")
            return {'disapproved': [], 'count': 0}

    def _get_device_gaps(self, start: date, end: date) -> Dict:
        """Mobile vs desktop CVR gap from GA4 device breakdown."""
        try:
            rows = (
                self.db.query(
                    GA4DeviceBreakdown.device_category,
                    func.sum(GA4DeviceBreakdown.sessions).label('sessions'),
                    func.sum(GA4DeviceBreakdown.conversions).label('conversions'),
                    func.avg(GA4DeviceBreakdown.bounce_rate).label('bounce'),
                )
                .filter(
                    GA4DeviceBreakdown.date >= start,
                    GA4DeviceBreakdown.date <= end,
                )
                .group_by(GA4DeviceBreakdown.device_category)
                .all()
            )
            by_device = {}
            for r in rows:
                sessions = r.sessions or 0
                convs = r.conversions or 0
                by_device[r.device_category] = {
                    'sessions': sessions,
                    'conversions': convs,
                    'cvr': convs / sessions if sessions > 0 else 0,
                    'bounce': float(r.bounce or 0),
                }
            return by_device
        except Exception as e:
            log.warning(f"Device gaps fetch failed: {e}")
            return {}

    def _batch_auction_data(
        self, campaign_ids: List[str], start: date, end: date
    ) -> Dict[str, Dict]:
        """Impression share + CPC trends per campaign."""
        mid = start + (end - start) // 2
        try:
            def _period_avg(s, e):
                rows = (
                    self.db.query(
                        GoogleAdsCampaign.campaign_id,
                        func.avg(GoogleAdsCampaign.search_rank_lost_impression_share).label('rank_lost'),
                        func.avg(GoogleAdsCampaign.search_budget_lost_impression_share).label('budget_lost'),
                        func.avg(GoogleAdsCampaign.avg_cpc).label('cpc'),
                        func.avg(GoogleAdsCampaign.ctr).label('ctr'),
                    )
                    .filter(
                        GoogleAdsCampaign.campaign_id.in_(campaign_ids),
                        GoogleAdsCampaign.date >= s,
                        GoogleAdsCampaign.date <= e,
                    )
                    .group_by(GoogleAdsCampaign.campaign_id)
                    .all()
                )
                return {r.campaign_id: {
                    'rank_lost': float(r.rank_lost or 0),
                    'budget_lost': float(r.budget_lost or 0),
                    'cpc': float(r.cpc or 0),
                    'ctr': float(r.ctr or 0),
                } for r in rows}

            prev = _period_avg(start, mid - timedelta(days=1))
            curr = _period_avg(mid, end)

            result = {}
            for cid in campaign_ids:
                p = prev.get(cid, {'rank_lost': 0, 'budget_lost': 0, 'cpc': 0, 'ctr': 0})
                c = curr.get(cid, {'rank_lost': 0, 'budget_lost': 0, 'cpc': 0, 'ctr': 0})
                result[cid] = {
                    'rank_lost_curr': c['rank_lost'],
                    'budget_lost_curr': c['budget_lost'],
                    'impression_share': max(0, 100 - c['rank_lost'] - c['budget_lost']),
                    'rank_lost_delta': c['rank_lost'] - p['rank_lost'],
                    'cpc_curr': c['cpc'],
                    'cpc_change': (
                        (c['cpc'] - p['cpc']) / p['cpc'] if p['cpc'] > 0 else 0
                    ),
                    'ctr_change': (
                        (c['ctr'] - p['ctr']) / p['ctr'] if p['ctr'] > 0 else 0
                    ),
                }
            return result
        except Exception as e:
            log.warning(f"Auction data fetch failed: {e}")
            return {}

    # ------------------------------------------------------------------
    # Analyzers — each adds scoped, impact-ranked findings
    # ------------------------------------------------------------------

    def _analyze_search_terms(
        self, diag: Dict, terms: List[Dict], campaign_spend: float,
    ) -> None:
        """Analyzer 1: Search term quality — campaign-scoped."""
        if not terms:
            return

        total_cost = sum(t['cost'] for t in terms)
        if total_cost == 0:
            return

        converting = [t for t in terms if t['conversions'] > 0]
        wasted = [t for t in terms if t['clicks'] >= 3 and t['conversions'] == 0]

        converting.sort(key=lambda t: t['conv_value'], reverse=True)
        wasted.sort(key=lambda t: t['cost'], reverse=True)

        wasted_cost = sum(t['cost'] for t in wasted)
        wasted_pct = (wasted_cost / total_cost * 100) if total_cost > 0 else 0
        spend_pct = (wasted_cost / campaign_spend * 100) if campaign_spend > 0 else 0

        if converting:
            top = converting[:3]
            total_conv_value = sum(t['conv_value'] for t in converting)
            term_list = ', '.join(f'"{t["term"]}"' for t in top)
            diag['working'].append(
                f"Top converting terms: {term_list} "
                f"(${total_conv_value:,.0f} revenue)"
            )

        if wasted and wasted_pct >= 15:
            top_wasted = wasted[:5]
            term_list = ', '.join(f'"{t["term"]}"' for t in top_wasted)
            diag['not_working'].append(
                f"{wasted_pct:.0f}% of search spend (${wasted_cost:,.0f}) on "
                f"non-converting terms — {spend_pct:.0f}% of campaign budget at risk"
            )
            diag['actions'].append(
                f"Add negatives for: {term_list}"
            )
        elif wasted and wasted_cost >= 50:
            top_wasted = wasted[:3]
            term_list = ', '.join(f'"{t["term"]}"' for t in top_wasted)
            diag['not_working'].append(
                f"${wasted_cost:,.0f} on non-converting terms ({wasted_pct:.0f}%)"
            )
            diag['actions'].append(
                f"Review negatives: {term_list}"
            )

    def _analyze_landing_pages(
        self, diag: Dict, lp_health: Dict,
        brand_keywords: List[str], campaign_name: str,
    ) -> None:
        """Analyzer 2: Landing page health — campaign-scoped when possible."""
        prev = lp_health.get('prev', {})
        curr = lp_health.get('curr', {})
        cwv = lp_health.get('cwv', {})
        total_sessions = lp_health.get('total_sessions', 0)

        if not curr:
            return

        # Try to scope to this campaign's brand keywords
        def _url_matches_campaign(url: str) -> bool:
            if not brand_keywords:
                return False
            url_lower = url.lower()
            return any(kw in url_lower for kw in brand_keywords)

        # Split pages into campaign-relevant vs all
        campaign_pages = {u: d for u, d in curr.items() if _url_matches_campaign(u)}
        scope_label = ''

        if campaign_pages:
            # Use campaign-scoped pages
            pages_to_check = campaign_pages
            scope_label = ''  # no label needed — it's scoped
        else:
            # Fall back to site-wide top pages, but label explicitly
            pages_to_check = curr
            scope_label = ' [site-wide]'

        # Best performing page (by conversions) within scope
        top_pages = sorted(pages_to_check.items(), key=lambda x: x[1]['conversions'], reverse=True)[:5]
        if top_pages:
            best_url, best_data = top_pages[0]
            if best_data['cvr'] > 0:
                session_share = (best_data['sessions'] / total_sessions) if total_sessions > 0 else 0
                # Avoid repeating the same generic site-wide "top LP" on every campaign.
                # Keep fallback praise only when the page is materially large.
                if scope_label != ' [site-wide]' or session_share >= 0.10:
                    session_pct = (
                        f" ({session_share:.0%} of paid traffic)"
                        if total_sessions > 0 else ''
                    )
                    diag['working'].append(
                        f"Top LP {_truncate_url(best_url)} CVR {best_data['cvr']:.1%}"
                        f"{session_pct}{scope_label}"
                    )

        # Pages with CVR decline
        for url, curr_data in pages_to_check.items():
            if url not in prev or curr_data['sessions'] < 20:
                continue
            prev_data = prev[url]
            if prev_data['cvr'] == 0:
                continue

            cvr_change = (curr_data['cvr'] - prev_data['cvr']) / prev_data['cvr']
            bounce_change = (
                (curr_data['bounce'] - prev_data['bounce']) / prev_data['bounce']
                if prev_data['bounce'] > 0 else 0
            )

            if cvr_change < -0.20:
                lost_convs = (prev_data['cvr'] - curr_data['cvr']) * curr_data['sessions']
                session_share = (
                    (curr_data['sessions'] / total_sessions) if total_sessions > 0 else 0
                )
                # For fallback site-wide LPs, suppress tiny issues that otherwise
                # get repeated across every campaign card.
                if scope_label == ' [site-wide]' and (session_share < 0.05 or lost_convs < 2):
                    continue
                session_pct = (
                    f"{session_share:.0%} of paid traffic"
                    if total_sessions > 0 else f"{curr_data['sessions']} sessions"
                )

                cwv_note = ''
                for cwv_url, cwv_data in cwv.items():
                    if url in cwv_url or cwv_url in url:
                        if cwv_data.get('lcp') and cwv_data['lcp'] > 2.5:
                            cwv_note = f" — LCP {cwv_data['lcp']:.1f}s"
                        break

                bounce_note = (
                    f', bounce +{abs(bounce_change):.0%}'
                    if bounce_change > 0.10 else ''
                )
                diag['not_working'].append(
                    f"LP {_truncate_url(url)} CVR dropped {abs(cvr_change):.0%}"
                    f"{bounce_note} — ~{lost_convs:.0f} conv lost "
                    f"({session_pct}){scope_label}"
                )
                diag['actions'].append(
                    f"Fix LP friction on {_truncate_url(url)}{cwv_note}"
                )
                diag['has_blockers'] = True
                break  # one LP issue per campaign

    def _analyze_feed(
        self, diag: Dict, feed_issues: Dict,
        campaign_id: str, mc_to_campaigns: Dict[str, Set[str]],
        product_revenue: Dict[str, Dict], campaign_spend: float,
        brand_keywords: Optional[List[str]] = None,
    ) -> None:
        """Analyzer 3: Feed health — scoped to products advertised in this campaign."""
        all_disapproved = feed_issues.get('disapproved', [])
        if not all_disapproved:
            return

        # Filter to disapprovals that affect THIS campaign's products
        campaign_issues = []
        global_issues = []
        for issue in all_disapproved:
            norm_pid = issue.get('normalized_pid', '')
            campaigns_for_product = mc_to_campaigns.get(norm_pid, set())
            if campaign_id in campaigns_for_product:
                # This disapproved product is advertised in this campaign
                rev_data = product_revenue.get(norm_pid, {})
                issue_with_impact = {**issue, 'revenue': rev_data.get('revenue', 0)}
                campaign_issues.append(issue_with_impact)
            elif not campaigns_for_product:
                # Can't link to any campaign — track as global
                global_issues.append(issue)

        if campaign_issues:
            # Sort: brand-relevant first, then by revenue
            def _sort_key(ci):
                title_lower = (ci.get('title') or '').lower()
                is_brand = any(kw in title_lower for kw in (brand_keywords or []))
                return (not is_brand, -ci['revenue'])

            campaign_issues.sort(key=_sort_key)
            seen_titles = set()
            unique_issues = []
            for ci in campaign_issues:
                title = (ci.get('title') or '')[:40]
                if title and title in seen_titles:
                    continue
                seen_titles.add(title)
                unique_issues.append(ci)

            total_at_risk = sum(ci['revenue'] for ci in unique_issues)

            # Group by issue code for summary
            by_code = defaultdict(list)
            for ci in unique_issues:
                by_code[ci['issue_code']].append(ci)
            top_codes = sorted(by_code.items(), key=lambda x: len(x[1]), reverse=True)[:3]
            code_summary = ', '.join(
                f"{code} ({len(items)})" for code, items in top_codes
            )

            count = len(unique_issues)
            risk_str = f" — ${total_at_risk:,.0f} revenue at risk" if total_at_risk > 0 else ''
            diag['not_working'].append(
                f"{count} advertised products disapproved: {code_summary}{risk_str}"
            )

            # Action: show top impacted SKUs — brand-relevant first, then by revenue
            # Separate brand-relevant vs generic
            brand_skus = []
            generic_skus = []
            for ci in unique_issues:
                title_lower = (ci.get('title') or '').lower()
                if brand_keywords and any(kw in title_lower for kw in brand_keywords):
                    brand_skus.append(ci)
                else:
                    generic_skus.append(ci)

            if brand_skus:
                top_skus = brand_skus[:3]
            elif generic_skus:
                # Show by revenue only, cap at 3
                top_skus = [s for s in generic_skus if s.get('revenue', 0) > 0][:3]

            sku_parts = []
            for s in top_skus:
                name = (s.get('title') or s.get('offer_id') or '?')[:35]
                rev = s.get('revenue', 0)
                if rev > 0:
                    sku_parts.append(f"{name} (${rev:,.0f})")
                else:
                    sku_parts.append(name)

            if sku_parts:
                n_more = count - len(top_skus)
                more_str = f" + {n_more} more" if n_more > 0 else ''
                diag['actions'].append(
                    f"Fix disapproved SKUs: {', '.join(sku_parts)}{more_str}"
                )
            else:
                top_code = top_codes[0][0] if top_codes else 'various'
                # Avoid repeating the same arbitrary SKU names on every campaign when
                # campaign-level revenue linkage is weak.
                diag['actions'].append(
                    f"Fix {count} disapproved products (top issue: {top_code})"
                )
            diag['has_blockers'] = True

        elif global_issues and len(global_issues) > 10:
            # Only show global feed health if significant and no campaign-specific issues
            by_code = defaultdict(list)
            for gi in global_issues:
                by_code[gi['issue_code']].append(gi)
            top_codes = sorted(by_code.items(), key=lambda x: len(x[1]), reverse=True)[:2]
            code_summary = ', '.join(f"{code} ({len(items)})" for code, items in top_codes)
            diag['not_working'].append(
                f"{len(global_issues)} products disapproved site-wide: "
                f"{code_summary} [site-wide]"
            )

    def _analyze_device(
        self, diag: Dict, device_gaps: Dict, campaign_conversions: float,
    ) -> None:
        """Analyzer 4: Mobile vs desktop performance — always site-wide, labelled."""
        desktop = device_gaps.get('desktop', {})
        mobile = device_gaps.get('mobile', {})

        if not desktop or not mobile:
            return

        d_cvr = desktop.get('cvr', 0)
        m_cvr = mobile.get('cvr', 0)
        m_sessions = mobile.get('sessions', 0)
        d_sessions = desktop.get('sessions', 0)
        total_sessions = m_sessions + d_sessions

        if d_cvr > 0 and m_cvr < d_cvr * 0.5 and m_sessions > 50:
            # Estimate conversions lost if mobile matched desktop CVR
            mobile_conv_gap = (d_cvr - m_cvr) * m_sessions
            mobile_traffic_pct = (m_sessions / total_sessions * 100) if total_sessions > 0 else 0

            diag['not_working'].append(
                f"Mobile CVR {m_cvr:.1%} vs desktop {d_cvr:.1%} — "
                f"~{mobile_conv_gap:.0f} conversions lost, "
                f"mobile is {mobile_traffic_pct:.0f}% of traffic [site-wide]"
            )
            diag['actions'].append(
                "Test mobile checkout/LP experience — "
                f"closing CVR gap could add ~{mobile_conv_gap:.0f} conversions [site-wide]"
            )

    def _analyze_auction(
        self, diag: Dict, auction: Dict, campaign_spend: float,
    ) -> None:
        """Analyzer 5: Auction pressure and impression share — campaign-scoped."""
        if not auction:
            return

        imp_share = auction.get('impression_share', 0)
        rank_delta = auction.get('rank_lost_delta', 0)
        cpc_change = auction.get('cpc_change', 0)
        ctr_change = auction.get('ctr_change', 0)
        cpc = auction.get('cpc_curr', 0)

        if imp_share >= 80:
            diag['working'].append(
                f"Impression share {imp_share:.0f}% — good visibility"
            )

        if rank_delta > 5:
            diag['not_working'].append(
                f"Rank-lost IS up {rank_delta:.0f}pp — competitors bidding higher"
            )
            diag['actions'].append(
                f"Review bid strategy — rank-lost IS increasing"
            )
        elif cpc_change > 0.20 and abs(ctr_change) < 0.15:
            cpc_increase_cost = campaign_spend * cpc_change
            diag['not_working'].append(
                f"CPC up {cpc_change:.0%} (now ${cpc:.2f}) — "
                f"~${cpc_increase_cost:,.0f} extra spend from rising CPCs"
            )
            diag['actions'].append(
                f"Review bid strategy — test tROAS or manual CPC caps"
            )

        budget_lost = auction.get('budget_lost_curr', 0)
        if budget_lost > 20:
            diag['not_working'].append(
                f"Budget-lost IS {budget_lost:.0f}% — capped before end of day"
            )
