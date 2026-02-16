"""
Blog Draft Service

Orchestrates LLM blog post generation from SEO underperformer data.
Connects SEO analysis → product context → LLM generation → stored drafts.
"""
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.services.seo_service import SEOService
from app.services.llm_service import LLMService
from app.models.content_gap import BlogDraft
from app.models.shopify import ShopifyProduct
from app.models.search_console_data import SearchConsoleQuery
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class BlogDraftService:
    """Orchestrates blog draft generation from SEO underperformer data."""

    def __init__(self, db: Session):
        self.db = db
        self.seo_service = SEOService(db)
        self.llm_service = LLMService()

    async def generate_draft_for_query(
        self,
        query: str,
        opportunity_type: str = "manual",
        days: int = 30,
    ) -> Optional[Dict]:
        """Generate a blog draft for a specific underperforming query."""
        try:
            # 1. Get SEO data for this query
            drill_down = self.seo_service.get_query_drill_down(query=query, days=days)

            current = drill_down.get("current", {})
            seo_context = {
                "position": current.get("position"),
                "impressions": current.get("impressions", 0),
                "clicks": current.get("clicks", 0),
                "actual_ctr": current.get("ctr", 0),
                "click_gap": drill_down.get("click_gap", 0),
                "related_queries": self._get_related_queries(query, days),
            }

            # 2. Get relevant products for internal linking context
            products = self._find_relevant_products(query)

            # 3. Determine source page (top-ranking page for this query)
            pages = drill_down.get("pages", [])
            page_url = pages[0]["page"] if pages else None

            # 4. Generate via LLM
            result = self.llm_service.generate_seo_blog_post(
                query=query,
                page_url=page_url,
                opportunity_type=opportunity_type,
                seo_data=seo_context,
                product_context=products,
                site_url=settings.gsc_site_url or "",
            )

            if not result:
                return None

            # 5. Save to database
            draft = BlogDraft(
                source_query=query,
                source_page=page_url,
                opportunity_type=opportunity_type,
                position_at_generation=current.get("position"),
                impressions_at_generation=current.get("impressions", 0),
                clicks_at_generation=current.get("clicks", 0),
                click_gap_at_generation=drill_down.get("click_gap", 0),
                title=result.get("title", query),
                meta_description=result.get("meta_description", ""),
                slug=result.get("slug", ""),
                content_html=result.get("content_html", ""),
                outline=result.get("outline"),
                target_keywords=result.get("target_keywords"),
                internal_links=result.get("internal_links"),
                word_count=result.get("word_count", 0),
                estimated_reading_time=result.get("estimated_reading_time"),
                llm_model=result.get("llm_model"),
                generation_tokens=result.get("generation_tokens"),
                status="draft",
            )
            self.db.add(draft)
            self.db.commit()
            self.db.refresh(draft)

            log.info(f"Saved blog draft #{draft.id} for query: {query}")
            return self._draft_to_dict(draft)

        except Exception as e:
            self.db.rollback()
            log.error(f"Error generating blog draft for '{query}': {e}")
            return None

    def get_ideas_from_competitor(
        self,
        article_id: int,
        num_ideas: int = 4,
    ) -> Optional[List[Dict]]:
        """
        Analyse a competitor article and suggest original content ideas.

        Step 1 of the two-step flow: Ideas → Pick → Generate.
        Returns a list of idea dicts.
        """
        from app.models.competitor_blog import CompetitorArticle

        try:
            article = (
                self.db.query(CompetitorArticle)
                .filter(CompetitorArticle.id == article_id)
                .first()
            )
            if not article:
                log.error(f"Competitor article #{article_id} not found")
                return None

            topic = article.title or ""
            products = self._find_relevant_products(topic)
            if not products and article.excerpt:
                words = [w for w in article.excerpt.split()[:20] if len(w) > 4]
                products = self._find_relevant_products(" ".join(words[:6]))

            ideas = self.llm_service.suggest_competitor_ideas(
                competitor_title=article.title,
                competitor_content=article.content_text or article.excerpt or "",
                competitor_site=article.site_name or article.site_domain,
                competitor_url=article.url,
                product_context=products,
                num_ideas=num_ideas,
            )

            return ideas

        except Exception as e:
            log.error(f"Error getting ideas from article #{article_id}: {e}")
            return None

    async def generate_from_competitor_idea(
        self,
        article_id: int,
        idea_title: str,
        idea_angle: str,
        idea_keywords: List[str],
    ) -> Optional[Dict]:
        """
        Generate a full blog post from a chosen competitor-inspired idea.

        Step 2 of the two-step flow: Ideas → Pick → Generate.
        Saves the result as a BlogDraft with opportunity_type='competitor_spin'.
        """
        from app.models.competitor_blog import CompetitorArticle

        try:
            article = (
                self.db.query(CompetitorArticle)
                .filter(CompetitorArticle.id == article_id)
                .first()
            )
            if not article:
                log.error(f"Competitor article #{article_id} not found")
                return None

            products = self._find_relevant_products(idea_title)
            if not products:
                products = self._find_relevant_products(article.title or "")

            result = self.llm_service.generate_from_idea(
                idea_title=idea_title,
                idea_angle=idea_angle,
                idea_keywords=idea_keywords,
                competitor_title=article.title,
                competitor_url=article.url,
                competitor_site=article.site_name or article.site_domain,
                product_context=products,
                site_url=settings.gsc_site_url or "",
            )

            if not result:
                return None

            draft = BlogDraft(
                source_query=idea_title,
                source_page=article.url,
                opportunity_type="competitor_spin",
                title=result.get("title", idea_title),
                meta_description=result.get("meta_description", ""),
                slug=result.get("slug", ""),
                content_html=result.get("content_html", ""),
                outline=result.get("outline"),
                target_keywords=result.get("target_keywords"),
                internal_links=result.get("internal_links"),
                word_count=result.get("word_count", 0),
                estimated_reading_time=result.get("estimated_reading_time"),
                llm_model=result.get("llm_model"),
                generation_tokens=result.get("generation_tokens"),
                status="draft",
                reviewer_notes=f"Inspired by: {article.site_name} — {article.url}",
            )
            self.db.add(draft)
            self.db.commit()
            self.db.refresh(draft)

            log.info(
                f"Saved competitor idea draft #{draft.id} "
                f"'{idea_title}' inspired by {article.site_domain}"
            )
            return self._draft_to_dict(draft)

        except Exception as e:
            self.db.rollback()
            log.error(f"Error generating from competitor idea: {e}")
            return None

    async def suggest_topics(
        self, days: int = 30, limit: int = 5
    ) -> List[Dict]:
        """Auto-suggest best blog post topics from SEO underperformers."""
        underperformers = self.seo_service.get_underperformers(
            days=days, limit=50
        )

        # Filter to content-worthy opportunities (not just title/meta fixes)
        content_worthy = [
            u
            for u in underperformers
            if u.get("content_decay")
            or (u.get("position") and u["position"] > 7.5)
            or u.get("click_gap", 0) > 20
        ]

        if not content_worthy:
            content_worthy = underperformers[:20]

        suggestions = self.llm_service.suggest_blog_topics(
            underperformers=content_worthy, limit=limit
        )
        return suggestions or []

    def list_drafts(
        self,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict:
        """List blog drafts with optional status filter."""
        query = self.db.query(BlogDraft)
        if status:
            query = query.filter(BlogDraft.status == status)

        total = query.count()
        drafts = (
            query.order_by(desc(BlogDraft.generated_at))
            .offset(offset)
            .limit(limit)
            .all()
        )

        return {
            "total": total,
            "drafts": [self._draft_to_dict(d) for d in drafts],
        }

    def get_draft(self, draft_id: int) -> Optional[Dict]:
        """Get a single draft by ID."""
        draft = (
            self.db.query(BlogDraft)
            .filter(BlogDraft.id == draft_id)
            .first()
        )
        if not draft:
            return None
        return self._draft_to_dict(draft)

    def update_draft_status(
        self,
        draft_id: int,
        status: str,
        reviewer_notes: Optional[str] = None,
    ) -> Optional[Dict]:
        """Update draft status (reviewed, approved, published, rejected)."""
        draft = (
            self.db.query(BlogDraft)
            .filter(BlogDraft.id == draft_id)
            .first()
        )
        if not draft:
            return None

        draft.status = status
        if reviewer_notes:
            draft.reviewer_notes = reviewer_notes
        if status == "reviewed":
            draft.reviewed_at = datetime.utcnow()
        elif status == "published":
            draft.published_at = datetime.utcnow()

        self.db.commit()
        self.db.refresh(draft)
        return self._draft_to_dict(draft)

    def _find_relevant_products(self, query: str) -> List[Dict]:
        """Find products relevant to a search query for internal linking."""
        terms = [t for t in query.lower().split() if len(t) > 2]
        if not terms:
            return []

        products = (
            self.db.query(ShopifyProduct)
            .filter(ShopifyProduct.status == "active")
            .all()
        )

        scored = []
        for p in products:
            title_lower = (p.title or "").lower()
            vendor_lower = (p.vendor or "").lower()
            type_lower = (p.product_type or "").lower()
            searchable = f"{title_lower} {vendor_lower} {type_lower}"

            score = sum(1 for t in terms if t in searchable)
            if score > 0:
                scored.append(
                    (
                        score,
                        {
                            "title": p.title,
                            "handle": p.handle,
                            "vendor": p.vendor,
                            "product_type": p.product_type,
                        },
                    )
                )

        scored.sort(key=lambda x: x[0], reverse=True)
        return [s[1] for s in scored[:8]]

    def _get_related_queries(self, query: str, days: int) -> List[Dict]:
        """Get related queries that rank for the same page."""
        from sqlalchemy import func

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # Find the top page for this query
        top_page = self.seo_service._get_top_page_for_query(
            query, start_date, end_date
        )
        if not top_page:
            return []

        # Get other queries ranking for the same page
        rows = (
            self.db.query(
                SearchConsoleQuery.query.label("query"),
                func.sum(SearchConsoleQuery.impressions).label("impressions"),
                func.avg(SearchConsoleQuery.position).label("position"),
            )
            .filter(
                SearchConsoleQuery.page == top_page,
                SearchConsoleQuery.date >= start_date,
                SearchConsoleQuery.date <= end_date,
                SearchConsoleQuery.query != query,
            )
            .group_by(SearchConsoleQuery.query)
            .order_by(desc(func.sum(SearchConsoleQuery.impressions)))
            .limit(10)
            .all()
        )

        return [
            {
                "query": r.query,
                "impressions": int(r.impressions or 0),
                "position": round(r.position, 1) if r.position else None,
            }
            for r in rows
        ]

    def _draft_to_dict(self, draft: BlogDraft) -> Dict:
        """Convert BlogDraft ORM object to API response dict."""
        return {
            "id": draft.id,
            "source_query": draft.source_query,
            "source_page": draft.source_page,
            "opportunity_type": draft.opportunity_type,
            "seo_snapshot": {
                "position": draft.position_at_generation,
                "impressions": draft.impressions_at_generation,
                "clicks": draft.clicks_at_generation,
                "click_gap": draft.click_gap_at_generation,
                "priority_score": draft.priority_score,
            },
            "title": draft.title,
            "meta_description": draft.meta_description,
            "slug": draft.slug,
            "content_html": draft.content_html,
            "outline": draft.outline,
            "target_keywords": draft.target_keywords,
            "internal_links": draft.internal_links,
            "word_count": draft.word_count,
            "estimated_reading_time": draft.estimated_reading_time,
            "status": draft.status,
            "reviewer_notes": draft.reviewer_notes,
            "llm_model": draft.llm_model,
            "generated_at": draft.generated_at.isoformat()
            if draft.generated_at
            else None,
            "reviewed_at": draft.reviewed_at.isoformat()
            if draft.reviewed_at
            else None,
            "published_at": draft.published_at.isoformat()
            if draft.published_at
            else None,
        }
