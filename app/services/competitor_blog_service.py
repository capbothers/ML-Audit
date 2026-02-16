"""
Competitor Blog Intelligence Service

Syncs, stores, and analyses competitor/supplier blog content.
Surfaces trends, new topics, and content inspiration.
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import desc, func, or_
from sqlalchemy.orm import Session

from app.connectors.competitor_blog_connector import CompetitorBlogConnector, DEFAULT_SITES
from app.models.competitor_blog import CompetitorSite, CompetitorArticle
from app.models.base import SessionLocal
from app.utils.logger import log


class CompetitorBlogService:
    """Service for competitor blog monitoring and intelligence"""

    async def sync_competitor_blogs(self, days: int = 90) -> Dict[str, Any]:
        """
        Scrape all configured competitor blogs and save new articles.

        Args:
            days: How far back to look for articles

        Returns:
            Dict with sync results
        """
        start_time = datetime.utcnow()
        since = datetime.utcnow() - timedelta(days=days)

        db = SessionLocal()
        try:
            # Ensure default sites are registered
            self._ensure_sites_registered(db)

            # Get active sites
            sites = db.query(CompetitorSite).filter(
                CompetitorSite.is_active == True
            ).all()

            site_configs = []
            for site in sites:
                config = {
                    "name": site.name,
                    "domain": site.domain,
                    "site_type": site.site_type,
                    "blog_url": site.blog_url,
                    "feed_url": site.feed_url,
                    "feed_type": site.feed_type or "scrape",
                    "sitemap_url": site.sitemap_url,
                    "article_selector": site.article_selector or "a[href*='/blog']",
                    "title_selector": site.title_selector or "h1",
                    "content_selector": site.content_selector or "article, main",
                }
                site_configs.append(config)

            # Fetch articles
            connector = CompetitorBlogConnector()
            result = await connector.sync(
                start_date=since,
                end_date=datetime.utcnow(),
                sites=site_configs,
            )

            if not result.get("success"):
                return {
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                }

            data = result["data"]
            articles = data.get("articles", [])
            site_stats = data.get("site_stats", {})

            # Save articles
            new_count = 0
            updated_count = 0
            for article_data in articles:
                was_new = self._save_article(db, article_data)
                if was_new:
                    new_count += 1
                else:
                    updated_count += 1

            db.flush()  # Ensure new articles are visible for count queries

            # Update site stats
            for site in sites:
                stats = site_stats.get(site.domain, {})
                site.last_scraped_at = datetime.utcnow()
                if stats.get("success"):
                    site.consecutive_failures = 0
                    site.total_articles = db.query(CompetitorArticle).filter(
                        CompetitorArticle.site_domain == site.domain
                    ).count()
                    # Update last new article date
                    latest = db.query(CompetitorArticle).filter(
                        CompetitorArticle.site_domain == site.domain
                    ).order_by(desc(CompetitorArticle.published_at)).first()
                    if latest and latest.published_at:
                        site.last_new_article_at = latest.published_at
                else:
                    site.consecutive_failures = (site.consecutive_failures or 0) + 1

            db.commit()

            duration = (datetime.utcnow() - start_time).total_seconds()
            return {
                "success": True,
                "new_articles": new_count,
                "updated_articles": updated_count,
                "total_fetched": len(articles),
                "sites_scraped": data.get("sites_scraped", 0),
                "site_stats": site_stats,
                "duration": round(duration, 1),
            }

        except Exception as e:
            db.rollback()
            log.error(f"Competitor blog sync failed: {e}")
            return {"success": False, "error": str(e)}
        finally:
            db.close()

    def _save_article(self, db: Session, data: Dict) -> bool:
        """Save or update an article. Returns True if new."""
        existing = db.query(CompetitorArticle).filter(
            CompetitorArticle.url == data["url"]
        ).first()

        if existing:
            # Update if we got better content
            if data.get("content_text") and len(data["content_text"]) > len(existing.content_text or ""):
                existing.content_text = data["content_text"]
                existing.excerpt = data.get("excerpt")
                existing.word_count = data.get("word_count")
            if data.get("published_at") and not existing.published_at:
                existing.published_at = data["published_at"]
            return False

        article = CompetitorArticle(
            site_domain=data["site_domain"],
            site_name=data.get("site_name"),
            url=data["url"],
            title=data["title"],
            excerpt=data.get("excerpt"),
            content_text=data.get("content_text"),
            author=data.get("author"),
            published_at=data.get("published_at"),
            image_url=data.get("image_url"),
            categories=data.get("categories"),
            word_count=data.get("word_count"),
            has_images=data.get("has_images", False),
            image_count=data.get("image_count", 0),
        )
        db.add(article)
        return True

    def _ensure_sites_registered(self, db: Session):
        """Register default sites if not already present"""
        for site_config in DEFAULT_SITES:
            existing = db.query(CompetitorSite).filter(
                CompetitorSite.domain == site_config["domain"]
            ).first()
            if not existing:
                site = CompetitorSite(
                    name=site_config["name"],
                    domain=site_config["domain"],
                    site_type=site_config.get("site_type", "competitor"),
                    blog_url=site_config.get("blog_url"),
                    feed_url=site_config.get("feed_url"),
                    feed_type=site_config.get("feed_type"),
                    sitemap_url=site_config.get("sitemap_url"),
                    article_selector=site_config.get("article_selector"),
                    title_selector=site_config.get("title_selector"),
                    content_selector=site_config.get("content_selector"),
                )
                db.add(site)
        db.commit()

    # ── Query Methods ──────────────────────────────────────────

    def get_dashboard(self, db: Session) -> Dict[str, Any]:
        """Get competitor blog dashboard data"""
        sites = db.query(CompetitorSite).filter(
            CompetitorSite.is_active == True
        ).all()

        # Recent articles (last 30 days)
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        recent_articles = db.query(CompetitorArticle).filter(
            CompetitorArticle.scraped_at >= thirty_days_ago
        ).order_by(desc(CompetitorArticle.published_at)).limit(50).all()

        # Articles per site
        site_counts = db.query(
            CompetitorArticle.site_domain,
            CompetitorArticle.site_name,
            func.count(CompetitorArticle.id).label("total"),
            func.max(CompetitorArticle.published_at).label("latest"),
        ).group_by(
            CompetitorArticle.site_domain, CompetitorArticle.site_name
        ).all()

        # Flagged articles
        flagged = db.query(CompetitorArticle).filter(
            CompetitorArticle.is_flagged == True
        ).order_by(desc(CompetitorArticle.published_at)).limit(20).all()

        return {
            "sites": [
                {
                    "id": s.id,
                    "name": s.name,
                    "domain": s.domain,
                    "site_type": s.site_type,
                    "is_active": s.is_active,
                    "total_articles": s.total_articles or 0,
                    "last_scraped_at": s.last_scraped_at.isoformat() if s.last_scraped_at else None,
                    "last_new_article_at": s.last_new_article_at.isoformat() if s.last_new_article_at else None,
                    "consecutive_failures": s.consecutive_failures or 0,
                }
                for s in sites
            ],
            "site_article_counts": [
                {
                    "domain": row.site_domain,
                    "name": row.site_name,
                    "total": row.total,
                    "latest": row.latest.isoformat() if row.latest else None,
                }
                for row in site_counts
            ],
            "recent_articles": [self._article_to_dict(a) for a in recent_articles],
            "flagged_articles": [self._article_to_dict(a) for a in flagged],
            "total_articles": db.query(CompetitorArticle).count(),
            "total_sites": len(sites),
        }

    def get_articles(
        self,
        db: Session,
        domain: Optional[str] = None,
        site_type: Optional[str] = None,
        search: Optional[str] = None,
        flagged_only: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get filtered list of competitor articles"""
        query = db.query(CompetitorArticle)

        if domain:
            query = query.filter(CompetitorArticle.site_domain == domain)
        if site_type:
            # Join to get site_type
            query = query.join(
                CompetitorSite,
                CompetitorArticle.site_domain == CompetitorSite.domain,
            ).filter(CompetitorSite.site_type == site_type)
        if search:
            pattern = f"%{search}%"
            query = query.filter(
                or_(
                    CompetitorArticle.title.ilike(pattern),
                    CompetitorArticle.excerpt.ilike(pattern),
                    CompetitorArticle.content_text.ilike(pattern),
                )
            )
        if flagged_only:
            query = query.filter(CompetitorArticle.is_flagged == True)

        total = query.count()
        articles = query.order_by(
            desc(CompetitorArticle.published_at)
        ).offset(offset).limit(limit).all()

        return {
            "articles": [self._article_to_dict(a) for a in articles],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def flag_article(
        self, db: Session, article_id: int, reason: str = None, notes: str = None
    ) -> Optional[Dict]:
        """Flag an article as interesting/inspirational"""
        article = db.query(CompetitorArticle).get(article_id)
        if not article:
            return None

        article.is_flagged = True
        article.flag_reason = reason
        article.inspiration_notes = notes
        db.commit()
        return self._article_to_dict(article)

    def unflag_article(self, db: Session, article_id: int) -> Optional[Dict]:
        """Remove flag from an article"""
        article = db.query(CompetitorArticle).get(article_id)
        if not article:
            return None

        article.is_flagged = False
        article.flag_reason = None
        article.inspiration_notes = None
        db.commit()
        return self._article_to_dict(article)

    def add_site(self, db: Session, data: Dict) -> Dict:
        """Add a new competitor site to monitor"""
        existing = db.query(CompetitorSite).filter(
            CompetitorSite.domain == data["domain"]
        ).first()
        if existing:
            return {"error": f"Site {data['domain']} already exists", "id": existing.id}

        site = CompetitorSite(
            name=data["name"],
            domain=data["domain"],
            site_type=data.get("site_type", "competitor"),
            blog_url=data.get("blog_url"),
            feed_url=data.get("feed_url"),
            feed_type=data.get("feed_type", "scrape"),
            sitemap_url=data.get("sitemap_url"),
            article_selector=data.get("article_selector"),
            title_selector=data.get("title_selector"),
            content_selector=data.get("content_selector"),
        )
        db.add(site)
        db.commit()
        db.refresh(site)
        return {"id": site.id, "name": site.name, "domain": site.domain}

    def remove_site(self, db: Session, site_id: int) -> bool:
        """Deactivate a competitor site"""
        site = db.query(CompetitorSite).get(site_id)
        if not site:
            return False
        site.is_active = False
        db.commit()
        return True

    def _article_to_dict(self, a: CompetitorArticle) -> Dict:
        return {
            "id": a.id,
            "site_domain": a.site_domain,
            "site_name": a.site_name,
            "url": a.url,
            "title": a.title,
            "excerpt": a.excerpt[:300] if a.excerpt else None,
            "author": a.author,
            "published_at": a.published_at.isoformat() if a.published_at else None,
            "image_url": a.image_url,
            "categories": a.categories,
            "tags": a.tags,
            "word_count": a.word_count,
            "has_images": a.has_images,
            "image_count": a.image_count,
            "relevance_score": a.relevance_score,
            "is_flagged": a.is_flagged,
            "flag_reason": a.flag_reason,
            "inspiration_notes": a.inspiration_notes,
            "scraped_at": a.scraped_at.isoformat() if a.scraped_at else None,
        }
