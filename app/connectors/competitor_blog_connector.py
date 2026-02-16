"""
Competitor Blog Connector

Scrapes blog content from competitor and supplier websites using:
1. RSS/Atom feeds (preferred — thebluespace via Shopify Atom)
2. Sitemap + HTML scraping (fallback for sites without feeds)
3. Direct HTML scraping (last resort)
"""
import asyncio
import hashlib
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
import feedparser
from bs4 import BeautifulSoup

from app.connectors.base_connector import BaseConnector
from app.utils.logger import log

# Default competitor/supplier sites to monitor
DEFAULT_SITES = [
    {
        "name": "The Blue Space",
        "domain": "thebluespace.com.au",
        "site_type": "competitor",
        "blog_url": "https://www.thebluespace.com.au/blogs/learn",
        "feed_url": "https://www.thebluespace.com.au/blogs/learn.atom",
        "feed_type": "atom",
    },
    {
        "name": "ADP Australia",
        "domain": "adpaustralia.com.au",
        "site_type": "competitor",
        "blog_url": "https://www.adpaustralia.com.au/blog",
        "feed_type": "scrape",
        "article_selector": "a[href*='/blog/']",
        "title_selector": "h1",
        "content_selector": "article, .blog-content, .entry-content, main",
    },
    {
        "name": "Argent Australia",
        "domain": "argentaust.com.au",
        "site_type": "supplier",
        "blog_url": "https://www.argentaust.com.au/products",
        "feed_type": "scrape",
        "sitemap_url": "https://www.argentaust.com.au/sitemap.xml",
        "article_selector": "a[href*='/blog'], a[href*='/news'], a[href*='/article']",
        "content_selector": "article, .content, main",
    },
    {
        "name": "Reece",
        "domain": "reece.com.au",
        "site_type": "competitor",
        "blog_url": "https://www.reece.com.au/articles",
        "feed_type": "scrape",
        "article_selector": "a[href*='/articles/']",
        "title_selector": "h1",
        "content_selector": "article, main, .article-content",
    },
    {
        "name": "Zip Water",
        "domain": "zipwater.com",
        "site_type": "supplier",
        "blog_url": "https://www.zipwater.com/news",
        "feed_type": "scrape",
        "article_selector": "a[href*='/news/'], a[href*='/case-studies/']",
        "title_selector": "h1",
        "content_selector": "article, main, .content",
    },
    {
        "name": "Just Bathroomware",
        "domain": "justbathroomware.com.au",
        "site_type": "competitor",
        "blog_url": "https://justbathroomware.com.au/",
        "feed_type": "scrape",
        "article_selector": "a[href*='/blog'], a[href*='/blogs/']",
        "content_selector": "article, main, .blog-content",
    },
]

# Standard headers to avoid bot detection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MLAudit/1.0; +https://example.com/bot)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}


class CompetitorBlogConnector(BaseConnector):
    """Connector for scraping competitor and supplier blog content"""

    RETRY_MAX_ATTEMPTS = 2
    RETRY_BASE_DELAY = 5.0

    def __init__(self):
        super().__init__("competitor_blogs")
        self.session: Optional[aiohttp.ClientSession] = None

    async def connect(self) -> bool:
        """Create HTTP session"""
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                headers=HEADERS,
                timeout=timeout,
            )
        return True

    async def validate_connection(self) -> bool:
        """Validate we can make HTTP requests"""
        await self.connect()
        return self.session is not None and not self.session.closed

    async def close(self):
        """Close HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def fetch_data(
        self, start_date: datetime, end_date: datetime, sites: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """
        Fetch blog articles from all configured competitor sites.

        Args:
            start_date: Only return articles published after this date
            end_date: Only return articles published before this date
            sites: Optional list of site configs (defaults to DEFAULT_SITES)

        Returns:
            Dict with articles list and per-site stats
        """
        await self.connect()
        target_sites = sites or DEFAULT_SITES
        all_articles = []
        site_stats = {}

        for site in target_sites:
            domain = site["domain"]
            try:
                articles = await self._fetch_site(site, start_date)
                site_stats[domain] = {
                    "success": True,
                    "articles_found": len(articles),
                }
                all_articles.extend(articles)
                log.info(f"Fetched {len(articles)} articles from {domain}")
            except Exception as e:
                log.warning(f"Failed to scrape {domain}: {e}")
                site_stats[domain] = {
                    "success": False,
                    "error": str(e),
                    "articles_found": 0,
                }

            # Be polite — wait between sites
            await asyncio.sleep(2)

        await self.close()

        return {
            "articles": all_articles,
            "site_stats": site_stats,
            "total_articles": len(all_articles),
            "sites_scraped": len(target_sites),
        }

    async def _fetch_site(
        self, site: Dict, since: datetime
    ) -> List[Dict]:
        """Fetch articles from a single site using the best available method"""
        feed_type = site.get("feed_type", "scrape")

        if feed_type in ("rss", "atom") and site.get("feed_url"):
            return await self._fetch_feed(site, since)
        else:
            return await self._fetch_by_scraping(site, since)

    async def _fetch_feed(self, site: Dict, since: datetime) -> List[Dict]:
        """Parse RSS/Atom feed"""
        feed_url = site["feed_url"]
        domain = site["domain"]

        async with self.session.get(feed_url) as resp:
            if resp.status != 200:
                raise Exception(f"Feed returned {resp.status}")
            body = await resp.text()

        feed = feedparser.parse(body)
        articles = []

        for entry in feed.entries:
            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                published = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                published = datetime(*entry.updated_parsed[:6])

            if published and published < since:
                continue

            # Extract content
            content_text = ""
            if hasattr(entry, "content"):
                content_html = entry.content[0].get("value", "")
                content_text = self._html_to_text(content_html)
            elif hasattr(entry, "summary"):
                content_text = self._html_to_text(entry.summary)

            excerpt = content_text[:500] if content_text else ""

            articles.append({
                "site_domain": domain,
                "site_name": site["name"],
                "url": entry.link,
                "title": entry.title,
                "excerpt": excerpt,
                "content_text": content_text,
                "author": getattr(entry, "author", None),
                "published_at": published,
                "categories": [t.term for t in getattr(entry, "tags", [])],
                "image_url": self._extract_image(entry),
                "word_count": len(content_text.split()) if content_text else 0,
            })

        return articles

    async def _fetch_by_scraping(self, site: Dict, since: datetime) -> List[Dict]:
        """Scrape a blog listing page + individual articles"""
        blog_url = site.get("blog_url")
        domain = site["domain"]
        if not blog_url:
            return []

        # Step 1: Get the blog listing page
        try:
            async with self.session.get(blog_url) as resp:
                if resp.status != 200:
                    raise Exception(f"Blog page returned {resp.status}")
                html = await resp.text()
        except Exception as e:
            log.warning(f"Could not fetch blog listing for {domain}: {e}")
            return []

        soup = BeautifulSoup(html, "lxml")
        base_url = f"https://www.{domain}"

        # Step 2: Find article links
        article_selector = site.get("article_selector", "a[href*='/blog']")
        links = set()

        for a in soup.select(article_selector):
            href = a.get("href", "")
            if not href or href == "#":
                continue
            full_url = urljoin(base_url, href)
            # Only keep links on the same domain
            if domain in urlparse(full_url).netloc:
                links.add(full_url)

        # Also look for common blog link patterns
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            if domain not in parsed.netloc:
                continue
            path = parsed.path.lower()
            if any(seg in path for seg in ["/blog/", "/blogs/", "/news/", "/articles/", "/article/", "/case-stud"]):
                # Skip listing/index pages
                segments = [s for s in path.strip("/").split("/") if s]
                if len(segments) >= 2:
                    links.add(full_url)

        # Remove the listing page itself
        links.discard(blog_url)
        links.discard(blog_url.rstrip("/"))
        links.discard(blog_url + "/")

        if not links:
            log.info(f"No article links found on {domain}")
            return []

        # Step 3: Fetch individual articles (limit to 20 per scrape to be polite)
        articles = []
        for url in list(links)[:20]:
            try:
                article = await self._scrape_article(url, site)
                if article:
                    articles.append(article)
                await asyncio.sleep(1)  # Rate limit
            except Exception as e:
                log.debug(f"Failed to scrape article {url}: {e}")

        return articles

    async def _scrape_article(self, url: str, site: Dict) -> Optional[Dict]:
        """Scrape a single article page"""
        domain = site["domain"]

        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
        except Exception:
            return None

        soup = BeautifulSoup(html, "lxml")

        # Title
        title_sel = site.get("title_selector", "h1")
        title_el = soup.select_one(title_sel)
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            title_el = soup.find("title")
            title = title_el.get_text(strip=True) if title_el else url.split("/")[-1]

        # Skip non-article pages
        if len(title) < 5 or title.lower() in ("blog", "news", "articles", "home"):
            return None

        # Content
        content_sel = site.get("content_selector", "article, main")
        content_el = soup.select_one(content_sel)
        content_text = ""
        if content_el:
            # Remove nav, header, footer, script, style
            for tag in content_el.find_all(["nav", "header", "footer", "script", "style", "form"]):
                tag.decompose()
            content_text = content_el.get_text(separator=" ", strip=True)

        word_count = len(content_text.split()) if content_text else 0
        if word_count < 50:
            return None  # Probably not an article

        # Published date
        published_at = self._extract_date(soup)

        # Images
        images = soup.find_all("img")
        og_image = soup.find("meta", property="og:image")
        image_url = og_image["content"] if og_image and og_image.get("content") else None

        # Author
        author = None
        author_el = soup.find(class_=re.compile(r"author", re.I)) or soup.find("meta", attrs={"name": "author"})
        if author_el:
            author = author_el.get("content") if author_el.name == "meta" else author_el.get_text(strip=True)

        # Categories from meta
        categories = []
        for meta in soup.find_all("meta", attrs={"property": "article:tag"}):
            if meta.get("content"):
                categories.append(meta["content"])

        excerpt = content_text[:500] if content_text else ""

        return {
            "site_domain": domain,
            "site_name": site["name"],
            "url": url,
            "title": title,
            "excerpt": excerpt,
            "content_text": content_text[:10000],  # Cap stored content
            "author": author,
            "published_at": published_at,
            "categories": categories or None,
            "image_url": image_url,
            "word_count": word_count,
            "has_images": len(images) > 0,
            "image_count": len(images),
        }

    def _html_to_text(self, html: str) -> str:
        """Convert HTML to plain text"""
        soup = BeautifulSoup(html, "lxml")
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)

    def _extract_image(self, feed_entry) -> Optional[str]:
        """Extract image URL from feed entry"""
        # Check media_content
        if hasattr(feed_entry, "media_content"):
            for media in feed_entry.media_content:
                if "image" in media.get("type", ""):
                    return media["url"]

        # Check enclosures
        if hasattr(feed_entry, "enclosures"):
            for enc in feed_entry.enclosures:
                if "image" in enc.get("type", ""):
                    return enc.get("href")

        # Check content for img tags
        if hasattr(feed_entry, "content"):
            html = feed_entry.content[0].get("value", "")
            soup = BeautifulSoup(html, "lxml")
            img = soup.find("img")
            if img and img.get("src"):
                return img["src"]

        return None

    def _extract_date(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Try to extract published date from HTML"""
        # Try meta tags
        for prop in ["article:published_time", "datePublished", "og:published_time"]:
            meta = soup.find("meta", attrs={"property": prop}) or soup.find("meta", attrs={"name": prop})
            if meta and meta.get("content"):
                try:
                    return datetime.fromisoformat(meta["content"].replace("Z", "+00:00").split("+")[0])
                except (ValueError, TypeError):
                    pass

        # Try time tag
        time_el = soup.find("time")
        if time_el and time_el.get("datetime"):
            try:
                return datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00").split("+")[0])
            except (ValueError, TypeError):
                pass

        # Try JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, list):
                    data = data[0]
                date_str = data.get("datePublished") or data.get("dateCreated")
                if date_str:
                    return datetime.fromisoformat(date_str.replace("Z", "+00:00").split("+")[0])
            except (json.JSONDecodeError, TypeError, KeyError, ValueError):
                pass

        return None
