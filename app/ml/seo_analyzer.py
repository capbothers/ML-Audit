"""
SEO Analysis Module
Analyzes website for SEO issues, technical problems, and optimization opportunities
"""
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse
import advertools as adv
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from app.utils.logger import log


class SEOAnalyzer:
    """
    Comprehensive SEO analysis tool
    """

    def __init__(self):
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504)
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def analyze_page(self, url: str) -> Dict:
        """
        Comprehensive analysis of a single page
        """
        log.info(f"Analyzing SEO for: {url}")

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            analysis = {
                'url': url,
                'status_code': response.status_code,
                'title': self._analyze_title(soup),
                'meta_description': self._analyze_meta_description(soup),
                'headings': self._analyze_headings(soup),
                'images': self._analyze_images(soup),
                'links': self._analyze_links(soup, url),
                'content': self._analyze_content(soup),
                'technical': self._analyze_technical(response, soup),
                'mobile': self._analyze_mobile(soup),
                'performance': self._analyze_performance(response),
                'issues': [],
                'warnings': [],
                'score': 0
            }

            # Calculate issues and score
            analysis['issues'], analysis['warnings'] = self._identify_issues(analysis)
            analysis['score'] = self._calculate_seo_score(analysis)

            log.info(f"SEO analysis complete for {url} - Score: {analysis['score']}")
            return analysis

        except Exception as e:
            log.error(f"Error analyzing {url}: {str(e)}")
            return {
                'url': url,
                'error': str(e),
                'score': 0
            }

    def _analyze_title(self, soup: BeautifulSoup) -> Dict:
        """Analyze page title"""
        title_tag = soup.find('title')
        title = title_tag.get_text() if title_tag else ""

        return {
            'text': title,
            'length': len(title),
            'exists': bool(title),
            'optimal_length': 30 <= len(title) <= 60,
            'has_keywords': len(title.split()) >= 3
        }

    def _analyze_meta_description(self, soup: BeautifulSoup) -> Dict:
        """Analyze meta description"""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        description = meta_desc.get('content', '') if meta_desc else ""

        return {
            'text': description,
            'length': len(description),
            'exists': bool(description),
            'optimal_length': 120 <= len(description) <= 160,
        }

    def _analyze_headings(self, soup: BeautifulSoup) -> Dict:
        """Analyze heading structure"""
        headings = {
            'h1': [h.get_text().strip() for h in soup.find_all('h1')],
            'h2': [h.get_text().strip() for h in soup.find_all('h2')],
            'h3': [h.get_text().strip() for h in soup.find_all('h3')],
        }

        return {
            'h1_count': len(headings['h1']),
            'h2_count': len(headings['h2']),
            'h3_count': len(headings['h3']),
            'has_single_h1': len(headings['h1']) == 1,
            'has_hierarchy': len(headings['h1']) > 0 and len(headings['h2']) > 0,
            'headings': headings
        }

    def _analyze_images(self, soup: BeautifulSoup) -> Dict:
        """Analyze images for SEO"""
        images = soup.find_all('img')

        images_without_alt = 0
        images_without_title = 0
        large_images = 0

        for img in images:
            if not img.get('alt'):
                images_without_alt += 1
            if not img.get('title'):
                images_without_title += 1

        return {
            'total_images': len(images),
            'images_without_alt': images_without_alt,
            'images_without_title': images_without_title,
            'alt_text_coverage': ((len(images) - images_without_alt) / len(images) * 100) if images else 100
        }

    def _analyze_links(self, soup: BeautifulSoup, base_url: str) -> Dict:
        """Analyze internal and external links"""
        links = soup.find_all('a', href=True)
        base_domain = urlparse(base_url).netloc

        internal_links = 0
        external_links = 0
        broken_links = 0
        nofollow_links = 0

        for link in links:
            href = link.get('href', '')

            # Skip anchors and javascript
            if href.startswith('#') or href.startswith('javascript:'):
                continue

            link_domain = urlparse(href).netloc

            if link_domain == base_domain or not link_domain:
                internal_links += 1
            else:
                external_links += 1

            if link.get('rel') and 'nofollow' in link.get('rel'):
                nofollow_links += 1

        return {
            'total_links': len(links),
            'internal_links': internal_links,
            'external_links': external_links,
            'nofollow_links': nofollow_links,
            'has_sufficient_internal_links': internal_links >= 3
        }

    def _analyze_content(self, soup: BeautifulSoup) -> Dict:
        """Analyze page content"""
        # Remove script and style elements
        for script in soup(['script', 'style']):
            script.decompose()

        text = soup.get_text()
        words = text.split()

        return {
            'word_count': len(words),
            'sufficient_content': len(words) >= 300,
            'character_count': len(text),
        }

    def _analyze_technical(self, response: requests.Response, soup: BeautifulSoup) -> Dict:
        """Analyze technical SEO factors"""
        # Check SSL
        has_ssl = response.url.startswith('https://')

        # Check canonical tag
        canonical = soup.find('link', rel='canonical')
        has_canonical = bool(canonical)

        # Check robots meta
        robots_meta = soup.find('meta', attrs={'name': 'robots'})
        robots_content = robots_meta.get('content', '') if robots_meta else ""

        # Check schema markup
        has_schema = bool(soup.find('script', type='application/ld+json'))

        # Check viewport meta
        viewport = soup.find('meta', attrs={'name': 'viewport'})
        has_viewport = bool(viewport)

        return {
            'has_ssl': has_ssl,
            'has_canonical': has_canonical,
            'has_robots_meta': bool(robots_meta),
            'robots_content': robots_content,
            'has_schema_markup': has_schema,
            'has_viewport': has_viewport,
            'response_time': response.elapsed.total_seconds()
        }

    def _analyze_mobile(self, soup: BeautifulSoup) -> Dict:
        """Analyze mobile-friendliness"""
        viewport = soup.find('meta', attrs={'name': 'viewport'})
        has_viewport = bool(viewport)

        viewport_content = viewport.get('content', '') if viewport else ""
        has_responsive_viewport = 'width=device-width' in viewport_content

        return {
            'has_viewport': has_viewport,
            'has_responsive_viewport': has_responsive_viewport,
            'likely_mobile_friendly': has_responsive_viewport
        }

    def _analyze_performance(self, response: requests.Response) -> Dict:
        """Analyze performance metrics"""
        response_time = response.elapsed.total_seconds()
        page_size = len(response.content)

        return {
            'response_time_seconds': response_time,
            'page_size_bytes': page_size,
            'page_size_kb': page_size / 1024,
            'fast_response': response_time < 2.0,
            'reasonable_size': page_size < 2 * 1024 * 1024  # Less than 2MB
        }

    def _identify_issues(self, analysis: Dict) -> tuple[List[Dict], List[Dict]]:
        """Identify SEO issues and warnings"""
        issues = []
        warnings = []

        # Title issues
        if not analysis['title']['exists']:
            issues.append({'type': 'title', 'severity': 'critical', 'message': 'Page has no title tag'})
        elif not analysis['title']['optimal_length']:
            warnings.append({'type': 'title', 'severity': 'medium', 'message': f"Title length ({analysis['title']['length']}) is not optimal (30-60 chars)"})

        # Meta description
        if not analysis['meta_description']['exists']:
            warnings.append({'type': 'meta', 'severity': 'medium', 'message': 'Page has no meta description'})
        elif not analysis['meta_description']['optimal_length']:
            warnings.append({'type': 'meta', 'severity': 'low', 'message': f"Meta description length ({analysis['meta_description']['length']}) is not optimal (120-160 chars)"})

        # Headings
        if not analysis['headings']['has_single_h1']:
            issues.append({'type': 'headings', 'severity': 'high', 'message': f"Page should have exactly one H1 tag (found {analysis['headings']['h1_count']})"})

        if not analysis['headings']['has_hierarchy']:
            warnings.append({'type': 'headings', 'severity': 'medium', 'message': 'Page lacks proper heading hierarchy'})

        # Images
        if analysis['images']['alt_text_coverage'] < 80:
            warnings.append({'type': 'images', 'severity': 'medium', 'message': f"Only {analysis['images']['alt_text_coverage']:.0f}% of images have alt text"})

        # Links
        if not analysis['links']['has_sufficient_internal_links']:
            warnings.append({'type': 'links', 'severity': 'low', 'message': 'Page has few internal links'})

        # Content
        if not analysis['content']['sufficient_content']:
            warnings.append({'type': 'content', 'severity': 'medium', 'message': f"Page has thin content ({analysis['content']['word_count']} words, recommended 300+)"})

        # Technical
        if not analysis['technical']['has_ssl']:
            issues.append({'type': 'technical', 'severity': 'critical', 'message': 'Site is not using HTTPS'})

        if not analysis['technical']['has_canonical']:
            warnings.append({'type': 'technical', 'severity': 'low', 'message': 'Page lacks canonical tag'})

        if not analysis['technical']['has_viewport']:
            issues.append({'type': 'mobile', 'severity': 'high', 'message': 'Page lacks viewport meta tag (not mobile-friendly)'})

        # Performance
        if not analysis['performance']['fast_response']:
            warnings.append({'type': 'performance', 'severity': 'medium', 'message': f"Slow response time ({analysis['performance']['response_time_seconds']:.2f}s)"})

        return issues, warnings

    def _calculate_seo_score(self, analysis: Dict) -> float:
        """Calculate overall SEO score (0-100)"""
        score = 100.0

        # Deduct points for issues
        for issue in analysis['issues']:
            if issue['severity'] == 'critical':
                score -= 15
            elif issue['severity'] == 'high':
                score -= 10
            elif issue['severity'] == 'medium':
                score -= 5

        # Deduct points for warnings
        for warning in analysis['warnings']:
            if warning['severity'] == 'high':
                score -= 5
            elif warning['severity'] == 'medium':
                score -= 3
            elif warning['severity'] == 'low':
                score -= 1

        return max(0, score)

    def audit_site(self, urls: List[str]) -> Dict:
        """
        Audit multiple pages and generate summary
        """
        log.info(f"Starting SEO audit for {len(urls)} pages")

        results = []
        for url in urls:
            result = self.analyze_page(url)
            results.append(result)

        # Calculate summary statistics
        avg_score = sum(r.get('score', 0) for r in results) / len(results)
        total_issues = sum(len(r.get('issues', [])) for r in results)
        total_warnings = sum(len(r.get('warnings', [])) for r in results)

        # Find common issues
        all_issues = []
        for r in results:
            all_issues.extend(r.get('issues', []))

        issue_counts = {}
        for issue in all_issues:
            key = issue['message']
            issue_counts[key] = issue_counts.get(key, 0) + 1

        common_issues = sorted(
            issue_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        summary = {
            'total_pages_analyzed': len(urls),
            'average_score': avg_score,
            'total_issues': total_issues,
            'total_warnings': total_warnings,
            'common_issues': [{'issue': k, 'count': v} for k, v in common_issues],
            'pages': results
        }

        log.info(f"SEO audit complete - Average score: {avg_score:.1f}")
        return summary
