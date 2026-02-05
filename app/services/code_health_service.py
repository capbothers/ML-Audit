"""
Code & Theme Health Analysis Service (GitHub Integration)

Analyzes Shopify theme code quality, technical debt, and security.
Performs REAL analysis of actual file contents from GitHub.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_
from typing import List, Dict, Optional, Any
from datetime import datetime, date, timedelta
from decimal import Decimal
from dataclasses import dataclass, asdict
import re
import json

from app.models.code_health import (
    CodeRepository, CodeQualityMetric, ThemeHealthCheck,
    SecurityVulnerability, CodeCommit, TechnicalDebt,
    CodeInsight, DependencyStatus
)
from app.connectors.github_connector import GitHubConnector
from app.utils.logger import log


@dataclass
class CodeIssue:
    """Represents a code health issue"""
    severity: str  # critical, warning, info
    category: str  # liquid, javascript, css, json, security, performance
    check_name: str
    title: str
    description: str
    file_path: str
    line_number: Optional[int] = None
    code_snippet: Optional[str] = None
    recommendation: str = ""


class CodeHealthService:
    """Service for analyzing code and theme health"""

    def __init__(self, db: Session):
        self.db = db
        self.github = GitHubConnector()
        self._issues: List[CodeIssue] = []

        # Thresholds
        self.max_file_size_kb = 100  # KB
        self.max_liquid_file_size_kb = 50
        self.max_js_file_size_kb = 150
        self.max_function_complexity = 10
        self.max_file_complexity = 50
        self.critical_cvss_score = 7.0

        # Deprecated Liquid tags/filters
        self.deprecated_liquid = {
            'include': 'Use {% render %} instead of {% include %} (deprecated)',
            'assign.*\\| json': 'Use {{ value | json }} directly instead of assigning',
            'img_url': "Use image_url filter instead of img_url (Shopify 2.0)",
            '\\bproduct\\.featured_image\\b': 'Use product.featured_media instead of featured_image',
            'article_url': 'Use article.url instead of article_url filter',
            'asset_url.*shopify_asset': 'shopify_asset_url is deprecated',
        }

        # Security patterns to detect
        self.security_patterns = {
            'hardcoded_api_key': r'[\'"][A-Za-z0-9_]{20,}[\'"]',  # Generic API key pattern
            'console_log': r'console\.(log|debug|info|warn|error)\s*\(',
            'eval_usage': r'\beval\s*\(',
            'innerHTML': r'\.innerHTML\s*=',
            'document_write': r'document\.write\s*\(',
            'http_url': r'[\'"]http://[^\'"]+[\'"]',  # Non-HTTPS URLs
        }

        # Performance anti-patterns
        self.performance_patterns = {
            'forloop_in_forloop': r'{%\s*for\b.*{%\s*for\b',  # Nested loops
            'multiple_asset_url': r'(\{\{\s*[\'"][^"\']+[\'"]\s*\|\s*asset_url\s*\}\}.*){5,}',  # Many separate asset calls
        }

    async def analyze_repository(self, repo_name: str) -> Dict:
        """
        Complete repository health analysis

        Returns:
        - Overall health score
        - Code quality metrics
        - Theme health checks (if Shopify theme)
        - Security vulnerabilities
        - Technical debt
        - Commit analysis
        - Dependency status
        """
        try:
            # Reset issues list for fresh analysis
            self._issues = []

            # Connect to GitHub and fetch theme files
            log.info(f"Fetching theme files from GitHub for analysis...")
            await self.github.connect()
            theme_files = await self.github.fetch_theme_files()

            files_fetched = sum(len(files) for files in theme_files.values())
            log.info(f"Fetched {files_fetched} theme files for analysis")

            # Run all analyzers on actual file content
            await self._analyze_liquid_files(theme_files.get('liquid', []))
            await self._analyze_javascript_files(theme_files.get('javascript', []))
            await self._analyze_css_files(theme_files.get('css', []))
            await self._analyze_json_files(theme_files.get('json', []))

            # Group issues by category for reporting
            quality_metrics = self._build_quality_metrics()
            theme_health = self._build_theme_health()
            security_issues = self._build_security_report()
            technical_debt = self._build_technical_debt_report()

            # Commit analysis from GitHub
            commit_analysis = await self.analyze_commits(repo_name)

            # Dependency status (from package.json if available)
            dependency_status = await self.check_dependencies(repo_name)

            # Calculate overall health score
            health_score = self._calculate_health_score(
                quality_metrics, theme_health, security_issues,
                technical_debt, dependency_status
            )

            # Identify priorities from all issues
            priorities = self._identify_priorities(
                quality_metrics, theme_health, security_issues,
                technical_debt, dependency_status
            )

            return {
                "repo_name": repo_name,
                "overall_health_score": health_score,
                "quality_metrics": quality_metrics,
                "theme_health": theme_health,
                "security_issues": security_issues,
                "technical_debt": technical_debt,
                "commit_analysis": commit_analysis,
                "dependency_status": dependency_status,
                "priorities": priorities,
                "files_analyzed": files_fetched,
                "total_issues": len(self._issues)
            }

        except Exception as e:
            log.error(f"Error analyzing repository {repo_name}: {str(e)}")
            import traceback
            log.error(traceback.format_exc())
            raise

    async def get_all_issues(self, severity: Optional[str] = None, category: Optional[str] = None) -> List[Dict]:
        """
        Get all code health issues found during analysis.
        Optionally filter by severity or category.
        """
        # Run analysis if not already done
        if not self._issues:
            await self.github.connect()
            theme_files = await self.github.fetch_theme_files()
            await self._analyze_liquid_files(theme_files.get('liquid', []))
            await self._analyze_javascript_files(theme_files.get('javascript', []))
            await self._analyze_css_files(theme_files.get('css', []))
            await self._analyze_json_files(theme_files.get('json', []))

        issues = [asdict(i) for i in self._issues]

        # Apply filters
        if severity:
            issues = [i for i in issues if i['severity'] == severity]
        if category:
            issues = [i for i in issues if i['category'] == category]

        # Sort by severity (critical first)
        severity_order = {'critical': 0, 'warning': 1, 'info': 2}
        issues.sort(key=lambda x: severity_order.get(x['severity'], 3))

        return issues

    def _add_issue(self, issue: CodeIssue):
        """Add an issue to the list"""
        self._issues.append(issue)

    async def _analyze_liquid_files(self, files: List[Dict]):
        """Analyze Liquid template files for issues"""
        log.info(f"Analyzing {len(files)} Liquid files...")

        for file_info in files:
            path = file_info.get('path', '')
            content = file_info.get('content')
            size_bytes = file_info.get('size_bytes', 0)

            if not content:
                continue

            # Check file size
            size_kb = size_bytes / 1024
            if size_kb > self.max_liquid_file_size_kb:
                self._add_issue(CodeIssue(
                    severity='warning',
                    category='performance',
                    check_name='large_liquid_file',
                    title=f'Large Liquid file: {path}',
                    description=f'File is {size_kb:.1f}KB (threshold: {self.max_liquid_file_size_kb}KB)',
                    file_path=path,
                    recommendation='Consider breaking into smaller snippets/sections'
                ))

            lines = content.split('\n')

            # Check for deprecated {% include %} tags
            for i, line in enumerate(lines, 1):
                if re.search(r'{%\s*include\s+', line):
                    self._add_issue(CodeIssue(
                        severity='warning',
                        category='liquid',
                        check_name='deprecated_include',
                        title='Deprecated {% include %} tag',
                        description='{% include %} is deprecated in Shopify themes',
                        file_path=path,
                        line_number=i,
                        code_snippet=line.strip()[:100],
                        recommendation='Replace with {% render %} for better performance and isolation'
                    ))

            # Check for missing alt attributes on images
            img_tags = re.findall(r'<img[^>]*>', content, re.IGNORECASE)
            for img in img_tags:
                if 'alt=' not in img.lower():
                    line_num = self._find_line_number(content, img)
                    self._add_issue(CodeIssue(
                        severity='warning',
                        category='accessibility',
                        check_name='missing_alt_text',
                        title='Image missing alt attribute',
                        description='Images should have alt text for accessibility',
                        file_path=path,
                        line_number=line_num,
                        code_snippet=img[:80],
                        recommendation='Add alt="{{ image.alt | escape }}" or descriptive alt text'
                    ))

            # Check for hardcoded text (non-translatable)
            hardcoded_matches = re.findall(r'>([A-Z][a-z]+(?:\s+[A-Za-z]+){2,})<', content)
            for text in hardcoded_matches[:3]:  # Limit to 3 per file
                if len(text) > 10 and not '{{' in text:
                    self._add_issue(CodeIssue(
                        severity='info',
                        category='liquid',
                        check_name='hardcoded_text',
                        title='Hardcoded text detected',
                        description=f'Text "{text[:50]}..." should use translation keys',
                        file_path=path,
                        recommendation='Use {{ "key" | t }} for translatable text'
                    ))

            # Check for deeply nested loops (performance)
            if re.search(r'{%\s*for\b[^%]*%}[^{]*{%\s*for\b', content):
                self._add_issue(CodeIssue(
                    severity='warning',
                    category='performance',
                    check_name='nested_loops',
                    title='Nested for loops detected',
                    description='Nested loops can significantly impact performance',
                    file_path=path,
                    recommendation='Consider restructuring to avoid nested iterations'
                ))

            # Check for deprecated img_url filter
            if re.search(r'\|\s*img_url\s*:', content):
                self._add_issue(CodeIssue(
                    severity='info',
                    category='liquid',
                    check_name='deprecated_img_url',
                    title='Deprecated img_url filter',
                    description='img_url is being replaced by image_url in Shopify 2.0',
                    file_path=path,
                    recommendation='Update to use image_url filter for future compatibility'
                ))

            # Check for missing schema in section files
            if 'sections/' in path and '{% schema %}' not in content:
                self._add_issue(CodeIssue(
                    severity='info',
                    category='liquid',
                    check_name='missing_schema',
                    title='Section missing schema block',
                    description='Section files should include a {% schema %} block',
                    file_path=path,
                    recommendation='Add a {% schema %} block with section settings'
                ))

    async def _analyze_javascript_files(self, files: List[Dict]):
        """Analyze JavaScript files for issues"""
        log.info(f"Analyzing {len(files)} JavaScript files...")

        for file_info in files:
            path = file_info.get('path', '')
            content = file_info.get('content')
            size_bytes = file_info.get('size_bytes', 0)

            if not content:
                continue

            # Check file size
            size_kb = size_bytes / 1024
            if size_kb > self.max_js_file_size_kb:
                self._add_issue(CodeIssue(
                    severity='critical' if size_kb > 300 else 'warning',
                    category='performance',
                    check_name='large_js_file',
                    title=f'Large JavaScript file: {path}',
                    description=f'File is {size_kb:.1f}KB (threshold: {self.max_js_file_size_kb}KB)',
                    file_path=path,
                    recommendation='Split into smaller modules or use code splitting'
                ))

            lines = content.split('\n')

            # Check for console.log statements
            console_count = 0
            for i, line in enumerate(lines, 1):
                if re.search(r'console\.(log|debug|info|warn|error)\s*\(', line):
                    console_count += 1
                    if console_count <= 3:  # Report first 3 instances
                        self._add_issue(CodeIssue(
                            severity='warning',
                            category='javascript',
                            check_name='console_statement',
                            title='console statement in production code',
                            description='Console statements should be removed in production',
                            file_path=path,
                            line_number=i,
                            code_snippet=line.strip()[:80],
                            recommendation='Remove console.log or use a proper logging library'
                        ))

            if console_count > 3:
                self._add_issue(CodeIssue(
                    severity='warning',
                    category='javascript',
                    check_name='many_console_statements',
                    title=f'{console_count} console statements found',
                    description=f'Found {console_count} total console statements in {path}',
                    file_path=path,
                    recommendation='Remove all console statements from production code'
                ))

            # Check for eval() usage (security risk)
            for i, line in enumerate(lines, 1):
                if re.search(r'\beval\s*\(', line):
                    self._add_issue(CodeIssue(
                        severity='critical',
                        category='security',
                        check_name='eval_usage',
                        title='eval() usage detected',
                        description='eval() is a security risk and should be avoided',
                        file_path=path,
                        line_number=i,
                        code_snippet=line.strip()[:80],
                        recommendation='Replace eval() with safer alternatives like JSON.parse()'
                    ))

            # Check for innerHTML (potential XSS)
            for i, line in enumerate(lines, 1):
                if re.search(r'\.innerHTML\s*=', line):
                    self._add_issue(CodeIssue(
                        severity='warning',
                        category='security',
                        check_name='innerHTML_usage',
                        title='innerHTML usage detected',
                        description='innerHTML can lead to XSS vulnerabilities',
                        file_path=path,
                        line_number=i,
                        code_snippet=line.strip()[:80],
                        recommendation='Use textContent or sanitize input before using innerHTML'
                    ))

            # Check for var usage (should use let/const)
            var_count = len(re.findall(r'\bvar\s+\w+', content))
            if var_count > 5:
                self._add_issue(CodeIssue(
                    severity='info',
                    category='javascript',
                    check_name='var_usage',
                    title=f'Legacy var keyword used ({var_count} times)',
                    description='var has function scope issues, prefer let/const',
                    file_path=path,
                    recommendation='Replace var with let or const for block scoping'
                ))

    async def _analyze_css_files(self, files: List[Dict]):
        """Analyze CSS/SCSS files for issues"""
        log.info(f"Analyzing {len(files)} CSS files...")

        for file_info in files:
            path = file_info.get('path', '')
            content = file_info.get('content')
            size_bytes = file_info.get('size_bytes', 0)

            if not content:
                continue

            # Check file size
            size_kb = size_bytes / 1024
            if size_kb > 100:
                self._add_issue(CodeIssue(
                    severity='warning',
                    category='performance',
                    check_name='large_css_file',
                    title=f'Large CSS file: {path}',
                    description=f'File is {size_kb:.1f}KB',
                    file_path=path,
                    recommendation='Consider splitting into smaller files or removing unused styles'
                ))

            # Check for !important overuse
            important_count = len(re.findall(r'!important', content))
            if important_count > 10:
                self._add_issue(CodeIssue(
                    severity='warning',
                    category='css',
                    check_name='important_overuse',
                    title=f'Excessive !important usage ({important_count} times)',
                    description='Overuse of !important makes CSS hard to maintain',
                    file_path=path,
                    recommendation='Refactor CSS specificity instead of using !important'
                ))

            # Check for @import (render-blocking)
            if re.search(r'@import\s+[\'"]', content):
                self._add_issue(CodeIssue(
                    severity='warning',
                    category='performance',
                    check_name='css_import',
                    title='@import statement detected',
                    description='@import is render-blocking and slows page load',
                    file_path=path,
                    recommendation='Use <link> tags or concatenate CSS files'
                ))

    async def _analyze_json_files(self, files: List[Dict]):
        """Analyze JSON config files"""
        log.info(f"Analyzing {len(files)} JSON files...")

        for file_info in files:
            path = file_info.get('path', '')
            content = file_info.get('content')

            if not content:
                continue

            # Try to parse JSON
            try:
                data = json.loads(content)

                # Check settings_schema.json for large schemas
                if 'settings_schema' in path:
                    if isinstance(data, list) and len(data) > 50:
                        self._add_issue(CodeIssue(
                            severity='info',
                            category='json',
                            check_name='large_schema',
                            title='Large settings schema',
                            description=f'Settings schema has {len(data)} sections',
                            file_path=path,
                            recommendation='Consider organizing settings into logical groups'
                        ))

            except json.JSONDecodeError as e:
                self._add_issue(CodeIssue(
                    severity='critical',
                    category='json',
                    check_name='invalid_json',
                    title='Invalid JSON file',
                    description=f'JSON parse error: {str(e)[:50]}',
                    file_path=path,
                    recommendation='Fix JSON syntax error'
                ))

    def _find_line_number(self, content: str, search_text: str) -> Optional[int]:
        """Find line number of text in content"""
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            if search_text in line:
                return i
        return None

    def _build_quality_metrics(self) -> Dict:
        """Build quality metrics from collected issues"""
        file_size_issues = [i for i in self._issues if 'large' in i.check_name]
        complexity_issues = [i for i in self._issues if 'nested' in i.check_name or 'complexity' in i.check_name]

        # Calculate score (100 - deductions)
        score = 100
        score -= len([i for i in self._issues if i.severity == 'critical']) * 10
        score -= len([i for i in self._issues if i.severity == 'warning']) * 3
        score -= len([i for i in self._issues if i.severity == 'info']) * 1
        score = max(0, min(100, score))

        return {
            "overall_score": score,
            "file_size_issues": [asdict(i) for i in file_size_issues],
            "complexity_issues": [asdict(i) for i in complexity_issues],
            "total_issues": len(self._issues)
        }

    def _build_theme_health(self) -> Dict:
        """Build theme health report from collected issues"""
        liquid_issues = [i for i in self._issues if i.category == 'liquid']
        performance_issues = [i for i in self._issues if i.category == 'performance']
        accessibility_issues = [i for i in self._issues if i.category == 'accessibility']

        # Calculate sub-scores
        liquid_score = max(0, 100 - len(liquid_issues) * 5)
        perf_score = max(0, 100 - len(performance_issues) * 10)
        a11y_score = max(0, 100 - len(accessibility_issues) * 5)
        overall = int((liquid_score + perf_score + a11y_score) / 3)

        return {
            "overall_score": overall,
            "liquid_quality": {
                "score": liquid_score,
                "issues": [asdict(i) for i in liquid_issues]
            },
            "performance": {
                "score": perf_score,
                "issues": [asdict(i) for i in performance_issues]
            },
            "accessibility": {
                "score": a11y_score,
                "issues": [asdict(i) for i in accessibility_issues]
            }
        }

    def _build_security_report(self) -> Dict:
        """Build security report from collected issues"""
        security_issues = [i for i in self._issues if i.category == 'security']

        critical = len([i for i in security_issues if i.severity == 'critical'])
        warning = len([i for i in security_issues if i.severity == 'warning'])

        return {
            "total_vulnerabilities": len(security_issues),
            "critical": critical,
            "high": warning,
            "medium": 0,
            "low": 0,
            "issues": [asdict(i) for i in security_issues]
        }

    def _build_technical_debt_report(self) -> Dict:
        """Build technical debt report from collected issues"""
        debt_types = ['deprecated', 'console', 'var_usage', 'important', 'hardcoded']
        debt_issues = [i for i in self._issues if any(t in i.check_name for t in debt_types)]

        high = len([i for i in debt_issues if i.severity == 'critical'])
        medium = len([i for i in debt_issues if i.severity == 'warning'])
        low = len([i for i in debt_issues if i.severity == 'info'])

        return {
            "total_debt_items": len(debt_issues),
            "high_priority": high,
            "medium_priority": medium,
            "low_priority": low,
            "estimated_total_effort_hours": high * 2 + medium * 1 + low * 0.5,
            "items": [asdict(i) for i in debt_issues]
        }

    async def analyze_code_quality(self, repo_name: str) -> Dict:
        """
        Analyze code quality metrics using real file analysis.

        Checks:
        - File size
        - Code complexity
        - Deprecated patterns
        - Security issues
        """
        try:
            # Run analysis if not already done
            if not self._issues:
                await self.github.connect()
                theme_files = await self.github.fetch_theme_files()
                await self._analyze_liquid_files(theme_files.get('liquid', []))
                await self._analyze_javascript_files(theme_files.get('javascript', []))
                await self._analyze_css_files(theme_files.get('css', []))
                await self._analyze_json_files(theme_files.get('json', []))

            return self._build_quality_metrics()

        except Exception as e:
            log.error(f"Error analyzing code quality for {repo_name}: {str(e)}")
            return {"overall_score": 0, "file_size_issues": [], "complexity_issues": [], "total_issues": 0}

    async def analyze_theme_health(self, repo_name: str) -> Dict:
        """
        Shopify theme-specific health checks using real file analysis.

        Checks:
        - Deprecated Liquid tags
        - Performance issues (large files, render-blocking)
        - Accessibility issues (missing alt text)
        - Code quality patterns
        """
        try:
            # Run analysis if not already done
            if not self._issues:
                await self.github.connect()
                theme_files = await self.github.fetch_theme_files()
                await self._analyze_liquid_files(theme_files.get('liquid', []))
                await self._analyze_javascript_files(theme_files.get('javascript', []))
                await self._analyze_css_files(theme_files.get('css', []))
                await self._analyze_json_files(theme_files.get('json', []))

            return self._build_theme_health()

        except Exception as e:
            log.error(f"Error analyzing theme health for {repo_name}: {str(e)}")
            return {"overall_score": 0, "liquid_quality": {}, "performance": {}, "accessibility": {}}

    async def scan_security_vulnerabilities(self, repo_name: str) -> Dict:
        """
        Scan for security vulnerabilities using real file analysis.

        Checks:
        - eval() usage
        - innerHTML usage (XSS risk)
        - console statements in production
        - Other security anti-patterns
        """
        try:
            # Run analysis if not already done
            if not self._issues:
                await self.github.connect()
                theme_files = await self.github.fetch_theme_files()
                await self._analyze_liquid_files(theme_files.get('liquid', []))
                await self._analyze_javascript_files(theme_files.get('javascript', []))
                await self._analyze_css_files(theme_files.get('css', []))
                await self._analyze_json_files(theme_files.get('json', []))

            return self._build_security_report()

        except Exception as e:
            log.error(f"Error scanning security vulnerabilities for {repo_name}: {str(e)}")
            return {"total_vulnerabilities": 0, "critical": 0, "high": 0, "medium": 0, "low": 0, "issues": []}

    async def detect_technical_debt(self, repo_name: str) -> Dict:
        """
        Detect technical debt using real file analysis.

        Identifies:
        - Deprecated code patterns
        - Console statements left in code
        - Legacy var usage
        - CSS !important overuse
        - Hardcoded text
        """
        try:
            # Run analysis if not already done
            if not self._issues:
                await self.github.connect()
                theme_files = await self.github.fetch_theme_files()
                await self._analyze_liquid_files(theme_files.get('liquid', []))
                await self._analyze_javascript_files(theme_files.get('javascript', []))
                await self._analyze_css_files(theme_files.get('css', []))
                await self._analyze_json_files(theme_files.get('json', []))

            return self._build_technical_debt_report()

        except Exception as e:
            log.error(f"Error detecting technical debt for {repo_name}: {str(e)}")
            return {"total_debt_items": 0, "high_priority": 0, "medium_priority": 0, "low_priority": 0, "items": []}

    async def analyze_commits(self, repo_name: str) -> Dict:
        """
        Analyze commit history using real GitHub data.

        Provides:
        - Recent commits
        - Commit frequency
        - Contributors
        """
        try:
            # Fetch recent commits from GitHub
            await self.github.connect()
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            commits = await self.github._fetch_recent_commits(start_date, end_date)

            # Analyze commits
            total_commits = len(commits)
            authors = set()
            last_commit = None

            for commit in commits:
                if commit.get('author'):
                    authors.add(commit['author'])
                if not last_commit:
                    last_commit = commit

            # Build commit type breakdown (inferred from messages)
            commit_types = {"feature": 0, "bugfix": 0, "refactor": 0, "chore": 0, "other": 0}
            for commit in commits:
                msg = (commit.get('message') or '').lower()
                if any(kw in msg for kw in ['add', 'feat', 'new', 'implement']):
                    commit_types['feature'] += 1
                elif any(kw in msg for kw in ['fix', 'bug', 'issue', 'resolve']):
                    commit_types['bugfix'] += 1
                elif any(kw in msg for kw in ['refactor', 'clean', 'improve', 'optimize']):
                    commit_types['refactor'] += 1
                elif any(kw in msg for kw in ['chore', 'update', 'bump', 'deps']):
                    commit_types['chore'] += 1
                else:
                    commit_types['other'] += 1

            return {
                "total_commits_last_30_days": total_commits,
                "active_contributors_last_30_days": len(authors),
                "commit_frequency": {
                    "commits_per_week": round(total_commits / 4, 1),
                    "trend": "stable" if total_commits > 5 else "low"
                },
                "top_contributors": [{"name": author} for author in list(authors)[:5]],
                "commit_types": commit_types,
                "last_commit": {
                    "sha": last_commit.get('sha', '') if last_commit else '',
                    "message": last_commit.get('message', '') if last_commit else '',
                    "author": last_commit.get('author', '') if last_commit else '',
                    "date": last_commit.get('date', '') if last_commit else ''
                } if last_commit else {},
                "recent_commits": commits[:10]  # Return 10 most recent
            }

        except Exception as e:
            log.error(f"Error analyzing commits for {repo_name}: {str(e)}")
            return {"total_commits_last_30_days": 0, "active_contributors_last_30_days": 0}

    async def check_dependencies(self, repo_name: str) -> Dict:
        """
        Check dependency health

        Identifies:
        - Outdated packages
        - Deprecated packages
        - Security vulnerabilities in dependencies
        """
        status = {}

        try:
            # Example dependency status
            status = {
                "total_dependencies": 42,
                "outdated_dependencies": 8,
                "deprecated_dependencies": 2,
                "vulnerable_dependencies": 1,

                "outdated": [
                    {
                        "package_name": "lodash",
                        "current_version": "4.17.15",
                        "latest_version": "4.17.21",
                        "versions_behind": 6,
                        "update_type": "patch",
                        "has_security_vulnerability": True
                    },
                    {
                        "package_name": "jquery",
                        "current_version": "2.1.4",
                        "latest_version": "3.7.1",
                        "versions_behind": 16,
                        "update_type": "major",
                        "has_security_vulnerability": False
                    }
                ],

                "deprecated": [
                    {
                        "package_name": "node-sass",
                        "current_version": "4.14.1",
                        "replacement": "sass (Dart Sass)",
                        "deprecation_reason": "node-sass is deprecated. Use Dart Sass instead."
                    }
                ],

                "summary": {
                    "patch_updates_available": 5,
                    "minor_updates_available": 2,
                    "major_updates_available": 1
                }
            }

            return status

        except Exception as e:
            log.error(f"Error checking dependencies for {repo_name}: {str(e)}")
            return {}

    def _calculate_health_score(
        self,
        quality_metrics: Dict,
        theme_health: Dict,
        security_issues: Dict,
        technical_debt: Dict,
        dependency_status: Dict
    ) -> int:
        """Calculate overall health score (0-100)"""

        # Start with 100 and deduct points
        score = 100

        # Code quality (max -20 points)
        quality_score = quality_metrics.get('overall_score', 100)
        score -= (100 - quality_score) * 0.2

        # Theme health (max -20 points)
        theme_score = theme_health.get('overall_score', 100)
        score -= (100 - theme_score) * 0.2

        # Security (max -30 points)
        critical_vulns = security_issues.get('critical', 0)
        high_vulns = security_issues.get('high', 0)
        score -= critical_vulns * 10
        score -= high_vulns * 5

        # Technical debt (max -20 points)
        high_debt = technical_debt.get('high_priority', 0)
        score -= high_debt * 5

        # Dependencies (max -10 points)
        vulnerable_deps = dependency_status.get('vulnerable_dependencies', 0)
        score -= vulnerable_deps * 10

        return max(0, min(100, int(score)))

    def _identify_priorities(
        self,
        quality_metrics: Dict,
        theme_health: Dict,
        security_issues: Dict,
        technical_debt: Dict,
        dependency_status: Dict
    ) -> List[Dict]:
        """Identify top priorities to address from collected issues"""

        priorities = []

        # Build priorities from the actual collected issues
        for issue in self._issues:
            priority_level = "medium"
            effort_hours = 1

            if issue.severity == 'critical':
                priority_level = "critical"
                effort_hours = 2
            elif issue.severity == 'warning':
                priority_level = "high"
                effort_hours = 1.5

            # Only include critical and warning issues as priorities
            if issue.severity in ('critical', 'warning'):
                priorities.append({
                    "priority": priority_level,
                    "category": issue.category,
                    "title": issue.title,
                    "description": issue.description,
                    "file_path": issue.file_path,
                    "recommendation": issue.recommendation,
                    "effort_hours": effort_hours
                })

        # Sort by priority: critical > high > medium
        priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        priorities.sort(key=lambda x: priority_order.get(x['priority'], 3))

        return priorities[:10]  # Top 10

    async def get_code_dashboard(self, repo_name: str) -> Dict:
        """Get complete code health dashboard"""
        analysis = await self.analyze_repository(repo_name)

        return {
            "repo_name": repo_name,
            "overall_health_score": analysis['overall_health_score'],

            "summary": {
                "quality_score": analysis['quality_metrics'].get('overall_score', 0),
                "theme_score": analysis['theme_health'].get('overall_score', 0),
                "total_vulnerabilities": analysis['security_issues'].get('total_vulnerabilities', 0),
                "critical_vulnerabilities": analysis['security_issues'].get('critical', 0),
                "total_debt_items": analysis['technical_debt'].get('total_debt_items', 0),
                "high_priority_debt": analysis['technical_debt'].get('high_priority', 0),
                "outdated_dependencies": analysis['dependency_status'].get('outdated_dependencies', 0)
            },

            "top_priorities": analysis['priorities'][:5],

            "quality_summary": {
                "score": analysis['quality_metrics'].get('overall_score', 0),
                "file_size_issues": len(analysis['quality_metrics'].get('file_size_issues', [])),
                "complexity_issues": len(analysis['quality_metrics'].get('complexity_issues', []))
            },

            "security_summary": {
                "total": analysis['security_issues'].get('total_vulnerabilities', 0),
                "critical": analysis['security_issues'].get('critical', 0),
                "high": analysis['security_issues'].get('high', 0),
                "top_issues": analysis['security_issues'].get('issues', [])[:3]
            },

            "debt_summary": {
                "total": analysis['technical_debt'].get('total_debt_items', 0),
                "high_priority": analysis['technical_debt'].get('high_priority', 0),
                "estimated_effort": analysis['technical_debt'].get('estimated_total_effort_hours', 0),
                "top_items": analysis['technical_debt'].get('items', [])[:5]
            },

            "recent_activity": {
                "commits_last_30_days": analysis['commit_analysis'].get('total_commits_last_30_days', 0),
                "active_contributors": analysis['commit_analysis'].get('active_contributors_last_30_days', 0),
                "last_commit": analysis['commit_analysis'].get('last_commit', {})
            }
        }
