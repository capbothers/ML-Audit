"""
GitHub data connector (lightweight version)
Fetches repository info, commits, files, and PRs for Shopify theme tracking
"""
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import requests
import base64
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class GitHubConnector(BaseConnector):
    """Connector for GitHub API - Shopify theme repository"""

    # Critical Shopify theme files to track
    CRITICAL_FILES = [
        'layout/theme.liquid',
        'templates/product.liquid',
        'templates/cart.liquid',
        'templates/collection.liquid',
        'templates/index.liquid',
        'sections/header.liquid',
        'sections/footer.liquid',
        'sections/product-template.liquid',
        'assets/theme.js',
        'config/settings_schema.json',
    ]

    def __init__(self):
        super().__init__("GitHub")
        self.access_token = settings.github_access_token
        self.repo_owner = settings.github_repo_owner
        self.repo_name = settings.github_repo_name
        self.repo = f"{self.repo_owner}/{self.repo_name}" if self.repo_owner and self.repo_name else None
        self.base_url = "https://api.github.com"
        self.headers = {
            'Authorization': f'token {self.access_token}',
            'Accept': 'application/vnd.github.v3+json'
        } if self.access_token else {}

    async def connect(self) -> bool:
        """Establish connection to GitHub"""
        try:
            if not self.access_token or not self.repo:
                log.error("Missing GitHub credentials")
                return False

            response = requests.get(
                f"{self.base_url}/repos/{self.repo}",
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                log.info(f"Connected to GitHub: {self.repo}")
                return True
            else:
                log.error(f"GitHub connection failed: {response.status_code}")
                return False
        except Exception as e:
            log.error(f"Failed to connect to GitHub: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        """Validate GitHub connection"""
        try:
            if not self.access_token or not self.repo:
                return False

            response = requests.get(
                f"{self.base_url}/repos/{self.repo}",
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            log.error(f"GitHub connection validation failed: {str(e)}")
            return False

    async def fetch_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch comprehensive GitHub data"""
        data = {
            "repository": await self._fetch_repository_info(),
            "recent_commits": await self._fetch_recent_commits(start_date, end_date),
            "branches": await self._fetch_branches(),
            "pull_requests": await self._fetch_pull_requests(),
            "critical_files": await self._fetch_critical_files(),
        }
        return data

    async def _fetch_repository_info(self) -> Dict:
        """Fetch repository information"""
        try:
            response = requests.get(
                f"{self.base_url}/repos/{self.repo}",
                headers=self.headers
            )

            if response.status_code != 200:
                return {}

            data = response.json()

            # Get languages
            lang_response = requests.get(
                f"{self.base_url}/repos/{self.repo}/languages",
                headers=self.headers
            )
            languages = lang_response.json() if lang_response.status_code == 200 else {}

            log.info(f"Fetched GitHub repository info: {self.repo}")
            return {
                "name": data.get('name'),
                "full_name": data.get('full_name'),
                "description": data.get('description'),
                "default_branch": data.get('default_branch'),
                "is_private": data.get('private', False),
                "created_at": data.get('created_at'),
                "updated_at": data.get('updated_at'),
                "pushed_at": data.get('pushed_at'),
                "size_kb": data.get('size', 0),
                "open_issues": data.get('open_issues_count', 0),
                "primary_language": data.get('language'),
                "languages": languages,
            }
        except Exception as e:
            log.error(f"Error fetching GitHub repository info: {str(e)}")
            return {}

    async def _fetch_recent_commits(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch recent commits"""
        try:
            params = {
                'since': start_date.isoformat(),
                'until': end_date.isoformat(),
                'per_page': 50
            }

            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/commits",
                headers=self.headers,
                params=params
            )

            if response.status_code != 200:
                return []

            commits_data = response.json()
            commits = []

            for commit in commits_data:
                commit_info = commit.get('commit', {})
                commits.append({
                    "sha": commit.get('sha', '')[:7],  # Short SHA
                    "message": commit_info.get('message', '').split('\n')[0][:100],  # First line, truncated
                    "author": commit_info.get('author', {}).get('name'),
                    "date": commit_info.get('author', {}).get('date'),
                    "url": commit.get('html_url'),
                })

            log.info(f"Fetched {len(commits)} recent commits from GitHub")
            return commits

        except Exception as e:
            log.error(f"Error fetching GitHub commits: {str(e)}")
            return []

    async def _fetch_branches(self) -> List[Dict]:
        """Fetch branches"""
        try:
            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/branches",
                headers=self.headers,
                params={'per_page': 30}
            )

            if response.status_code != 200:
                return []

            branches_data = response.json()
            branches = []

            for branch in branches_data:
                branches.append({
                    "name": branch.get('name'),
                    "protected": branch.get('protected', False),
                    "commit_sha": branch.get('commit', {}).get('sha', '')[:7],
                })

            log.info(f"Fetched {len(branches)} branches from GitHub")
            return branches

        except Exception as e:
            log.error(f"Error fetching GitHub branches: {str(e)}")
            return []

    async def _fetch_pull_requests(self) -> Dict:
        """Fetch pull requests summary"""
        try:
            # Get open PRs
            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/pulls",
                headers=self.headers,
                params={'state': 'open', 'per_page': 20}
            )

            open_prs = []
            if response.status_code == 200:
                for pr in response.json():
                    open_prs.append({
                        "number": pr.get('number'),
                        "title": pr.get('title')[:80],
                        "author": pr.get('user', {}).get('login'),
                        "created_at": pr.get('created_at'),
                        "head_branch": pr.get('head', {}).get('ref'),
                        "base_branch": pr.get('base', {}).get('ref'),
                        "url": pr.get('html_url'),
                    })

            # Get recently merged PRs
            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/pulls",
                headers=self.headers,
                params={'state': 'closed', 'sort': 'updated', 'direction': 'desc', 'per_page': 10}
            )

            recent_merged = []
            if response.status_code == 200:
                for pr in response.json():
                    if pr.get('merged_at'):
                        recent_merged.append({
                            "number": pr.get('number'),
                            "title": pr.get('title')[:80],
                            "merged_at": pr.get('merged_at'),
                            "merged_by": pr.get('merged_by', {}).get('login') if pr.get('merged_by') else None,
                        })

            log.info(f"Fetched {len(open_prs)} open PRs, {len(recent_merged)} recently merged")
            return {
                "open": open_prs,
                "recently_merged": recent_merged,
                "open_count": len(open_prs),
            }

        except Exception as e:
            log.error(f"Error fetching GitHub pull requests: {str(e)}")
            return {"open": [], "recently_merged": [], "open_count": 0}

    async def _fetch_critical_files(self) -> List[Dict]:
        """Fetch info about critical Shopify theme files"""
        try:
            files = []

            for file_path in self.CRITICAL_FILES:
                response = requests.get(
                    f"{self.base_url}/repos/{self.repo}/contents/{file_path}",
                    headers=self.headers
                )

                if response.status_code == 200:
                    data = response.json()

                    # Get last commit for this file
                    commit_response = requests.get(
                        f"{self.base_url}/repos/{self.repo}/commits",
                        headers=self.headers,
                        params={'path': file_path, 'per_page': 1}
                    )

                    last_modified = None
                    last_modified_by = None
                    if commit_response.status_code == 200:
                        commits = commit_response.json()
                        if commits:
                            commit_info = commits[0].get('commit', {})
                            last_modified = commit_info.get('author', {}).get('date')
                            last_modified_by = commit_info.get('author', {}).get('name')

                    files.append({
                        "path": file_path,
                        "size_bytes": data.get('size', 0),
                        "sha": data.get('sha', '')[:7],
                        "last_modified": last_modified,
                        "last_modified_by": last_modified_by,
                        "exists": True,
                    })
                elif response.status_code == 404:
                    files.append({
                        "path": file_path,
                        "exists": False,
                    })

            found_count = sum(1 for f in files if f.get('exists', False))
            log.info(f"Checked {len(self.CRITICAL_FILES)} critical files, {found_count} found")
            return files

        except Exception as e:
            log.error(f"Error fetching GitHub critical files: {str(e)}")
            return []

    async def fetch_file_content(self, file_path: str) -> Optional[str]:
        """Fetch content of a specific file"""
        try:
            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/contents/{file_path}",
                headers=self.headers
            )

            if response.status_code == 200:
                data = response.json()
                if 'content' in data:
                    content = base64.b64decode(data['content']).decode('utf-8')
                    return content
            return None
        except Exception as e:
            log.error(f"Error fetching file content {file_path}: {str(e)}")
            return None

    async def search_code(self, query: str) -> List[Dict]:
        """Search for code in the repository"""
        try:
            response = requests.get(
                f"{self.base_url}/search/code",
                headers=self.headers,
                params={
                    'q': f'{query} repo:{self.repo}',
                    'per_page': 20
                }
            )

            if response.status_code != 200:
                return []

            results = []
            for item in response.json().get('items', []):
                results.append({
                    "path": item.get('path'),
                    "name": item.get('name'),
                    "url": item.get('html_url'),
                })

            return results

        except Exception as e:
            log.error(f"Error searching GitHub code: {str(e)}")
            return []

    async def fetch_quick(self) -> Dict[str, Any]:
        """Quick fetch - just repo info and recent commits (no file contents)"""
        data = {
            "repository": await self._fetch_repository_info(),
            "recent_commits": await self._fetch_recent_commits(
                datetime.now() - timedelta(days=7),
                datetime.now()
            ),
            "pull_requests": await self._fetch_pull_requests(),
        }
        return data

    async def fetch_theme_files(self) -> Dict[str, List[Dict]]:
        """
        Fetch all theme files for code health analysis.
        Returns files grouped by type with their content.
        """
        log.info("Fetching Shopify theme files for analysis...")

        files = {
            'liquid': [],      # .liquid templates
            'javascript': [],  # .js files
            'css': [],         # .css/.scss files
            'json': [],        # config/*.json, locales/*.json
        }

        try:
            # Fetch file tree
            tree = await self._fetch_repo_tree()
            if not tree:
                return files

            # Filter files by type
            liquid_files = [f for f in tree if f.get('path', '').endswith('.liquid')]
            js_files = [f for f in tree if f.get('path', '').endswith('.js')]
            css_files = [f for f in tree if f.get('path', '').endswith(('.css', '.scss'))]
            json_files = [f for f in tree if f.get('path', '').endswith('.json')
                         and ('config/' in f.get('path', '') or 'locales/' in f.get('path', ''))]

            # Fetch content for each file type (with limits to avoid rate limiting)
            files['liquid'] = await self._fetch_files_with_content(liquid_files[:100])
            files['javascript'] = await self._fetch_files_with_content(js_files[:20])
            files['css'] = await self._fetch_files_with_content(css_files[:20])
            files['json'] = await self._fetch_files_with_content(json_files[:30])

            total = sum(len(v) for v in files.values())
            log.info(f"Fetched {total} theme files for analysis")

            return files

        except Exception as e:
            log.error(f"Error fetching theme files: {str(e)}")
            return files

    async def _fetch_repo_tree(self) -> List[Dict]:
        """Fetch the full repository tree"""
        try:
            # Get default branch
            repo_info = await self._fetch_repository_info()
            branch = repo_info.get('default_branch', 'main')

            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/git/trees/{branch}",
                headers=self.headers,
                params={'recursive': '1'}
            )

            if response.status_code != 200:
                log.error(f"Failed to fetch repo tree: {response.status_code}")
                return []

            tree = response.json().get('tree', [])
            # Only return files (not directories)
            return [item for item in tree if item.get('type') == 'blob']

        except Exception as e:
            log.error(f"Error fetching repo tree: {str(e)}")
            return []

    async def _fetch_files_with_content(self, files: List[Dict]) -> List[Dict]:
        """Fetch content for a list of files"""
        results = []

        for file_info in files:
            path = file_info.get('path', '')
            size = file_info.get('size', 0)

            # Skip very large files (over 500KB) to avoid issues
            if size > 500000:
                results.append({
                    'path': path,
                    'size_bytes': size,
                    'content': None,
                    'too_large': True
                })
                continue

            content = await self.fetch_file_content(path)
            results.append({
                'path': path,
                'size_bytes': size,
                'content': content,
                'too_large': False
            })

        return results
