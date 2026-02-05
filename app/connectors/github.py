"""
GitHub Connector

Syncs repository commits, file tracking, pull requests, and issues from GitHub API.
Used for tracking Shopify theme changes and code health.
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import desc
import time
import requests
import base64

from app.connectors.base import BaseConnector
from app.models.github_data import (
    GitHubRepository, GitHubCommit, GitHubFile,
    GitHubPullRequest, GitHubIssue
)
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class GitHubConnector(BaseConnector):
    """
    GitHub API connector

    Syncs:
    - Repository metadata
    - Commit history (for deployment frequency tracking)
    - File contents (for critical theme files)
    - Pull requests (for code review tracking)
    - Issues (optional)
    """

    # Critical theme files to track
    CRITICAL_FILES = [
        'templates/product.liquid',
        'templates/cart.liquid',
        'templates/collection.liquid',
        'sections/product-template.liquid',
        'sections/cart-template.liquid',
        'assets/checkout.js',
        'assets/cart.js',
        'layout/theme.liquid'
    ]

    def __init__(self, db: Session):
        super().__init__(db, source_name="github", source_type="code_repository")
        self.access_token = settings.github_access_token
        self.repo = settings.github_repo  # Format: "owner/repo-name"
        self.base_url = "https://api.github.com"
        self.headers = {
            'Authorization': f'token {self.access_token}',
            'Accept': 'application/vnd.github.v3+json'
        }

    async def authenticate(self) -> bool:
        """
        Authenticate with GitHub API

        Returns:
            True if authentication successful
        """
        try:
            if not self.access_token or not self.repo:
                log.error("Missing GitHub credentials in settings")
                return False

            # Test authentication by getting repo info
            response = requests.get(
                f"{self.base_url}/repos/{self.repo}",
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                self._authenticated = True
                log.info(f"GitHub authentication successful for {self.repo}")
                return True
            else:
                log.error(f"GitHub authentication failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            log.error(f"GitHub authentication failed: {str(e)}")
            self._authenticated = False
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync data from GitHub

        Args:
            start_date: Start date for sync (defaults to last sync or 1 year ago)
            end_date: End date for sync (defaults to now)

        Returns:
            Dict with sync results
        """
        sync_start_time = time.time()

        try:
            # Authenticate if needed
            if not self._authenticated:
                if not await self.authenticate():
                    raise Exception("Authentication failed")

            # Log sync start
            await self.log_sync_start()

            # Determine date range
            if not end_date:
                end_date = datetime.now()

            if not start_date:
                # Get last successful sync
                last_sync = await self.get_last_successful_sync()
                if last_sync:
                    start_date = last_sync
                else:
                    # First sync: get 1 year of data
                    start_date = datetime.now() - timedelta(days=365)

            log.info(f"Syncing GitHub data from {start_date.date()} to {end_date.date()}")

            total_records = 0

            # Sync repository info
            repo_synced = await self._sync_repository()
            total_records += repo_synced

            # Sync commits
            commits_synced = await self._sync_commits(start_date, end_date)
            total_records += commits_synced

            # Sync critical files
            files_synced = await self._sync_critical_files()
            total_records += files_synced

            # Sync pull requests
            prs_synced = await self._sync_pull_requests(start_date, end_date)
            total_records += prs_synced

            # Sync issues (optional)
            issues_synced = await self._sync_issues()
            total_records += issues_synced

            # Calculate sync duration
            sync_duration = time.time() - sync_start_time

            # Log success
            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=sync_duration
            )

            log.info(f"GitHub sync completed: {total_records} records in {sync_duration:.1f}s")

            return {
                "success": True,
                "records_synced": total_records,
                "repository": repo_synced,
                "commits": commits_synced,
                "files": files_synced,
                "pull_requests": prs_synced,
                "issues": issues_synced,
                "duration_seconds": sync_duration
            }

        except Exception as e:
            error_msg = f"GitHub sync failed: {str(e)}"
            log.error(error_msg)
            await self.log_sync_failure(error_msg)

            return {
                "success": False,
                "error": error_msg,
                "records_synced": 0
            }

    async def _sync_repository(self) -> int:
        """Sync repository metadata"""
        try:
            response = requests.get(
                f"{self.base_url}/repos/{self.repo}",
                headers=self.headers
            )

            if response.status_code != 200:
                log.error(f"Failed to get repository info: {response.status_code}")
                return 0

            data = response.json()

            # Parse dates
            created_at = None
            updated_at = None
            last_push = None

            if 'created_at' in data:
                created_at = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
            if 'updated_at' in data:
                updated_at = datetime.fromisoformat(data['updated_at'].replace('Z', '+00:00'))
            if 'pushed_at' in data:
                last_push = datetime.fromisoformat(data['pushed_at'].replace('Z', '+00:00'))

            # Get languages
            languages_response = requests.get(
                f"{self.base_url}/repos/{self.repo}/languages",
                headers=self.headers
            )

            languages = {}
            if languages_response.status_code == 200:
                languages = languages_response.json()

            # Create or update repository record
            repo_owner, repo_name = self.repo.split('/')

            record = GitHubRepository(
                repo_full_name=self.repo,
                repo_owner=repo_owner,
                repo_name=repo_name,
                default_branch=data.get('default_branch', 'main'),
                description=data.get('description'),
                is_private=data.get('private', False),
                stars=data.get('stargazers_count', 0),
                forks=data.get('forks_count', 0),
                watchers=data.get('watchers_count', 0),
                open_issues=data.get('open_issues_count', 0),
                size_kb=data.get('size', 0),
                last_push=last_push,
                last_commit_sha=None,  # Will be updated by commit sync
                primary_language=data.get('language'),
                languages=languages if languages else None,
                created_at_github=created_at,
                updated_at_github=updated_at
            )

            self.db.merge(record)
            self.db.commit()

            log.info(f"Synced GitHub repository: {self.repo}")
            return 1

        except Exception as e:
            log.error(f"Error syncing GitHub repository: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_commits(self, start_date: datetime, end_date: datetime) -> int:
        """Sync commit history"""
        try:
            # Get commits since start_date
            params = {
                'since': start_date.isoformat(),
                'per_page': 100
            }

            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/commits",
                headers=self.headers,
                params=params
            )

            if response.status_code != 200:
                log.error(f"Failed to get commits: {response.status_code}")
                return 0

            commits = response.json()
            records_synced = 0

            for commit_data in commits:
                commit_sha = commit_data['sha']

                # Get detailed commit info
                detail_response = requests.get(
                    f"{self.base_url}/repos/{self.repo}/commits/{commit_sha}",
                    headers=self.headers
                )

                if detail_response.status_code != 200:
                    continue

                detail = detail_response.json()
                commit_info = detail.get('commit', {})

                # Parse author date
                author_date = None
                if 'author' in commit_info and 'date' in commit_info['author']:
                    author_date = datetime.fromisoformat(commit_info['author']['date'].replace('Z', '+00:00'))

                # Parse committer date
                committer_date = None
                if 'committer' in commit_info and 'date' in commit_info['committer']:
                    committer_date = datetime.fromisoformat(commit_info['committer']['date'].replace('Z', '+00:00'))

                # Get parent commit SHAs
                parent_shas = [parent['sha'] for parent in commit_data.get('parents', [])]

                # Get file stats
                files_changed = len(detail.get('files', []))
                additions = detail.get('stats', {}).get('additions', 0)
                deletions = detail.get('stats', {}).get('deletions', 0)

                # Get changed files
                changed_files = [file['filename'] for file in detail.get('files', [])]

                # Infer commit type from message
                commit_message = commit_info.get('message', '')
                commit_type = self._infer_commit_type(commit_message)

                # Create record
                record = GitHubCommit(
                    commit_sha=commit_sha,
                    repo_full_name=self.repo,
                    commit_message=commit_message,
                    author_name=commit_info.get('author', {}).get('name'),
                    author_email=commit_info.get('author', {}).get('email'),
                    author_date=author_date,
                    committer_name=commit_info.get('committer', {}).get('name'),
                    committer_email=commit_info.get('committer', {}).get('email'),
                    committer_date=committer_date,
                    parent_shas=parent_shas if parent_shas else None,
                    branch=None,  # Would need additional API call
                    files_changed=files_changed,
                    additions=additions,
                    deletions=deletions,
                    total_changes=additions + deletions,
                    changed_files=changed_files if changed_files else None,
                    commit_type=commit_type
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 50 == 0:
                    self.db.commit()
                    time.sleep(0.1)  # Rate limit

            self.db.commit()

            log.info(f"Synced {records_synced} GitHub commits")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GitHub commits: {str(e)}")
            self.db.rollback()
            return 0

    def _infer_commit_type(self, message: str) -> str:
        """Infer commit type from commit message"""
        message_lower = message.lower()

        if message_lower.startswith('feat') or 'feature' in message_lower:
            return 'FEATURE'
        elif message_lower.startswith('fix') or 'bug' in message_lower:
            return 'FIX'
        elif message_lower.startswith('refactor') or 'refactor' in message_lower:
            return 'REFACTOR'
        elif message_lower.startswith('docs') or 'documentation' in message_lower:
            return 'DOCS'
        elif message_lower.startswith('test') or 'test' in message_lower:
            return 'TEST'
        elif message_lower.startswith('chore'):
            return 'CHORE'
        elif message_lower.startswith('style'):
            return 'STYLE'
        else:
            return 'OTHER'

    async def _sync_critical_files(self) -> int:
        """Sync contents of critical theme files"""
        try:
            records_synced = 0

            for file_path in self.CRITICAL_FILES:
                # Get file contents
                response = requests.get(
                    f"{self.base_url}/repos/{self.repo}/contents/{file_path}",
                    headers=self.headers
                )

                if response.status_code == 200:
                    data = response.json()

                    # Decode content (base64 encoded)
                    content = None
                    if 'content' in data:
                        try:
                            content = base64.b64decode(data['content']).decode('utf-8')
                        except:
                            log.warning(f"Could not decode content for {file_path}")

                    # Determine file type
                    file_type = self._determine_file_type(file_path)

                    # Get last commit for this file
                    commits_response = requests.get(
                        f"{self.base_url}/repos/{self.repo}/commits",
                        headers=self.headers,
                        params={'path': file_path, 'per_page': 1}
                    )

                    last_modified = None
                    last_modified_by = None
                    last_commit_sha = None

                    if commits_response.status_code == 200:
                        commits = commits_response.json()
                        if commits:
                            last_commit = commits[0]
                            last_commit_sha = last_commit['sha']
                            commit_data = last_commit.get('commit', {})
                            if 'author' in commit_data:
                                last_modified_by = commit_data['author'].get('name')
                                if 'date' in commit_data['author']:
                                    last_modified = datetime.fromisoformat(
                                        commit_data['author']['date'].replace('Z', '+00:00')
                                    )

                    # Calculate complexity score (simple metric based on lines)
                    complexity_score = len(content.splitlines()) if content else 0

                    record = GitHubFile(
                        repo_full_name=self.repo,
                        file_path=file_path,
                        file_type=file_type,
                        size_bytes=data.get('size', 0),
                        content=content,
                        sha=data.get('sha'),
                        last_modified=last_modified,
                        last_modified_by=last_modified_by,
                        last_commit_sha=last_commit_sha,
                        is_critical_file=True,
                        complexity_score=complexity_score
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 10 == 0:
                        self.db.commit()
                        time.sleep(0.1)

                elif response.status_code == 404:
                    log.info(f"Critical file not found: {file_path}")
                else:
                    log.warning(f"Could not fetch {file_path}: {response.status_code}")

            self.db.commit()

            log.info(f"Synced {records_synced} GitHub critical files")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GitHub files: {str(e)}")
            self.db.rollback()
            return 0

    def _determine_file_type(self, file_path: str) -> str:
        """Determine file type from path"""
        if file_path.endswith('.liquid'):
            if '/templates/' in file_path:
                return 'LIQUID_TEMPLATE'
            elif '/sections/' in file_path:
                return 'LIQUID_SECTION'
            elif '/snippets/' in file_path:
                return 'LIQUID_SNIPPET'
            else:
                return 'LIQUID'
        elif file_path.endswith('.js'):
            return 'JAVASCRIPT'
        elif file_path.endswith('.css') or file_path.endswith('.scss'):
            return 'CSS'
        elif file_path.endswith('.json'):
            return 'JSON'
        else:
            return 'OTHER'

    async def _sync_pull_requests(self, start_date: datetime, end_date: datetime) -> int:
        """Sync pull request data"""
        try:
            # Get all PRs (open and recently closed)
            params = {
                'state': 'all',
                'sort': 'updated',
                'direction': 'desc',
                'per_page': 100
            }

            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/pulls",
                headers=self.headers,
                params=params
            )

            if response.status_code != 200:
                log.error(f"Failed to get pull requests: {response.status_code}")
                return 0

            prs = response.json()
            records_synced = 0

            for pr in prs:
                pr_number = pr['number']

                # Parse dates
                created_at = datetime.fromisoformat(pr['created_at'].replace('Z', '+00:00'))
                updated_at = datetime.fromisoformat(pr['updated_at'].replace('Z', '+00:00'))

                merged_at = None
                if pr.get('merged_at'):
                    merged_at = datetime.fromisoformat(pr['merged_at'].replace('Z', '+00:00'))

                closed_at = None
                if pr.get('closed_at'):
                    closed_at = datetime.fromisoformat(pr['closed_at'].replace('Z', '+00:00'))

                # Determine state
                state = 'MERGED' if pr.get('merged_at') else pr['state'].upper()

                record = GitHubPullRequest(
                    pr_number=pr_number,
                    repo_full_name=self.repo,
                    title=pr['title'],
                    body=pr.get('body'),
                    state=state,
                    author_username=pr['user']['login'] if pr.get('user') else None,
                    head_branch=pr['head']['ref'] if pr.get('head') else None,
                    base_branch=pr['base']['ref'] if pr.get('base') else None,
                    additions=0,  # Would need PR details API
                    deletions=0,
                    changed_files=0,
                    commits=0,
                    reviewers=None,
                    approved=False,
                    changes_requested=False,
                    created_at_github=created_at,
                    updated_at_github=updated_at,
                    merged_at=merged_at,
                    closed_at=closed_at,
                    merged_by=pr['merged_by']['login'] if pr.get('merged_by') else None,
                    merge_commit_sha=pr.get('merge_commit_sha')
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 50 == 0:
                    self.db.commit()
                    time.sleep(0.1)

            self.db.commit()

            log.info(f"Synced {records_synced} GitHub pull requests")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GitHub pull requests: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_issues(self) -> int:
        """Sync issue data (optional)"""
        try:
            # Get open issues only
            params = {
                'state': 'open',
                'per_page': 100
            }

            response = requests.get(
                f"{self.base_url}/repos/{self.repo}/issues",
                headers=self.headers,
                params=params
            )

            if response.status_code != 200:
                log.error(f"Failed to get issues: {response.status_code}")
                return 0

            issues = response.json()
            records_synced = 0

            for issue in issues:
                # Skip pull requests (they appear in issues endpoint)
                if 'pull_request' in issue:
                    continue

                issue_number = issue['number']

                # Parse dates
                created_at = datetime.fromisoformat(issue['created_at'].replace('Z', '+00:00'))
                updated_at = datetime.fromisoformat(issue['updated_at'].replace('Z', '+00:00'))

                closed_at = None
                if issue.get('closed_at'):
                    closed_at = datetime.fromisoformat(issue['closed_at'].replace('Z', '+00:00'))

                # Extract labels
                labels = [label['name'] for label in issue.get('labels', [])]

                # Extract assignees
                assignees = [assignee['login'] for assignee in issue.get('assignees', [])]

                record = GitHubIssue(
                    issue_number=issue_number,
                    repo_full_name=self.repo,
                    title=issue['title'],
                    body=issue.get('body'),
                    state=issue['state'].upper(),
                    labels=labels if labels else None,
                    author_username=issue['user']['login'] if issue.get('user') else None,
                    assignees=assignees if assignees else None,
                    milestone=issue['milestone']['title'] if issue.get('milestone') else None,
                    created_at_github=created_at,
                    updated_at_github=updated_at,
                    closed_at=closed_at
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 50 == 0:
                    self.db.commit()
                    time.sleep(0.1)

            self.db.commit()

            log.info(f"Synced {records_synced} GitHub issues")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GitHub issues: {str(e)}")
            self.db.rollback()
            return 0

    async def get_latest_data_timestamp(self) -> Optional[datetime]:
        """Get timestamp of most recent commit"""
        try:
            latest = self.db.query(GitHubCommit).filter(
                GitHubCommit.repo_full_name == self.repo
            ).order_by(desc(GitHubCommit.author_date)).first()

            if latest and latest.author_date:
                return latest.author_date

            return None

        except Exception as e:
            log.error(f"Error getting latest GitHub timestamp: {str(e)}")
            return None
