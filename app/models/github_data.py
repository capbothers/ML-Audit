"""
GitHub Data Models

Stores repository info, commits, and file health data.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class GitHubRepository(Base):
    """GitHub repository information"""
    __tablename__ = "github_repositories"

    id = Column(Integer, primary_key=True, index=True)

    # Repository identification
    repo_full_name = Column(String, unique=True, index=True, nullable=False)
    # Format: owner/repo_name

    repo_owner = Column(String, nullable=False)
    repo_name = Column(String, nullable=False)

    # Repository details
    default_branch = Column(String, default="main", nullable=False)
    description = Column(Text, nullable=True)
    is_private = Column(Boolean, default=False)

    # Stats
    stars = Column(Integer, default=0)
    forks = Column(Integer, default=0)
    watchers = Column(Integer, default=0)
    open_issues = Column(Integer, default=0)

    # Size
    size_kb = Column(Integer, default=0)
    # Repository size in KB

    # Activity
    last_push = Column(DateTime, nullable=True, index=True)
    last_commit_sha = Column(String, nullable=True)

    # Languages
    primary_language = Column(String, nullable=True)
    languages = Column(JSON, nullable=True)
    # e.g., {"JavaScript": 45, "Liquid": 30, "CSS": 25}

    # Metadata
    created_at_github = Column(DateTime, nullable=True)
    updated_at_github = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GitHubRepository {self.repo_full_name}>"


class GitHubCommit(Base):
    """GitHub commit data"""
    __tablename__ = "github_commits"

    id = Column(Integer, primary_key=True, index=True)

    # Commit identification
    commit_sha = Column(String, unique=True, index=True, nullable=False)
    repo_full_name = Column(String, index=True, nullable=False)

    # Commit details
    commit_message = Column(Text, nullable=True)
    author_name = Column(String, nullable=True)
    author_email = Column(String, nullable=True)
    author_date = Column(DateTime, index=True, nullable=False)

    committer_name = Column(String, nullable=True)
    committer_email = Column(String, nullable=True)
    committer_date = Column(DateTime, nullable=True)

    # Parent commits
    parent_shas = Column(JSON, nullable=True)
    # List of parent commit SHAs

    # Branch
    branch = Column(String, nullable=True)

    # Stats
    files_changed = Column(Integer, default=0)
    additions = Column(Integer, default=0)
    deletions = Column(Integer, default=0)
    total_changes = Column(Integer, default=0)

    # Changed files
    changed_files = Column(JSON, nullable=True)
    # List of file paths that were changed

    # Commit type (inferred from message)
    commit_type = Column(String, nullable=True)
    # Types: feature, fix, refactor, docs, chore, etc.

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GitHubCommit {self.commit_sha[:7]} by {self.author_name}>"


class GitHubFile(Base):
    """GitHub file tracking (for theme files)"""
    __tablename__ = "github_files"

    id = Column(Integer, primary_key=True, index=True)

    # File identification
    repo_full_name = Column(String, index=True, nullable=False)
    file_path = Column(String, index=True, nullable=False)
    # Relative path from repo root

    # File type
    file_type = Column(String, nullable=True)
    # Types: liquid_template, liquid_section, liquid_snippet, javascript, css, json

    # File size
    size_bytes = Column(Integer, default=0)

    # File contents (for critical files)
    content = Column(Text, nullable=True)
    # Store contents for analysis

    # Hash
    sha = Column(String, nullable=True)
    # Git blob SHA

    # Last modification
    last_modified = Column(DateTime, nullable=True, index=True)
    last_modified_by = Column(String, nullable=True)
    last_commit_sha = Column(String, nullable=True)

    # File health
    is_critical_file = Column(Boolean, default=False)
    # Mark critical theme files (checkout, cart, product template)

    complexity_score = Column(Integer, nullable=True)
    # Code complexity score

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<GitHubFile {self.file_path}>"


class GitHubPullRequest(Base):
    """GitHub pull request data"""
    __tablename__ = "github_pull_requests"

    id = Column(Integer, primary_key=True, index=True)

    # PR identification
    pr_number = Column(Integer, index=True, nullable=False)
    repo_full_name = Column(String, index=True, nullable=False)

    # PR details
    title = Column(String, nullable=False)
    body = Column(Text, nullable=True)
    state = Column(String, index=True, nullable=False)
    # States: open, closed, merged

    # Author
    author_username = Column(String, nullable=True)

    # Branches
    head_branch = Column(String, nullable=True)
    base_branch = Column(String, nullable=True)

    # Stats
    additions = Column(Integer, default=0)
    deletions = Column(Integer, default=0)
    changed_files = Column(Integer, default=0)
    commits = Column(Integer, default=0)

    # Review
    reviewers = Column(JSON, nullable=True)
    # List of reviewer usernames

    approved = Column(Boolean, default=False)
    changes_requested = Column(Boolean, default=False)

    # Timing
    created_at_github = Column(DateTime, nullable=True)
    updated_at_github = Column(DateTime, nullable=True)
    merged_at = Column(DateTime, nullable=True)
    closed_at = Column(DateTime, nullable=True)

    # Merge details
    merged_by = Column(String, nullable=True)
    merge_commit_sha = Column(String, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GitHubPullRequest #{self.pr_number} - {self.title}>"


class GitHubIssue(Base):
    """GitHub issue tracking"""
    __tablename__ = "github_issues"

    id = Column(Integer, primary_key=True, index=True)

    # Issue identification
    issue_number = Column(Integer, index=True, nullable=False)
    repo_full_name = Column(String, index=True, nullable=False)

    # Issue details
    title = Column(String, nullable=False)
    body = Column(Text, nullable=True)
    state = Column(String, index=True, nullable=False)
    # States: open, closed

    # Labels
    labels = Column(JSON, nullable=True)
    # List of label names

    # Author/Assignees
    author_username = Column(String, nullable=True)
    assignees = Column(JSON, nullable=True)
    # List of assignee usernames

    # Milestone
    milestone = Column(String, nullable=True)

    # Timing
    created_at_github = Column(DateTime, nullable=True)
    updated_at_github = Column(DateTime, nullable=True)
    closed_at = Column(DateTime, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GitHubIssue #{self.issue_number} - {self.title}>"
