"""
Code & Theme Health Models (GitHub Integration)

Monitors Shopify theme code quality, technical debt, and security.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class CodeRepository(Base):
    """GitHub repository tracking"""
    __tablename__ = "code_repositories"

    id = Column(Integer, primary_key=True, index=True)

    # Repository details
    repo_name = Column(String, unique=True, index=True, nullable=False)
    repo_owner = Column(String, nullable=False)
    repo_url = Column(String, nullable=True)
    default_branch = Column(String, default="main", nullable=False)

    # Repository type
    repo_type = Column(String, index=True, nullable=False)
    # Types: shopify_theme, app, backend, frontend

    # Language breakdown
    primary_language = Column(String, nullable=True)
    languages = Column(JSON, nullable=True)
    # e.g., {"Liquid": 45, "JavaScript": 30, "CSS": 25}

    # Repository stats
    total_files = Column(Integer, default=0)
    total_lines_of_code = Column(Integer, default=0)
    total_commits = Column(Integer, default=0)
    contributors_count = Column(Integer, default=0)

    # Activity
    last_commit_date = Column(DateTime, nullable=True, index=True)
    last_commit_author = Column(String, nullable=True)
    last_commit_message = Column(Text, nullable=True)

    # Health score
    overall_health_score = Column(Integer, default=0, index=True)
    # Score 0-100

    # Tracking
    is_active = Column(Boolean, default=True)
    last_analyzed = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<CodeRepository {self.repo_owner}/{self.repo_name}>"


class CodeQualityMetric(Base):
    """Code quality metrics from analysis"""
    __tablename__ = "code_quality_metrics"

    id = Column(Integer, primary_key=True, index=True)

    # Repository reference
    repo_id = Column(Integer, ForeignKey("code_repositories.id"), nullable=False)
    repo_name = Column(String, index=True, nullable=False)

    # Metric type
    metric_category = Column(String, index=True, nullable=False)
    # Categories: complexity, maintainability, performance, security, best_practices

    metric_name = Column(String, index=True, nullable=False)
    # e.g., "cyclomatic_complexity", "code_duplication", "file_size"

    # Metric value
    metric_value = Column(Float, nullable=True)
    metric_unit = Column(String, nullable=True)
    # e.g., "percentage", "count", "lines", "KB"

    # Thresholds
    threshold_warning = Column(Float, nullable=True)
    threshold_critical = Column(Float, nullable=True)

    # Status
    status = Column(String, index=True, nullable=False)
    # Status: good, warning, critical

    # Context
    file_path = Column(String, nullable=True)
    # Specific file if metric is file-level

    line_number = Column(Integer, nullable=True)
    # Specific line if applicable

    details = Column(JSON, nullable=True)
    # Additional metric details

    # Metadata
    analysis_date = Column(Date, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<CodeQualityMetric {self.metric_name} = {self.metric_value} ({self.status})>"


class ThemeHealthCheck(Base):
    """Shopify theme-specific health checks"""
    __tablename__ = "theme_health_checks"

    id = Column(Integer, primary_key=True, index=True)

    # Repository reference
    repo_id = Column(Integer, ForeignKey("code_repositories.id"), nullable=False)
    repo_name = Column(String, index=True, nullable=False)

    # Check type
    check_category = Column(String, index=True, nullable=False)
    # Categories: liquid_quality, performance, accessibility, seo, security

    check_name = Column(String, index=True, nullable=False)
    # e.g., "deprecated_liquid_tags", "large_bundle_size", "missing_alt_text"

    # Check result
    status = Column(String, index=True, nullable=False)
    # Status: pass, warning, fail

    severity = Column(String, index=True, nullable=False)
    # Severity: low, medium, high, critical

    # Issue details
    issue_count = Column(Integer, default=0)
    affected_files = Column(JSON, nullable=True)
    # List of files with issues

    description = Column(Text, nullable=True)
    recommendation = Column(Text, nullable=True)

    # Impact
    performance_impact = Column(String, nullable=True)
    # Impact: none, low, medium, high

    user_impact = Column(String, nullable=True)
    # Impact: none, low, medium, high

    # Examples
    examples = Column(JSON, nullable=True)
    # Example issues with file/line references

    # Metadata
    analysis_date = Column(Date, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<ThemeHealthCheck {self.check_name} - {self.status}>"


class SecurityVulnerability(Base):
    """Detected security vulnerabilities"""
    __tablename__ = "security_vulnerabilities"

    id = Column(Integer, primary_key=True, index=True)

    # Repository reference
    repo_id = Column(Integer, ForeignKey("code_repositories.id"), nullable=False)
    repo_name = Column(String, index=True, nullable=False)

    # Vulnerability details
    vulnerability_type = Column(String, index=True, nullable=False)
    # Types: dependency_vulnerability, xss, csrf, sql_injection, exposed_secrets

    vulnerability_id = Column(String, nullable=True)
    # CVE ID or vendor-specific ID

    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)

    # Severity
    severity = Column(String, index=True, nullable=False)
    # Severity: low, medium, high, critical

    cvss_score = Column(Float, nullable=True)
    # CVSS 3.0 score (0-10)

    # Location
    package_name = Column(String, nullable=True)
    # If dependency vulnerability

    package_version = Column(String, nullable=True)
    fixed_version = Column(String, nullable=True)

    file_path = Column(String, nullable=True)
    # If code vulnerability

    line_number = Column(Integer, nullable=True)

    # Remediation
    recommendation = Column(Text, nullable=True)
    patch_available = Column(Boolean, default=False)

    # Status
    status = Column(String, default="open", index=True)
    # Status: open, acknowledged, fixed, false_positive

    # Metadata
    detected_date = Column(Date, index=True, nullable=False)
    fixed_date = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<SecurityVulnerability {self.title} - {self.severity}>"


class CodeCommit(Base):
    """Git commit history and analysis"""
    __tablename__ = "code_commits"

    id = Column(Integer, primary_key=True, index=True)

    # Repository reference
    repo_id = Column(Integer, ForeignKey("code_repositories.id"), nullable=False)
    repo_name = Column(String, index=True, nullable=False)

    # Commit details
    commit_sha = Column(String, unique=True, index=True, nullable=False)
    commit_message = Column(Text, nullable=True)
    author_name = Column(String, nullable=True)
    author_email = Column(String, nullable=True)
    committed_date = Column(DateTime, index=True, nullable=False)

    # Commit size
    files_changed = Column(Integer, default=0)
    lines_added = Column(Integer, default=0)
    lines_deleted = Column(Integer, default=0)
    lines_changed = Column(Integer, default=0)

    # Commit type (inferred from message)
    commit_type = Column(String, index=True, nullable=True)
    # Types: feature, bugfix, refactor, docs, test, chore, hotfix

    # Impact analysis
    impact_score = Column(Float, nullable=True)
    # Score based on lines changed, files affected

    is_breaking_change = Column(Boolean, default=False)
    # Detected from commit message or file changes

    # Code churn
    is_high_churn = Column(Boolean, default=False, index=True)
    # Files changed frequently might indicate instability

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<CodeCommit {self.commit_sha[:7]} by {self.author_name}>"


class TechnicalDebt(Base):
    """Identified technical debt items"""
    __tablename__ = "technical_debt"

    id = Column(Integer, primary_key=True, index=True)

    # Repository reference
    repo_id = Column(Integer, ForeignKey("code_repositories.id"), nullable=False)
    repo_name = Column(String, index=True, nullable=False)

    # Debt type
    debt_type = Column(String, index=True, nullable=False)
    # Types: outdated_dependency, deprecated_code, code_duplication,
    # large_file, complex_function, missing_tests, todo_comment, hack

    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)

    # Location
    file_path = Column(String, nullable=True, index=True)
    line_number = Column(Integer, nullable=True)
    code_snippet = Column(Text, nullable=True)

    # Severity and effort
    severity = Column(String, index=True, nullable=False)
    # Severity: low, medium, high

    estimated_effort_hours = Column(Float, nullable=True)
    # Estimated hours to fix

    # Impact
    business_impact = Column(String, nullable=True)
    # Impact: none, low, medium, high

    technical_impact = Column(String, nullable=True)
    # Impact: maintainability, performance, security, reliability

    # Recommendation
    recommendation = Column(Text, nullable=True)
    priority = Column(String, index=True, nullable=True)
    # Priority: low, medium, high, critical

    # Status
    status = Column(String, default="identified", index=True)
    # Status: identified, acknowledged, in_progress, resolved, wont_fix

    # Metadata
    detected_date = Column(Date, index=True, nullable=False)
    resolved_date = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<TechnicalDebt {self.debt_type} - {self.severity}>"


class CodeInsight(Base):
    """LLM-generated code health insights"""
    __tablename__ = "code_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Repository reference
    repo_id = Column(Integer, ForeignKey("code_repositories.id"), nullable=True)
    repo_name = Column(String, index=True, nullable=True)

    # Insight type
    insight_type = Column(String, index=True, nullable=False)
    # Types: health_summary, security_analysis, refactoring_recommendations,
    # performance_analysis, best_practices

    # LLM-generated content
    executive_summary = Column(Text, nullable=True)
    detailed_analysis = Column(Text, nullable=True)

    top_priorities = Column(JSON, nullable=True)
    # Top issues to address

    recommendations = Column(JSON, nullable=True)
    # Specific recommendations with code examples

    # Metrics
    total_issues_found = Column(Integer, default=0)
    critical_issues = Column(Integer, default=0)
    overall_health_score = Column(Integer, nullable=True)
    # Score 0-100

    # Metadata
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    llm_model = Column(String, nullable=True)

    def __repr__(self):
        return f"<CodeInsight {self.insight_type} for {self.repo_name}>"


class DependencyStatus(Base):
    """Package/dependency health tracking"""
    __tablename__ = "dependency_status"

    id = Column(Integer, primary_key=True, index=True)

    # Repository reference
    repo_id = Column(Integer, ForeignKey("code_repositories.id"), nullable=False)
    repo_name = Column(String, index=True, nullable=False)

    # Dependency details
    package_name = Column(String, index=True, nullable=False)
    current_version = Column(String, nullable=False)
    latest_version = Column(String, nullable=True)

    # Package manager
    package_manager = Column(String, nullable=True)
    # e.g., npm, yarn, pip, gem

    # Status
    is_outdated = Column(Boolean, default=False, index=True)
    is_deprecated = Column(Boolean, default=False, index=True)
    has_security_vulnerability = Column(Boolean, default=False, index=True)

    # Version comparison
    versions_behind = Column(Integer, default=0)
    # How many versions behind

    update_type = Column(String, nullable=True)
    # Types: patch, minor, major

    # Metadata
    last_checked = Column(Date, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<DependencyStatus {self.package_name} {self.current_version}>"
