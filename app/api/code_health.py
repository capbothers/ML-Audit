"""
Code & Theme Health API (GitHub Integration)

Endpoints for analyzing Shopify theme code quality and health.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.code_health_service import CodeHealthService
from app.services.llm_service import LLMService
from app.utils.logger import log
from app.utils.cache import get_cached, set_cached, _MISS

router = APIRouter(prefix="/code", tags=["code"])


@router.get("/dashboard")
async def get_code_dashboard(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    db: Session = Depends(get_db)
):
    """
    Complete code health dashboard

    Returns:
    - Overall health score
    - Summary of issues by category
    - Top priorities to address
    - Recent activity
    """
    try:
        service = CodeHealthService(db)
        dashboard = await service.get_code_dashboard(repo_name)

        return {
            "success": True,
            "data": dashboard
        }

    except Exception as e:
        log.error(f"Error generating code health dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analyze")
async def analyze_repository(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    db: Session = Depends(get_db)
):
    """
    Complete repository analysis

    Returns full analysis including:
    - Code quality metrics
    - Theme health (if Shopify theme)
    - Security vulnerabilities
    - Technical debt
    - Commit analysis
    - Dependency status
    """
    try:
        service = CodeHealthService(db)
        analysis = await service.analyze_repository(repo_name)

        return {
            "success": True,
            "data": analysis
        }

    except Exception as e:
        log.error(f"Error analyzing repository: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quality")
async def get_code_quality(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    db: Session = Depends(get_db)
):
    """
    Code quality metrics

    Shows:
    - File size issues
    - Code complexity
    - Code duplication
    - Comment ratio
    - Naming convention violations
    """
    try:
        service = CodeHealthService(db)
        quality = await service.analyze_code_quality(repo_name)

        return {
            "success": True,
            "data": quality
        }

    except Exception as e:
        log.error(f"Error getting code quality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/theme-health")
async def get_theme_health(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    db: Session = Depends(get_db)
):
    """
    Shopify theme health checks

    Shows:
    - Liquid quality (deprecated tags, complexity)
    - Performance (bundle size, image optimization)
    - Accessibility (alt text, color contrast)
    - SEO (meta descriptions, structured data)
    - Required files
    """
    try:
        service = CodeHealthService(db)
        health = await service.analyze_theme_health(repo_name)

        return {
            "success": True,
            "data": health
        }

    except Exception as e:
        log.error(f"Error getting theme health: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/security")
async def get_security_vulnerabilities(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    severity: Optional[str] = Query(None, description="Filter by severity: critical, high, medium, low"),
    status: Optional[str] = Query(None, description="Filter by status: open, fixed, acknowledged"),
    db: Session = Depends(get_db)
):
    """
    Security vulnerability scan

    Shows:
    - Dependency vulnerabilities (CVEs)
    - XSS vulnerabilities in Liquid
    - Exposed secrets/API keys
    - Insecure configurations
    """
    try:
        service = CodeHealthService(db)
        vulnerabilities = await service.scan_security_vulnerabilities(repo_name)

        # Apply filters
        issues = vulnerabilities.get('issues', [])

        if severity:
            issues = [i for i in issues if i['severity'] == severity]

        if status:
            issues = [i for i in issues if i.get('status') == status]

        vulnerabilities['issues'] = issues
        vulnerabilities['filtered_count'] = len(issues)

        return {
            "success": True,
            "data": vulnerabilities
        }

    except Exception as e:
        log.error(f"Error getting security vulnerabilities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/technical-debt")
async def get_technical_debt(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    debt_type: Optional[str] = Query(None, description="Filter by debt type"),
    severity: Optional[str] = Query(None, description="Filter by severity: high, medium, low"),
    db: Session = Depends(get_db)
):
    """
    Technical debt analysis

    Shows:
    - Outdated dependencies
    - Deprecated code
    - Code duplication
    - Large/complex files
    - Missing tests
    - TODO comments
    """
    try:
        service = CodeHealthService(db)
        debt = await service.detect_technical_debt(repo_name)

        # Apply filters
        items = debt.get('items', [])

        if debt_type:
            items = [i for i in items if i['debt_type'] == debt_type]

        if severity:
            items = [i for i in items if i['severity'] == severity]

        debt['items'] = items
        debt['filtered_count'] = len(items)

        return {
            "success": True,
            "data": debt
        }

    except Exception as e:
        log.error(f"Error getting technical debt: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/commits")
async def get_commit_analysis(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Commit history analysis

    Shows:
    - Commit frequency
    - Code churn (files changed frequently)
    - Contributor activity
    - Commit types (feature, bugfix, etc.)
    - Breaking changes
    """
    try:
        service = CodeHealthService(db)
        analysis = await service.analyze_commits(repo_name)

        return {
            "success": True,
            "data": analysis
        }

    except Exception as e:
        log.error(f"Error getting commit analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dependencies")
async def get_dependency_status(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    show_outdated_only: bool = Query(False, description="Show only outdated dependencies"),
    db: Session = Depends(get_db)
):
    """
    Dependency health check

    Shows:
    - Outdated packages
    - Deprecated packages
    - Security vulnerabilities in dependencies
    - Available updates (patch, minor, major)
    """
    try:
        service = CodeHealthService(db)
        status = await service.check_dependencies(repo_name)

        # Filter if requested
        if show_outdated_only:
            status['dependencies'] = status.get('outdated', [])
        else:
            status['dependencies'] = status.get('outdated', []) + status.get('deprecated', [])

        return {
            "success": True,
            "data": status
        }

    except Exception as e:
        log.error(f"Error getting dependency status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/priorities")
async def get_priorities(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    category: Optional[str] = Query(None, description="Filter by category: security, technical_debt, performance"),
    db: Session = Depends(get_db)
):
    """
    Top priorities to address

    Shows highest-priority issues sorted by:
    1. Critical security vulnerabilities
    2. High-severity technical debt
    3. Performance issues
    """
    try:
        service = CodeHealthService(db)
        analysis = await service.analyze_repository(repo_name)

        priorities = analysis['priorities']

        # Filter by category if specified
        if category:
            priorities = [p for p in priorities if p['category'] == category]

        return {
            "success": True,
            "data": {
                "priorities": priorities,
                "total_count": len(priorities)
            }
        }

    except Exception as e:
        log.error(f"Error getting priorities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/issues")
async def get_code_issues(
    severity: Optional[str] = Query(None, description="Filter by severity: critical, warning, info"),
    category: Optional[str] = Query(None, description="Filter by category: liquid, javascript, css, json, security, performance, accessibility"),
    db: Session = Depends(get_db)
):
    """
    Get all code health issues found during analysis.

    This endpoint performs real analysis on the Shopify theme files and returns
    actual issues found, including:
    - Deprecated {% include %} tags (should use {% render %})
    - Missing alt text on images
    - Large file sizes affecting performance
    - console.log statements in JavaScript
    - Security issues (eval usage, innerHTML)
    - CSS anti-patterns (!important overuse)
    - Invalid JSON files

    Each issue includes:
    - severity: critical, warning, or info
    - category: liquid, javascript, css, json, security, performance, accessibility
    - file_path: where the issue was found
    - line_number: specific line (when available)
    - recommendation: how to fix it
    """
    try:
        # Cache all issues for 1 hour (GitHub fetch is slow ~50s)
        cache_key = "code_issues_all"
        all_issues = get_cached(cache_key)
        if all_issues is _MISS:
            service = CodeHealthService(db)
            all_issues = await service.get_all_issues()
            set_cached(cache_key, all_issues, seconds=3600)

        issues = all_issues
        if severity:
            issues = [i for i in issues if i['severity'] == severity]
        if category:
            issues = [i for i in issues if i['category'] == category]

        critical_count = len([i for i in issues if i['severity'] == 'critical'])
        warning_count = len([i for i in issues if i['severity'] == 'warning'])
        info_count = len([i for i in issues if i['severity'] == 'info'])

        return {
            "success": True,
            "data": {
                "issues": issues,
                "total_count": len(issues),
                "summary": {
                    "critical": critical_count,
                    "warning": warning_count,
                    "info": info_count
                },
                "filters_applied": {
                    "severity": severity,
                    "category": category
                }
            }
        }

    except Exception as e:
        log.error(f"Error getting code issues: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_code_insights(
    repo_name: str = Query(..., description="GitHub repository name (owner/repo)"),
    db: Session = Depends(get_db)
):
    """
    AI-powered code health insights

    Uses Claude to analyze code health and provide strategic recommendations:
    - Critical security issues to fix
    - Technical debt priorities
    - Performance optimizations
    - Best practice improvements
    - Refactoring recommendations
    """
    try:
        # Get code analysis
        code_service = CodeHealthService(db)
        analysis = await code_service.analyze_repository(repo_name)

        # Generate LLM insights
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": analysis
            }

        llm_analysis = llm_service.analyze_code_health(
            repo_name=repo_name,
            overall_health_score=analysis['overall_health_score'],
            quality_metrics=analysis['quality_metrics'],
            theme_health=analysis['theme_health'],
            security_issues=analysis['security_issues'],
            technical_debt=analysis['technical_debt'],
            commit_analysis=analysis['commit_analysis'],
            dependency_status=analysis['dependency_status'],
            priorities=analysis['priorities']
        )

        return {
            "success": True,
            "data": {
                "analysis": analysis,
                "llm_insights": llm_analysis
            }
        }

    except Exception as e:
        log.error(f"Error generating LLM code insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
