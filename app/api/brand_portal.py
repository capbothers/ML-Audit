"""
Brand Portal API — Supplier-facing pricing intelligence endpoints.

All endpoints require a `brand` query parameter. No cost, margin, or
internal pricing policy data is ever returned.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.brand_portal_service import BrandPortalService
from app.utils.cache import get_cached, set_cached, _MISS

router = APIRouter(prefix="/brand-portal", tags=["brand-portal"])


@router.get("/brands")
async def list_brands(db: Session = Depends(get_db)):
    """List all brands with SKU counts."""
    cache_key = "brand_portal_brands"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached
    result = BrandPortalService(db).get_brands()
    set_cached(cache_key, result, 300)
    return result


@router.get("/overview")
async def brand_overview(
    brand: str = Query(..., description="Brand/vendor name"),
    db: Session = Depends(get_db),
):
    """Brand overview: KPIs, category breakdown, competitor activity, monthly trends."""
    if not brand:
        raise HTTPException(400, "brand is required")
    cache_key = f"brand_portal_overview|{brand.upper()}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached
    result = BrandPortalService(db).get_brand_overview(brand)
    set_cached(cache_key, result, 300)
    return result


@router.get("/products")
async def brand_products(
    brand: str = Query(..., description="Brand/vendor name"),
    category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search SKU or title"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Paginated product list with pricing for one brand."""
    if not brand:
        raise HTTPException(400, "brand is required")
    cache_key = f"brand_portal_products|{brand.upper()}|{category}|{search}|{page}|{per_page}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached
    result = BrandPortalService(db).get_brand_products(brand, category, search, page, per_page)
    set_cached(cache_key, result, 60)
    return result


@router.get("/sku")
async def sku_detail(
    brand: str = Query(..., description="Brand/vendor name"),
    sku: str = Query(..., description="Product SKU"),
    days: int = Query(90, ge=7, le=365),
    db: Session = Depends(get_db),
):
    """SKU detail: competitor map + price history. No cost/margin data."""
    if not brand or not sku:
        raise HTTPException(400, "brand and sku are required")
    cache_key = f"brand_portal_sku|{brand.upper()}|{sku.upper()}|{days}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached
    result = BrandPortalService(db).get_sku_detail(brand, sku, days)
    if result is None:
        raise HTTPException(404, "SKU not found or does not belong to this brand")
    set_cached(cache_key, result, 60)
    return result


@router.get("/search")
async def search_skus(
    brand: str = Query(..., description="Brand/vendor name"),
    query: str = Query(..., min_length=1, description="Search term"),
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Search for SKUs within a brand."""
    if not brand:
        raise HTTPException(400, "brand is required")
    return BrandPortalService(db).search_skus(brand, query, limit)
