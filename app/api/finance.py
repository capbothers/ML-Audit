"""
Finance Dashboard API

Monthly P&L, expense management, and overhead allocation endpoints.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query, File, UploadFile, Body
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.models.base import get_db
from app.services.finance_service import FinanceService
from app.services.expense_import_service import ExpenseImportService
from app.api.data_quality import get_stale_data_warning
from app.utils.logger import log
from app.utils.cache import get_cached, set_cached, _MISS

router = APIRouter(prefix="/finance", tags=["finance"])


class ExpenseCreate(BaseModel):
    month: str  # YYYY-MM format
    category: str
    description: str
    amount: float
    is_recurring: bool = True
    notes: Optional[str] = None


@router.get("/pl")
async def get_pl(
    months: int = Query(6, description="Number of months to return"),
    db: Session = Depends(get_db)
):
    """Get monthly P&L for the last N months."""
    cached = get_cached(f"finance_pl|{months}")
    if cached is not _MISS:
        return cached

    service = FinanceService(db)
    data = service.get_pl_summary(months)

    result = {
        "success": True,
        "data": {
            "months": data,
            "count": len(data),
        }
    }
    set_cached(f"finance_pl|{months}", result, 300)
    return result


@router.get("/pl/{month_str}")
async def get_pl_month(
    month_str: str,
    db: Session = Depends(get_db)
):
    """
    Get P&L for a specific month.

    Month format: YYYY-MM (e.g., 2026-01)
    """
    try:
        month = datetime.strptime(month_str, '%Y-%m').date()
    except ValueError:
        return {"success": False, "error": f"Invalid month format: {month_str}. Use YYYY-MM."}

    service = FinanceService(db)
    data = service.get_pl_for_month(month)

    if not data:
        return {
            "success": True,
            "data": None,
            "message": f"No P&L data for {month_str}. Run POST /finance/pl/calculate?month={month_str} first."
        }

    return {"success": True, "data": data}


@router.post("/pl/calculate")
async def calculate_pl(
    month: str = Query(..., description="Month to calculate (YYYY-MM format)"),
    db: Session = Depends(get_db)
):
    """
    Calculate (or recalculate) P&L for a month.

    Pulls revenue from Shopify, COGS from order items, ad spend from Google Ads,
    and expenses from business_expenses table.
    """
    try:
        month_date = datetime.strptime(month, '%Y-%m').date()
    except ValueError:
        return {"success": False, "error": f"Invalid month format: {month}. Use YYYY-MM."}

    service = FinanceService(db)
    result = service.calculate_monthly_pl(month_date)

    return {
        "success": True,
        "data": result,
        "message": f"P&L calculated for {month}"
    }


@router.post("/pl/calculate-all")
async def calculate_all_pl(
    months: int = Query(6, description="Number of months back to calculate"),
    db: Session = Depends(get_db)
):
    """Calculate P&L for the last N months."""
    service = FinanceService(db)
    results = []

    today = date.today()
    for i in range(months):
        m = today.month - i
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        month_date = date(y, m, 1)
        result = service.calculate_monthly_pl(month_date)
        results.append(result)

    return {
        "success": True,
        "data": {
            "months_calculated": len(results),
            "months": list(reversed(results)),
        }
    }


@router.get("/expenses")
async def get_expenses(
    month: Optional[str] = Query(None, description="Filter by month (YYYY-MM)"),
    category: Optional[str] = Query(None, description="Filter by category"),
    db: Session = Depends(get_db)
):
    """List expenses, optionally filtered by month and category."""
    month_date = None
    if month:
        try:
            month_date = datetime.strptime(month, '%Y-%m').date()
            month_date = date(month_date.year, month_date.month, 1)
        except ValueError:
            return {"success": False, "error": f"Invalid month format: {month}. Use YYYY-MM."}

    service = ExpenseImportService(db)
    expenses = service.get_expenses(month=month_date, category=category)

    return {
        "success": True,
        "data": {
            "expenses": expenses,
            "count": len(expenses),
        }
    }


@router.post("/expenses/upload")
async def upload_expenses(
    file: UploadFile = File(..., description="CSV file with expenses"),
    db: Session = Depends(get_db)
):
    """
    Upload expenses from a CSV file.

    Expected CSV format:
    ```
    month,category,description,amount
    2026-01,payroll,Staff wages,45000
    2026-01,rent,Warehouse,8500
    2026-01,shipping,Fulfillment costs,12000
    ```

    Flexible column names are supported (e.g., 'date' instead of 'month').
    Categories are auto-normalized (e.g., 'wages' -> 'payroll').
    Re-uploading updates existing entries (upsert by month+category+description).
    """
    content = await file.read()
    csv_text = content.decode('utf-8-sig')  # Handle BOM from Excel exports

    service = ExpenseImportService(db)
    result = service.import_csv(csv_text)

    return {
        "success": result.get("success", False),
        "data": result,
    }


@router.post("/expenses")
async def add_expense(
    expense: ExpenseCreate,
    db: Session = Depends(get_db)
):
    """Add a single expense manually."""
    try:
        month_date = datetime.strptime(expense.month, '%Y-%m').date()
        month_date = date(month_date.year, month_date.month, 1)
    except ValueError:
        return {"success": False, "error": f"Invalid month format: {expense.month}. Use YYYY-MM."}

    service = ExpenseImportService(db)
    result = service.add_expense(
        month=month_date,
        category=expense.category,
        description=expense.description,
        amount=Decimal(str(expense.amount)),
        is_recurring=expense.is_recurring,
        notes=expense.notes,
    )

    return {"success": True, "data": result}


@router.get("/overhead")
async def get_overhead_trend(
    months: int = Query(6, description="Number of months"),
    db: Session = Depends(get_db)
):
    """Get overhead per order trend over time."""
    cached = get_cached(f"finance_overhead|{months}")
    if cached is not _MISS:
        return cached

    service = FinanceService(db)
    data = service.get_overhead_trend(months)

    result = {
        "success": True,
        "data": {
            "trend": data,
            "count": len(data),
        }
    }
    set_cached(f"finance_overhead|{months}", result, 300)
    return result


@router.get("/summary")
async def get_finance_summary(
    db: Session = Depends(get_db)
):
    """High-level finance summary: current month vs prior month."""
    cached = get_cached("finance_summary")
    if cached is not _MISS:
        return cached

    service = FinanceService(db)
    summary = service.get_finance_summary()

    response = {
        "success": True,
        "data": summary,
    }

    # Inject stale-data warning if any source is behind
    stale_warning = get_stale_data_warning(db)
    if stale_warning:
        response['data_warning'] = stale_warning

    set_cached("finance_summary", response, 300)
    return response
