"""
Business Expense CSV Import Service

Imports expenses from CSV files (exported from Xero, MYOB, QuickBooks, etc.)
Supports flexible column names and category normalization.
"""
import csv
import io
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Optional, Tuple
from sqlalchemy.orm import Session

from app.models.business_expense import BusinessExpense, EXPENSE_CATEGORIES
from app.utils.logger import log


# Map common CSV column names to our fields
COLUMN_ALIASES = {
    # month
    'month': 'month',
    'date': 'month',
    'period': 'month',
    'invoice_date': 'month',
    'pay_date': 'month',
    # category
    'category': 'category',
    'type': 'category',
    'expense_type': 'category',
    'account': 'category',
    'account_name': 'category',
    # description
    'description': 'description',
    'name': 'description',
    'memo': 'description',
    'details': 'description',
    'item': 'description',
    'reference': 'description',
    'invoice_number': 'description',
    # amount
    'amount': 'amount',
    'total': 'amount',
    'value': 'amount',
    'cost': 'amount',
    'debit': 'amount',
    'net_amount': 'amount',
    'amount_inc_gst': 'amount',
    # notes
    'notes': 'notes',
    'comment': 'notes',
    'comments': 'notes',
    # is_recurring
    'is_recurring': 'is_recurring',
    'recurring': 'is_recurring',
}

# Map common category names to standard categories
CATEGORY_ALIASES = {
    # payroll
    'payroll': 'payroll',
    'wages': 'payroll',
    'salary': 'payroll',
    'salaries': 'payroll',
    'staff': 'payroll',
    'staff wages': 'payroll',
    'employee': 'payroll',
    'superannuation': 'payroll',
    'super': 'payroll',
    'workers comp': 'payroll',
    # rent
    'rent': 'rent',
    'lease': 'rent',
    'warehouse': 'rent',
    'warehouse rent': 'rent',
    'office rent': 'rent',
    'premises': 'rent',
    # shipping
    'shipping': 'shipping',
    'freight': 'shipping',
    'delivery': 'shipping',
    'postage': 'shipping',
    'fulfillment': 'shipping',
    'fulfilment': 'shipping',
    'courier': 'shipping',
    'auspost': 'shipping',
    'australia post': 'shipping',
    # utilities
    'utilities': 'utilities',
    'utility': 'utilities',
    'electricity': 'utilities',
    'power': 'utilities',
    'gas': 'utilities',
    'water': 'utilities',
    'internet': 'utilities',
    'phone': 'utilities',
    'telecom': 'utilities',
    'power + internet': 'utilities',
    # insurance
    'insurance': 'insurance',
    'business insurance': 'insurance',
    'liability insurance': 'insurance',
    'public liability': 'insurance',
    # software
    'software': 'software',
    'subscriptions': 'software',
    'shopify': 'software',
    'tools': 'software',
    'saas': 'software',
    'shopify + tools': 'software',
    # marketing_other
    'marketing': 'marketing_other',
    'marketing_other': 'marketing_other',
    'advertising': 'marketing_other',
    'facebook ads': 'marketing_other',
    'meta ads': 'marketing_other',
    'social media': 'marketing_other',
    # professional_services
    'professional_services': 'professional_services',
    'professional services': 'professional_services',
    'accounting': 'professional_services',
    'legal': 'professional_services',
    'bookkeeping': 'professional_services',
    'consultant': 'professional_services',
    'consulting': 'professional_services',
    # other
    'other': 'other',
    'miscellaneous': 'other',
    'misc': 'other',
    'general': 'other',
    'office supplies': 'other',
    'maintenance': 'other',
    'repairs': 'other',
    'cleaning': 'other',
    'travel': 'other',
}


class ExpenseImportService:
    def __init__(self, db: Session):
        self.db = db

    def import_csv(self, csv_content: str) -> Dict:
        """
        Import expenses from CSV content.

        Returns summary dict with counts and any errors.
        """
        rows = list(csv.DictReader(io.StringIO(csv_content)))

        if not rows:
            return {"success": False, "error": "CSV is empty or has no data rows"}

        # Map columns
        column_map = self._map_columns(rows[0].keys())

        if 'month' not in column_map:
            return {"success": False, "error": "No month/date column found. Expected: month, date, period, or invoice_date"}
        if 'amount' not in column_map:
            return {"success": False, "error": "No amount column found. Expected: amount, total, value, or cost"}
        if 'category' not in column_map:
            return {"success": False, "error": "No category column found. Expected: category, type, expense_type, or account"}

        created = 0
        updated = 0
        skipped = 0
        errors = []

        for i, row in enumerate(rows, start=2):  # start=2 for 1-indexed + header
            try:
                result = self._process_row(row, column_map, i)
                if result == 'created':
                    created += 1
                elif result == 'updated':
                    updated += 1
                elif result == 'skipped':
                    skipped += 1
            except Exception as e:
                errors.append(f"Row {i}: {str(e)}")
                skipped += 1

        self.db.commit()

        return {
            "success": True,
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "total_rows": len(rows),
            "errors": errors[:20] if errors else [],
        }

    def _map_columns(self, headers) -> Dict[str, str]:
        """Map CSV headers to our standard field names."""
        column_map = {}
        for header in headers:
            normalized = header.strip().lower().replace('-', '_')
            if normalized in COLUMN_ALIASES:
                our_field = COLUMN_ALIASES[normalized]
                if our_field not in column_map:  # first match wins
                    column_map[our_field] = header
        return column_map

    def _process_row(self, row: Dict, column_map: Dict, row_num: int) -> str:
        """Process a single CSV row. Returns 'created', 'updated', or 'skipped'."""

        # Parse month
        month_raw = row.get(column_map.get('month', ''), '').strip()
        if not month_raw:
            raise ValueError("Empty month/date value")
        month = self._parse_month(month_raw)

        # Parse amount
        amount_raw = row.get(column_map.get('amount', ''), '').strip()
        if not amount_raw:
            raise ValueError("Empty amount value")
        amount = self._parse_amount(amount_raw)
        if amount <= 0:
            return 'skipped'  # Skip zero or negative amounts

        # Parse category
        category_raw = row.get(column_map.get('category', ''), '').strip()
        if not category_raw:
            raise ValueError("Empty category value")
        category = self._normalize_category(category_raw)

        # Optional fields
        description = row.get(column_map.get('description', ''), '').strip() or category_raw
        notes = row.get(column_map.get('notes', ''), '').strip() or None
        is_recurring = self._parse_bool(
            row.get(column_map.get('is_recurring', ''), '').strip()
        )

        # Upsert by (month, category, description)
        existing = self.db.query(BusinessExpense).filter(
            BusinessExpense.month == month,
            BusinessExpense.category == category,
            BusinessExpense.description == description
        ).first()

        if existing:
            existing.amount = amount
            existing.notes = notes
            existing.is_recurring = is_recurring
            existing.updated_at = datetime.utcnow()
            return 'updated'
        else:
            expense = BusinessExpense(
                month=month,
                category=category,
                description=description,
                amount=amount,
                is_recurring=is_recurring,
                notes=notes,
            )
            self.db.add(expense)
            return 'created'

    def _parse_month(self, value: str) -> date:
        """Parse month from various formats to first-of-month date."""
        value = value.strip()

        # Try YYYY-MM format
        for fmt in ['%Y-%m', '%Y/%m', '%m/%Y', '%m-%Y']:
            try:
                dt = datetime.strptime(value, fmt)
                return date(dt.year, dt.month, 1)
            except ValueError:
                continue

        # Try full date formats — normalize to first of month
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y']:
            try:
                dt = datetime.strptime(value, fmt)
                return date(dt.year, dt.month, 1)
            except ValueError:
                continue

        raise ValueError(f"Cannot parse month from '{value}'. Expected YYYY-MM or similar.")

    def _parse_amount(self, value: str) -> Decimal:
        """Parse amount, stripping currency symbols and commas."""
        value = value.strip().replace('$', '').replace(',', '').replace(' ', '')
        if value.startswith('(') and value.endswith(')'):
            value = '-' + value[1:-1]  # Handle accounting negative format
        try:
            return Decimal(value)
        except InvalidOperation:
            raise ValueError(f"Cannot parse amount from '{value}'")

    def _normalize_category(self, value: str) -> str:
        """Normalize category name to standard categories."""
        normalized = value.strip().lower()
        if normalized in CATEGORY_ALIASES:
            return CATEGORY_ALIASES[normalized]
        # If no match, use 'other'
        log.warning(f"Unknown expense category '{value}', mapping to 'other'")
        return 'other'

    def _parse_bool(self, value: str) -> bool:
        """Parse boolean from string."""
        if not value:
            return True  # Default to recurring
        return value.lower() in ('true', 'yes', '1', 'y')

    def get_expenses(self, month: Optional[date] = None,
                     category: Optional[str] = None) -> List[Dict]:
        """Get expenses with optional filters."""
        query = self.db.query(BusinessExpense)

        if month:
            query = query.filter(BusinessExpense.month == month)
        if category:
            query = query.filter(BusinessExpense.category == category)

        query = query.order_by(BusinessExpense.month.desc(), BusinessExpense.category)

        return [
            {
                "id": e.id,
                "month": e.month.isoformat(),
                "category": e.category,
                "description": e.description,
                "amount": float(e.amount),
                "is_recurring": e.is_recurring,
                "notes": e.notes,
            }
            for e in query.all()
        ]

    def add_expense(self, month: date, category: str, description: str,
                    amount: Decimal, is_recurring: bool = True,
                    notes: Optional[str] = None) -> Dict:
        """Add a single expense manually."""
        category = self._normalize_category(category)

        expense = BusinessExpense(
            month=month,
            category=category,
            description=description,
            amount=amount,
            is_recurring=is_recurring,
            notes=notes,
        )
        self.db.add(expense)
        self.db.commit()

        return {
            "id": expense.id,
            "month": expense.month.isoformat(),
            "category": expense.category,
            "description": expense.description,
            "amount": float(expense.amount),
        }
