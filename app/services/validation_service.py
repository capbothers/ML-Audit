"""
Data Validation Service

Validates incoming data before persistence to ensure data quality.
Records validation failures for audit and debugging.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re

from app.models.base import SessionLocal
from app.models.data_quality import ValidationFailure
from app.utils.logger import log


@dataclass
class ValidationError:
    """Represents a single validation error"""
    field_name: str
    failure_type: str  # missing_required, invalid_type, out_of_range, invalid_format, referential_integrity
    message: str
    raw_value: Any = None
    expected_format: str = None
    rule_name: str = None
    severity: str = "warning"  # error = block save, warning = log and continue


@dataclass
class ValidationResult:
    """Result of validating an entity"""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)

    @property
    def has_blocking_errors(self) -> bool:
        """Returns True if any errors should block the save"""
        return any(e.severity == "error" for e in self.errors)

    @property
    def all_issues(self) -> List[ValidationError]:
        """Returns all issues (errors + warnings)"""
        return self.errors + self.warnings


class ValidationService:
    """
    Centralized validation service for all data sources.

    Validates data before persistence and logs failures for audit.
    """

    def __init__(self):
        # Validation rules can be extended via configuration
        self._custom_rules: Dict[str, Callable] = {}

    # ==================== SHOPIFY VALIDATION ====================

    def validate_shopify_order(self, order_data: dict) -> ValidationResult:
        """
        Validate a Shopify order before saving.

        Rules:
        - id: required
        - total_price: required, must be numeric, non-negative
        - currency: must be valid ISO code if present
        - email: must be valid format if present
        - created_at: must be valid ISO timestamp if present
        """
        errors = []
        warnings = []

        # Required fields
        if not order_data.get('id'):
            errors.append(ValidationError(
                field_name="id",
                failure_type="missing_required",
                message="Order ID is required",
                severity="error"
            ))

        # Total price validation - required field
        total_price = order_data.get('total_price')
        if total_price is None:
            errors.append(ValidationError(
                field_name="total_price",
                failure_type="missing_required",
                message="Total price is required",
                severity="error"
            ))
        else:
            try:
                price_val = Decimal(str(total_price))
                if price_val < 0:
                    errors.append(ValidationError(
                        field_name="total_price",
                        failure_type="out_of_range",
                        message="Total price cannot be negative",
                        raw_value=str(total_price),
                        expected_format="non-negative number",
                        severity="error"
                    ))
            except (InvalidOperation, ValueError):
                errors.append(ValidationError(
                    field_name="total_price",
                    failure_type="invalid_type",
                    message="Total price must be a valid number",
                    raw_value=str(total_price),
                    expected_format="numeric",
                    severity="error"
                ))

        # Email validation (warning only - save anyway)
        email = order_data.get('email')
        if email and not self._is_valid_email(email):
            warnings.append(ValidationError(
                field_name="email",
                failure_type="invalid_format",
                message="Email format is invalid",
                raw_value=email,
                expected_format="valid@email.com",
                severity="warning"
            ))

        # Currency validation
        currency = order_data.get('currency')
        if currency and not self._is_valid_currency(currency):
            warnings.append(ValidationError(
                field_name="currency",
                failure_type="invalid_format",
                message=f"Currency code '{currency}' may not be valid ISO 4217",
                raw_value=currency,
                expected_format="ISO 4217 (e.g., AUD, USD)",
                severity="warning"
            ))

        # Timestamp validation
        created_at = order_data.get('created_at')
        if created_at and not self._is_valid_timestamp(created_at):
            warnings.append(ValidationError(
                field_name="created_at",
                failure_type="invalid_format",
                message="Created at timestamp is not valid ISO format",
                raw_value=str(created_at)[:100],
                expected_format="ISO 8601",
                severity="warning"
            ))

        # Line items validation
        line_items = order_data.get('line_items', [])
        if line_items:
            for i, item in enumerate(line_items[:10]):  # Check first 10
                if isinstance(item, dict):
                    item_price = item.get('price')
                    if item_price is not None:
                        try:
                            if Decimal(str(item_price)) < 0:
                                warnings.append(ValidationError(
                                    field_name=f"line_items[{i}].price",
                                    failure_type="out_of_range",
                                    message=f"Line item {i} has negative price",
                                    raw_value=str(item_price),
                                    severity="warning"
                                ))
                        except (InvalidOperation, ValueError):
                            warnings.append(ValidationError(
                                field_name=f"line_items[{i}].price",
                                failure_type="invalid_type",
                                message=f"Line item {i} price must be a valid number",
                                raw_value=str(item_price)[:50],
                                expected_format="numeric",
                                severity="warning"
                            ))

        has_errors = len(errors) > 0
        return ValidationResult(
            valid=not has_errors,
            errors=errors,
            warnings=warnings
        )

    # ==================== KLAVIYO VALIDATION ====================

    def validate_klaviyo_campaign(self, campaign_data: dict) -> ValidationResult:
        """Validate a Klaviyo campaign before saving."""
        errors = []
        warnings = []

        # Required fields
        if not campaign_data.get('id'):
            errors.append(ValidationError(
                field_name="id",
                failure_type="missing_required",
                message="Campaign ID is required",
                severity="error"
            ))

        # Metrics validation
        metrics = campaign_data.get('metrics', {})
        if metrics:
            # Open rate should be 0-100
            open_rate = metrics.get('open_rate')
            if open_rate is not None:
                try:
                    rate = float(open_rate)
                    if rate < 0 or rate > 100:
                        warnings.append(ValidationError(
                            field_name="metrics.open_rate",
                            failure_type="out_of_range",
                            message="Open rate should be between 0 and 100",
                            raw_value=str(open_rate),
                            severity="warning"
                        ))
                except (ValueError, TypeError):
                    warnings.append(ValidationError(
                        field_name="metrics.open_rate",
                        failure_type="invalid_type",
                        message="Open rate must be a valid number",
                        raw_value=str(open_rate),
                        expected_format="numeric (0-100)",
                        severity="warning"
                    ))

            # Click rate should be 0-100
            click_rate = metrics.get('click_rate')
            if click_rate is not None:
                try:
                    rate = float(click_rate)
                    if rate < 0 or rate > 100:
                        warnings.append(ValidationError(
                            field_name="metrics.click_rate",
                            failure_type="out_of_range",
                            message="Click rate should be between 0 and 100",
                            raw_value=str(click_rate),
                            severity="warning"
                        ))
                except (ValueError, TypeError):
                    warnings.append(ValidationError(
                        field_name="metrics.click_rate",
                        failure_type="invalid_type",
                        message="Click rate must be a valid number",
                        raw_value=str(click_rate),
                        expected_format="numeric (0-100)",
                        severity="warning"
                    ))

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    def validate_klaviyo_flow(self, flow_data: dict) -> ValidationResult:
        """Validate a Klaviyo flow before saving."""
        errors = []

        if not flow_data.get('id'):
            errors.append(ValidationError(
                field_name="id",
                failure_type="missing_required",
                message="Flow ID is required",
                severity="error"
            ))

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_klaviyo_segment(self, segment_data: dict) -> ValidationResult:
        """Validate a Klaviyo segment before saving."""
        errors = []

        if not segment_data.get('id'):
            errors.append(ValidationError(
                field_name="id",
                failure_type="missing_required",
                message="Segment ID is required",
                severity="error"
            ))

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    # ==================== GA4 VALIDATION ====================

    def validate_ga4_daily_metric(self, metric_data: dict) -> ValidationResult:
        """Validate a GA4 daily metric record before saving."""
        errors = []
        warnings = []

        # Date is required
        date_str = metric_data.get('date')
        if not date_str:
            errors.append(ValidationError(
                field_name="date",
                failure_type="missing_required",
                message="Date is required for GA4 metrics",
                severity="error"
            ))
        elif not self._is_valid_ga4_date(date_str):
            errors.append(ValidationError(
                field_name="date",
                failure_type="invalid_format",
                message="Date format is invalid",
                raw_value=str(date_str),
                expected_format="YYYYMMDD or YYYY-MM-DD",
                severity="error"
            ))

        # Sessions should be non-negative
        sessions = metric_data.get('sessions')
        if sessions is not None:
            try:
                if int(sessions) < 0:
                    warnings.append(ValidationError(
                        field_name="sessions",
                        failure_type="out_of_range",
                        message="Sessions count cannot be negative",
                        raw_value=str(sessions),
                        severity="warning"
                    ))
            except (ValueError, TypeError):
                warnings.append(ValidationError(
                    field_name="sessions",
                    failure_type="invalid_type",
                    message="Sessions must be a valid integer",
                    raw_value=str(sessions),
                    expected_format="integer",
                    severity="warning"
                ))

        # Bounce rate should be 0-100
        bounce_rate = metric_data.get('bounce_rate')
        if bounce_rate is not None:
            try:
                rate = float(bounce_rate)
                if rate < 0 or rate > 100:
                    warnings.append(ValidationError(
                        field_name="bounce_rate",
                        failure_type="out_of_range",
                        message="Bounce rate should be between 0 and 100",
                        raw_value=str(bounce_rate),
                        severity="warning"
                    ))
            except (ValueError, TypeError):
                warnings.append(ValidationError(
                    field_name="bounce_rate",
                    failure_type="invalid_type",
                    message="Bounce rate must be a valid number",
                    raw_value=str(bounce_rate),
                    expected_format="numeric (0-100)",
                    severity="warning"
                ))

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    # ==================== SEARCH CONSOLE VALIDATION ====================

    def validate_search_query(self, query_data: dict) -> ValidationResult:
        """Validate a Search Console query record before saving."""
        errors = []
        warnings = []

        # Query text is required
        if not query_data.get('query'):
            errors.append(ValidationError(
                field_name="query",
                failure_type="missing_required",
                message="Query text is required",
                severity="error"
            ))

        # CTR should be 0-1 (decimal) or 0-100 (percentage)
        # Search Console API returns 0-1 range
        ctr = query_data.get('ctr')
        if ctr is not None:
            try:
                ctr_val = float(ctr)
                if ctr_val < 0:
                    warnings.append(ValidationError(
                        field_name="ctr",
                        failure_type="out_of_range",
                        message="CTR cannot be negative",
                        raw_value=str(ctr),
                        expected_format="0.0-1.0 (decimal) or 0-100 (percent)",
                        severity="warning"
                    ))
                elif ctr_val > 1:
                    # Could be percentage format (0-100) - warn but don't block
                    if ctr_val > 100:
                        warnings.append(ValidationError(
                            field_name="ctr",
                            failure_type="out_of_range",
                            message="CTR exceeds 100% - value appears invalid",
                            raw_value=str(ctr),
                            expected_format="0.0-1.0 (decimal) or 0-100 (percent)",
                            severity="warning"
                        ))
            except (ValueError, TypeError):
                warnings.append(ValidationError(
                    field_name="ctr",
                    failure_type="invalid_type",
                    message="CTR must be a valid number",
                    raw_value=str(ctr),
                    expected_format="numeric",
                    severity="warning"
                ))

        # Position should be positive
        position = query_data.get('position')
        if position is not None:
            try:
                if float(position) < 0:
                    warnings.append(ValidationError(
                        field_name="position",
                        failure_type="out_of_range",
                        message="Position cannot be negative",
                        raw_value=str(position),
                        severity="warning"
                    ))
            except (ValueError, TypeError):
                warnings.append(ValidationError(
                    field_name="position",
                    failure_type="invalid_type",
                    message="Position must be a valid number",
                    raw_value=str(position),
                    expected_format="numeric",
                    severity="warning"
                ))

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    # ==================== PERSISTENCE ====================

    def persist_validation_failures(
        self,
        failures: List[ValidationError],
        entity_type: str,
        entity_id: str,
        source: str,
        sync_log_id: int = None
    ) -> int:
        """
        Persist validation failures to database.

        Returns the number of failures persisted.
        """
        if not failures:
            return 0

        db = SessionLocal()
        persisted = 0

        try:
            for failure in failures:
                record = ValidationFailure(
                    sync_log_id=sync_log_id,
                    entity_type=entity_type,
                    entity_id=str(entity_id) if entity_id else "unknown",
                    source=source,
                    field_name=failure.field_name,
                    failure_type=failure.failure_type,
                    failure_message=failure.message,
                    raw_value=str(failure.raw_value)[:1000] if failure.raw_value else None,
                    expected_format=failure.expected_format,
                    validation_rule=failure.rule_name,
                    severity=failure.severity
                )
                db.add(record)
                persisted += 1

            db.commit()
            log.debug(f"Persisted {persisted} validation failures for {source}/{entity_type}/{entity_id}")

        except Exception as e:
            db.rollback()
            log.error(f"Failed to persist validation failures: {e}")
            return 0
        finally:
            db.close()

        return persisted

    # ==================== HELPER METHODS ====================

    def _is_valid_email(self, email: str) -> bool:
        """Basic email format validation"""
        if not isinstance(email, str):
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    def _is_valid_currency(self, currency: str) -> bool:
        """Check if currency is a valid 3-letter ISO code"""
        if not isinstance(currency, str):
            return False
        # Common currencies - extend as needed
        valid_currencies = {
            'AUD', 'USD', 'EUR', 'GBP', 'CAD', 'NZD', 'JPY', 'CNY',
            'SGD', 'HKD', 'CHF', 'SEK', 'NOK', 'DKK', 'INR', 'BRL'
        }
        return currency.upper() in valid_currencies or (len(currency) == 3 and currency.isalpha())

    def _is_valid_timestamp(self, timestamp: str) -> bool:
        """Check if timestamp is valid ISO 8601 format"""
        if not isinstance(timestamp, str):
            return False
        try:
            # Try common formats
            if 'T' in timestamp:
                datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                datetime.strptime(timestamp, '%Y-%m-%d')
            return True
        except (ValueError, AttributeError):
            return False

    def _is_valid_ga4_date(self, date_str: str) -> bool:
        """Check if date is valid GA4 format (YYYYMMDD or YYYY-MM-DD)"""
        if not isinstance(date_str, str):
            return False
        try:
            if len(date_str) == 8 and date_str.isdigit():
                datetime.strptime(date_str, '%Y%m%d')
                return True
            elif len(date_str) == 10 and '-' in date_str:
                datetime.strptime(date_str, '%Y-%m-%d')
                return True
            return False
        except ValueError:
            return False


# Singleton instance
validation_service = ValidationService()
