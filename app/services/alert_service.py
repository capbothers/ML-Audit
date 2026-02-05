"""
Alert Service
Sends automated alerts via email, Slack, etc.
"""
import smtplib
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict, List, Optional
import aiohttp
from datetime import datetime
from dataclasses import dataclass, field

from app.config import get_settings
from app.utils.logger import log
from app.utils.retry import calculate_backoff, is_retryable_error

settings = get_settings()


@dataclass
class DeliveryResult:
    """Tracks delivery attempt results for auditing."""
    success: bool = False
    channel: str = ""
    attempts: int = 0
    total_delay_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    final_error: Optional[str] = None


class AlertService:
    """
    Manages automated alerts and notifications with retry logic.
    """

    # Retry configuration
    RETRY_MAX_ATTEMPTS = 3
    RETRY_BASE_DELAY = 2.0  # seconds
    RETRY_MAX_DELAY = 30.0  # seconds

    def __init__(self):
        self.smtp_configured = all([
            settings.smtp_host,
            settings.smtp_user,
            settings.smtp_password,
            settings.alert_email_to
        ])

        self.slack_configured = bool(settings.slack_webhook_url)

        # Track delivery stats
        self.total_sent = 0
        self.total_failed = 0
        self.total_retries = 0

    async def send_critical_alert(
        self,
        title: str,
        message: str,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Send critical alert via all available channels.

        Returns:
            Dict with keys: success (bool), results (dict of channel -> DeliveryResult),
            total_attempts, total_delay_seconds
        """
        log.warning(f"Critical alert: {title}")

        results = {}
        any_success = False

        # Send email
        if self.smtp_configured:
            email_result = await self.send_email_alert(title, message, data, priority='critical')
            results['email'] = email_result
            any_success = any_success or email_result.success

        # Send Slack
        if self.slack_configured:
            slack_result = await self.send_slack_alert(title, message, data, priority='critical')
            results['slack'] = slack_result
            any_success = any_success or slack_result.success

        # Aggregate stats
        total_attempts = sum(r.attempts for r in results.values())
        total_delay = sum(r.total_delay_seconds for r in results.values())

        return {
            'success': any_success,
            'results': {ch: self._delivery_result_to_dict(r) for ch, r in results.items()},
            'total_attempts': total_attempts,
            'total_delay_seconds': total_delay
        }

    def _delivery_result_to_dict(self, result: DeliveryResult) -> Dict[str, Any]:
        """Convert DeliveryResult to dict for storage."""
        return {
            'success': result.success,
            'attempts': result.attempts,
            'total_delay_seconds': result.total_delay_seconds,
            'errors': result.errors[:5],  # Cap at 5 errors
            'final_error': result.final_error
        }

    async def send_email_alert(
        self,
        title: str,
        message: str,
        data: Optional[Dict] = None,
        priority: str = 'medium'
    ) -> DeliveryResult:
        """
        Send email alert with retry logic.

        Returns DeliveryResult with success status and delivery attempt details.
        Retries on transient errors (connection, timeout) with exponential backoff.
        """
        result = DeliveryResult(channel='email')

        if not self.smtp_configured:
            log.warning("Email not configured, skipping email alert")
            result.final_error = "Email not configured"
            return result

        for attempt in range(1, self.RETRY_MAX_ATTEMPTS + 1):
            result.attempts = attempt

            try:
                msg = MIMEMultipart('alternative')
                msg['Subject'] = f"[{priority.upper()}] {title}"
                msg['From'] = settings.alert_email_from or settings.smtp_user
                msg['To'] = settings.alert_email_to

                # Create HTML body
                html_body = self._create_html_email(title, message, data, priority)

                msg.attach(MIMEText(message, 'plain'))
                msg.attach(MIMEText(html_body, 'html'))

                # Send email
                with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as server:
                    server.starttls()
                    server.login(settings.smtp_user, settings.smtp_password)
                    server.send_message(msg)

                # Success
                result.success = True
                self.total_sent += 1
                if attempt > 1:
                    self.total_retries += (attempt - 1)
                    log.info(f"Email alert sent after {attempt} attempts: {title}")
                else:
                    log.info(f"Email alert sent: {title}")
                return result

            except Exception as e:
                error_str = f"{type(e).__name__}: {str(e)}"
                result.errors.append(error_str)
                result.final_error = error_str

                # Check if we should retry
                if attempt >= self.RETRY_MAX_ATTEMPTS or not self._is_retryable_email_error(e):
                    self.total_failed += 1
                    log.error(f"Email alert failed after {attempt} attempts: {error_str}")
                    return result

                # Calculate backoff delay
                delay = calculate_backoff(
                    attempt,
                    base_delay=self.RETRY_BASE_DELAY,
                    max_delay=self.RETRY_MAX_DELAY
                )
                result.total_delay_seconds += delay

                log.warning(f"Email attempt {attempt} failed: {e}. Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)

        # Should not reach here
        self.total_failed += 1
        return result

    def _is_retryable_email_error(self, error: Exception) -> bool:
        """Check if email error is retryable."""
        # Connection and timeout errors are retryable
        if isinstance(error, (ConnectionError, TimeoutError, OSError)):
            return True

        # SMTP temporary failures (4xx) are retryable
        if isinstance(error, smtplib.SMTPResponseException):
            return 400 <= error.smtp_code < 500

        # Check error message for common transient issues
        error_str = str(error).lower()
        retryable_patterns = ['timeout', 'connection', 'temporary', 'try again', 'unavailable']
        return any(pattern in error_str for pattern in retryable_patterns)

    async def send_slack_alert(
        self,
        title: str,
        message: str,
        data: Optional[Dict] = None,
        priority: str = 'medium'
    ) -> DeliveryResult:
        """
        Send Slack alert with retry logic.

        Returns DeliveryResult with success status and delivery attempt details.
        Retries on transient errors (connection, timeout, 5xx) with exponential backoff.
        """
        result = DeliveryResult(channel='slack')

        if not self.slack_configured:
            log.warning("Slack not configured, skipping Slack alert")
            result.final_error = "Slack not configured"
            return result

        # Color based on priority
        colors = {
            'critical': '#dc3545',
            'high': '#fd7e14',
            'medium': '#ffc107',
            'low': '#28a745'
        }
        color = colors.get(priority, '#6c757d')

        # Create Slack message
        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "fields": [
                        {
                            "title": "Priority",
                            "value": priority.upper(),
                            "short": True
                        },
                        {
                            "title": "Timestamp",
                            "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                            "short": True
                        }
                    ],
                    "footer": "ML-Audit Growth Intelligence Platform"
                }
            ]
        }

        # Add data fields if provided
        if data:
            for key, value in list(data.items())[:5]:  # Max 5 additional fields
                payload["attachments"][0]["fields"].append({
                    "title": key.replace('_', ' ').title(),
                    "value": str(value),
                    "short": True
                })

        for attempt in range(1, self.RETRY_MAX_ATTEMPTS + 1):
            result.attempts = attempt

            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                    async with session.post(
                        settings.slack_webhook_url,
                        json=payload
                    ) as response:
                        if response.status == 200:
                            # Success
                            result.success = True
                            self.total_sent += 1
                            if attempt > 1:
                                self.total_retries += (attempt - 1)
                                log.info(f"Slack alert sent after {attempt} attempts: {title}")
                            else:
                                log.info(f"Slack alert sent: {title}")
                            return result

                        # Check if status code is retryable
                        if self._is_retryable_slack_status(response.status):
                            error_str = f"HTTP {response.status}"
                            result.errors.append(error_str)

                            if attempt >= self.RETRY_MAX_ATTEMPTS:
                                result.final_error = error_str
                                self.total_failed += 1
                                log.error(f"Slack alert failed after {attempt} attempts: {error_str}")
                                return result

                            delay = calculate_backoff(
                                attempt,
                                base_delay=self.RETRY_BASE_DELAY,
                                max_delay=self.RETRY_MAX_DELAY
                            )
                            result.total_delay_seconds += delay

                            log.warning(f"Slack attempt {attempt} failed: {error_str}. Retrying in {delay:.1f}s...")
                            await asyncio.sleep(delay)
                            continue

                        # Non-retryable error (4xx except 429)
                        result.final_error = f"HTTP {response.status}"
                        self.total_failed += 1
                        log.error(f"Slack alert failed with status {response.status}")
                        return result

            except Exception as e:
                error_str = f"{type(e).__name__}: {str(e)}"
                result.errors.append(error_str)

                # Check if we should retry
                if attempt >= self.RETRY_MAX_ATTEMPTS or not self._is_retryable_slack_error(e):
                    result.final_error = error_str
                    self.total_failed += 1
                    log.error(f"Slack alert failed after {attempt} attempts: {error_str}")
                    return result

                delay = calculate_backoff(
                    attempt,
                    base_delay=self.RETRY_BASE_DELAY,
                    max_delay=self.RETRY_MAX_DELAY
                )
                result.total_delay_seconds += delay

                log.warning(f"Slack attempt {attempt} failed: {e}. Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)

        # Should not reach here
        self.total_failed += 1
        return result

    def _is_retryable_slack_status(self, status_code: int) -> bool:
        """Check if HTTP status code warrants a retry."""
        # 429 (rate limit) and 5xx (server errors) are retryable
        return status_code == 429 or status_code >= 500

    def _is_retryable_slack_error(self, error: Exception) -> bool:
        """Check if Slack error is retryable."""
        # Connection and timeout errors are retryable
        if isinstance(error, (ConnectionError, TimeoutError, OSError)):
            return True

        # aiohttp specific errors
        if isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError)):
            return True

        # Check error message for common transient issues
        error_str = str(error).lower()
        retryable_patterns = ['timeout', 'connection', 'temporary', 'unavailable']
        return any(pattern in error_str for pattern in retryable_patterns)

    async def send_daily_summary(
        self,
        recommendations: List[Dict],
        metrics: Dict
    ) -> bool:
        """
        Send daily summary of insights and recommendations
        """
        title = f"Daily Growth Intelligence Summary - {datetime.utcnow().strftime('%Y-%m-%d')}"

        # Build summary message
        critical_count = len([r for r in recommendations if r.get('priority') == 'critical'])
        high_count = len([r for r in recommendations if r.get('priority') == 'high'])

        message = f"""
Daily Summary:
- {len(recommendations)} total recommendations
- {critical_count} critical actions needed
- {high_count} high-priority actions

Top Recommendations:
"""

        for rec in recommendations[:5]:
            message += f"\n• [{rec.get('priority', 'N/A').upper()}] {rec.get('title', 'N/A')}"

        # Send via both channels
        success = False

        if self.smtp_configured:
            email_result = await self.send_email_alert(
                title,
                message,
                data=metrics,
                priority='medium'
            )
            success = success or email_result.success

        if self.slack_configured:
            slack_result = await self.send_slack_alert(
                title,
                message,
                data=metrics,
                priority='medium'
            )
            success = success or slack_result.success

        return success

    async def send_churn_alert(
        self,
        high_risk_customers: List[Dict]
    ) -> bool:
        """
        Send alert for high-risk churn customers
        """
        if not high_risk_customers:
            return False

        total_value = sum(c.get('total_spent', 0) for c in high_risk_customers)

        title = f"Churn Alert: {len(high_risk_customers)} High-Risk Customers"
        message = f"""
WARNING: {len(high_risk_customers)} customers are at high risk of churning.

Total customer value at risk: ${total_value:,.2f}

These customers need immediate attention through:
- Win-back email campaigns
- Personalized offers
- Engagement outreach

Top at-risk customers:
"""

        for customer in high_risk_customers[:10]:
            message += f"\n• {customer.get('email')} - ${customer.get('total_spent', 0):,.2f} spent, {customer.get('churn_probability', 0):.1%} churn risk"

        result = await self.send_critical_alert(
            title,
            message,
            data={
                'high_risk_count': len(high_risk_customers),
                'total_value_at_risk': f"${total_value:,.2f}"
            }
        )
        return result['success']

    async def send_anomaly_alert(
        self,
        anomaly: Dict
    ) -> bool:
        """
        Send alert for detected anomaly
        """
        title = f"Anomaly Detected: {anomaly.get('metric', 'Unknown Metric')}"

        message = f"""
Unusual pattern detected in {anomaly.get('metric', 'metric')}

Details:
- Current value: {anomaly.get('value', 'N/A')}
- Expected value: {anomaly.get('expected_value', 'N/A')}
- Deviation: {anomaly.get('deviation_pct', 0):.1f}%
- Direction: {anomaly.get('direction', 'unknown')}
- Severity: {anomaly.get('severity', 'unknown')}

Immediate investigation recommended.
"""

        priority = 'critical' if anomaly.get('severity') in ['critical', 'high'] else 'high'

        result = await self.send_critical_alert(title, message, data=anomaly)
        return result['success']

    def _create_html_email(
        self,
        title: str,
        message: str,
        data: Optional[Dict],
        priority: str
    ) -> str:
        """Create HTML email body"""

        priority_colors = {
            'critical': '#dc3545',
            'high': '#fd7e14',
            'medium': '#ffc107',
            'low': '#28a745'
        }

        color = priority_colors.get(priority, '#6c757d')

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
        .header {{ background-color: {color}; color: white; padding: 20px; }}
        .content {{ padding: 20px; }}
        .footer {{ background-color: #f8f9fa; padding: 10px; text-align: center; font-size: 12px; }}
        .data-table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        .data-table td {{ padding: 8px; border-bottom: 1px solid #dee2e6; }}
        .data-table td:first-child {{ font-weight: bold; width: 40%; }}
    </style>
</head>
<body>
    <div class="header">
        <h2>{title}</h2>
        <p>Priority: {priority.upper()}</p>
    </div>
    <div class="content">
        <p>{message.replace(chr(10), '<br>')}</p>
"""

        if data:
            html += '<table class="data-table">'
            for key, value in data.items():
                html += f"<tr><td>{key.replace('_', ' ').title()}</td><td>{value}</td></tr>"
            html += '</table>'

        html += """
    </div>
    <div class="footer">
        <p>ML-Audit Growth Intelligence Platform</p>
        <p>Generated at """ + datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC") + """</p>
    </div>
</body>
</html>
"""

        return html

    def get_delivery_stats(self) -> Dict[str, Any]:
        """
        Get delivery statistics for monitoring.

        Returns:
            Dict with delivery metrics: total_sent, total_failed, total_retries,
            success_rate, and configuration.
        """
        total_attempts = self.total_sent + self.total_failed
        success_rate = (self.total_sent / total_attempts * 100) if total_attempts > 0 else 0.0

        return {
            "total_sent": self.total_sent,
            "total_failed": self.total_failed,
            "total_retries": self.total_retries,
            "total_attempts": total_attempts,
            "success_rate": round(success_rate, 2),
            "retry_config": {
                "max_attempts": self.RETRY_MAX_ATTEMPTS,
                "base_delay": self.RETRY_BASE_DELAY,
                "max_delay": self.RETRY_MAX_DELAY
            },
            "channels_configured": {
                "email": self.smtp_configured,
                "slack": self.slack_configured
            }
        }
