import structlog

from src.domain.interfaces import NotificationInterface

logger = structlog.get_logger()


class EmailNotificationService(NotificationInterface):
    """Email notification service (stub with logs)."""

    def __init__(self, smtp_host: str, smtp_port: int, from_email: str, to_email: str):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.from_email = from_email
        self.to_email = to_email

    async def send_success_notification(self, message: str) -> None:
        """Send success notification via email (stub)."""
        logger.info(
            "SUCCESS NOTIFICATION",
            smtp_host=self.smtp_host,
            smtp_port=self.smtp_port,
            from_email=self.from_email,
            to_email=self.to_email,
            message=message,
        )
        # In real implementation: send actual email via SMTP

    async def send_failure_notification(self, message: str, error: Exception) -> None:
        """Send failure notification via email (stub)."""
        logger.error(
            "FAILURE NOTIFICATION",
            smtp_host=self.smtp_host,
            smtp_port=self.smtp_port,
            from_email=self.from_email,
            to_email=self.to_email,
            message=message,
            error=str(error),
            exc_info=True,
        )
        # In real implementation: send actual email via SMTP with error details
