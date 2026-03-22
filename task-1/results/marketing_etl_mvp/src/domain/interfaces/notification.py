from abc import ABC, abstractmethod


class NotificationInterface(ABC):
    """Interface for notification services."""

    @abstractmethod
    async def send_success_notification(self, message: str) -> None:
        """Send success notification."""
        pass

    @abstractmethod
    async def send_failure_notification(self, message: str, error: Exception) -> None:
        """Send failure notification."""
        pass
