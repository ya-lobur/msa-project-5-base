from datetime import datetime

import structlog

from src.domain.entities import Delivery
from src.domain.interfaces import DataSourceInterface

logger = structlog.get_logger()


class CSVRepository(DataSourceInterface):
    """CSV file repository implementation (stub with logs)."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    async def get_deliveries(self) -> list[Delivery]:
        """Retrieve deliveries from CSV file (stub)."""
        logger.info("Reading deliveries from CSV", file_path=self.file_path)

        # Stub data - simulating CSV read
        deliveries = [
            Delivery(
                delivery_id=i,
                order_id=i,
                status="delivered" if i % 2 == 0 else "in_transit",
                delivered_at=datetime.now() if i % 2 == 0 else None,
                tracking_number=f"TRACK{i:06d}",
            )
            for i in range(1, 51)
        ]

        logger.info("Deliveries read successfully from CSV", count=len(deliveries))
        return deliveries

    async def get_customers(self, limit: int = 100) -> list:
        """This method is not used in CSVRepository."""
        return []

    async def get_orders(self, limit: int = 100) -> list:
        """This method is not used in CSVRepository."""
        return []
