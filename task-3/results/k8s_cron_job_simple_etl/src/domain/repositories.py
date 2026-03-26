"""Repository interfaces for the ETL system."""

from abc import ABC, abstractmethod
from typing import AsyncIterator

from src.domain.entities import Shipment


class ShipmentRepository(ABC):
    """Abstract repository for shipment data access."""

    @abstractmethod
    async def get_all(self, batch_size: int = 1000) -> AsyncIterator[list[Shipment]]:
        """
        Retrieve all shipments in batches.

        Args:
            batch_size: Number of records to fetch per batch

        Yields:
            Batches of shipment entities
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close repository connections and cleanup resources."""
        pass
