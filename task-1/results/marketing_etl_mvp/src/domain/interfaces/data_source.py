from abc import ABC, abstractmethod

from src.domain.entities import Customer, Delivery, Order


class DataSourceInterface(ABC):
    """Interface for data sources."""

    @abstractmethod
    async def get_customers(self, limit: int = 100) -> list[Customer]:
        """Retrieve customers from the data source."""
        pass

    @abstractmethod
    async def get_orders(self, limit: int = 100) -> list[Order]:
        """Retrieve orders from the data source."""
        pass

    @abstractmethod
    async def get_deliveries(self) -> list[Delivery]:
        """Retrieve deliveries from the data source."""
        pass
