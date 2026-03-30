from src.domain.entities import Customer, Delivery, Order
from src.domain.interfaces import DataSourceInterface


class CombinedDataSource(DataSourceInterface):
    """Combined data source that aggregates multiple repositories."""

    def __init__(self, postgres_repo: DataSourceInterface, csv_repo: DataSourceInterface):
        self.postgres_repo = postgres_repo
        self.csv_repo = csv_repo

    async def get_customers(self, limit: int = 100) -> list[Customer]:
        """Get customers from PostgreSQL repository."""
        return await self.postgres_repo.get_customers(limit)

    async def get_orders(self, limit: int = 100) -> list[Order]:
        """Get orders from PostgreSQL repository."""
        return await self.postgres_repo.get_orders(limit)

    async def get_deliveries(self) -> list[Delivery]:
        """Get deliveries from CSV repository."""
        return await self.csv_repo.get_deliveries()
