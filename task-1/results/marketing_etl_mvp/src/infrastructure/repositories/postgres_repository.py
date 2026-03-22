from datetime import datetime
from decimal import Decimal

import structlog

from src.domain.entities import Customer, Order
from src.domain.interfaces import DataSourceInterface

logger = structlog.get_logger()


class PostgresRepository(DataSourceInterface):
    """PostgreSQL repository implementation (stub with logs)."""

    def __init__(self, database_url: str):
        self.database_url = database_url

    async def get_customers(self, limit: int = 100) -> list[Customer]:
        """Retrieve customers from PostgreSQL (stub)."""
        logger.info("Fetching customers from PostgreSQL", limit=limit, database_url=self.database_url)

        # Stub data
        customers = [
            Customer(
                customer_id=i,
                email=f"customer{i}@example.com",
                name=f"Customer {i}",
                created_at=datetime.now(),
                is_active=i % 3 != 0,  # Every 3rd customer is inactive
            )
            for i in range(1, min(limit + 1, 101))
        ]

        logger.info("Customers fetched successfully", count=len(customers))
        return customers

    async def get_orders(self, limit: int = 100) -> list[Order]:
        """Retrieve orders from PostgreSQL (stub)."""
        logger.info("Fetching orders from PostgreSQL", limit=limit, database_url=self.database_url)

        # Stub data
        orders = [
            Order(
                order_id=i,
                customer_id=(i % 50) + 1,
                total_amount=Decimal(str(100.0 * i)),
                status="pending" if i % 5 == 0 else "completed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            for i in range(1, min(limit + 1, 101))
        ]

        logger.info("Orders fetched successfully", count=len(orders))
        return orders

    async def get_deliveries(self) -> list:
        """This method is not used in PostgresRepository."""
        return []
