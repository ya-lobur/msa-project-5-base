"""Database infrastructure implementation using asyncpg."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator

import asyncpg

from src.domain.entities import Shipment
from src.domain.repositories import ShipmentRepository

logger = logging.getLogger(__name__)


class PostgresShipmentRepository(ShipmentRepository):
    """PostgreSQL implementation of ShipmentRepository using asyncpg."""

    def __init__(self, connection_string: str):
        """
        Initialize repository with database connection string.

        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self.pool: asyncpg.Pool | None = None

    async def _ensure_pool(self) -> asyncpg.Pool:
        """Ensure connection pool is initialized."""
        if self.pool is None:
            logger.info("Creating database connection pool")
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=1,
                max_size=5,
                command_timeout=60,
            )
        return self.pool

    async def get_all(self, batch_size: int = 1000) -> AsyncIterator[list[Shipment]]:
        """
        Retrieve all shipments in batches using cursor.

        Args:
            batch_size: Number of records to fetch per batch

        Yields:
            Batches of shipment entities
        """
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            logger.info(f"Fetching shipments with batch size {batch_size}")

            # Use server-side cursor for efficient batch processing
            async with conn.transaction():
                cursor = await conn.cursor(
                    """
                    SELECT id, client_id, driver_id, vehicle_id, origin, destination,
                           cargo_type, cargo_weight, price, status, created_at, updated_at,
                           pickup_date, delivery_date, notes
                    FROM shipments
                    ORDER BY id
                    """
                )

                while True:
                    rows = await cursor.fetch(batch_size)
                    if not rows:
                        break

                    shipments = [
                        Shipment(
                            id=row["id"],
                            client_id=row["client_id"],
                            driver_id=row["driver_id"],
                            vehicle_id=row["vehicle_id"],
                            origin=row["origin"],
                            destination=row["destination"],
                            cargo_type=row["cargo_type"],
                            cargo_weight=Decimal(str(row["cargo_weight"])),
                            price=Decimal(str(row["price"])),
                            status=row["status"],
                            created_at=row["created_at"],
                            updated_at=row["updated_at"],
                            pickup_date=row["pickup_date"],
                            delivery_date=row["delivery_date"],
                            notes=row["notes"],
                        )
                        for row in rows
                    ]

                    yield shipments

    async def close(self) -> None:
        """Close connection pool and cleanup resources."""
        if self.pool:
            logger.info("Closing database connection pool")
            await self.pool.close()
            self.pool = None
