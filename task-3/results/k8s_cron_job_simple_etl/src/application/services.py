"""Application layer - business logic and use cases."""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Protocol

from src.domain.repositories import ShipmentRepository

logger = logging.getLogger(__name__)


class StorageWriter(Protocol):
    """Protocol for storage writer implementations."""

    async def write_csv(self, data: list[dict], filename: str) -> Path:
        """Write data to CSV file."""
        ...


class ExportService:
    """Service for exporting shipment data to CSV."""

    def __init__(self, repository: ShipmentRepository, storage: StorageWriter, batch_size: int = 1000):
        """
        Initialize export service.

        Args:
            repository: Repository for accessing shipment data
            storage: Storage writer for CSV output
            batch_size: Number of records to process per batch
        """
        self.repository = repository
        self.storage = storage
        self.batch_size = batch_size

    async def export_shipments(self) -> tuple[int, Path]:
        """
        Export all shipments to CSV file.

        Returns:
            Tuple of (record_count, output_file_path)
        """
        logger.info("Starting shipment export")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"shipments_{timestamp}.csv"

        total_records = 0
        all_data = []

        try:
            async for batch in self.repository.get_all(batch_size=self.batch_size):
                batch_data = [
                    {
                        "id": ship.id,
                        "client_id": ship.client_id,
                        "driver_id": ship.driver_id,
                        "vehicle_id": ship.vehicle_id,
                        "origin": ship.origin,
                        "destination": ship.destination,
                        "cargo_type": ship.cargo_type,
                        "cargo_weight": str(ship.cargo_weight),
                        "price": str(ship.price),
                        "status": ship.status,
                        "created_at": ship.created_at.isoformat(),
                        "updated_at": ship.updated_at.isoformat(),
                        "pickup_date": ship.pickup_date.isoformat() if ship.pickup_date else "",
                        "delivery_date": ship.delivery_date.isoformat() if ship.delivery_date else "",
                        "notes": ship.notes or "",
                    }
                    for ship in batch
                ]
                all_data.extend(batch_data)
                total_records += len(batch)
                logger.info(f"Processed {total_records} records")

            output_path = await self.storage.write_csv(all_data, filename)
            logger.info(f"Export completed: {total_records} records written to {output_path}")

            return total_records, output_path

        except Exception as e:
            logger.error(f"Export failed: {e}", exc_info=True)
            raise
        finally:
            await self.repository.close()
