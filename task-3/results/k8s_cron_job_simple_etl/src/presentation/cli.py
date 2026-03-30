"""CLI entry point for the ETL application."""

import asyncio
import logging
import sys
from datetime import datetime

from config.settings import settings
from src.application.services import ExportService
from src.infrastructure.database import PostgresShipmentRepository
from src.infrastructure.storage import CSVStorageWriter


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


async def main():
    """Main entry point for the ETL job."""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("Starting Shipment ETL Job")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Database: {settings.db_host}:{settings.db_port}/{settings.db_name}")
    logger.info(f"Output directory: {settings.output_dir}")
    logger.info(f"Batch size: {settings.batch_size}")
    logger.info("=" * 60)

    try:
        # Initialize dependencies
        repository = PostgresShipmentRepository(settings.database_url)
        storage = CSVStorageWriter(settings.output_dir)
        service = ExportService(repository, storage, settings.batch_size)

        # Execute export
        start_time = datetime.now()
        record_count, output_file = await service.export_shipments()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("ETL Job Completed Successfully")
        logger.info(f"Records exported: {record_count}")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error("=" * 60)
        logger.error("ETL Job Failed")
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("=" * 60)
        return 1


def run():
    """Synchronous wrapper for async main function."""
    exit_code = asyncio.run(main())
    sys.exit(exit_code)


if __name__ == "__main__":
    run()
