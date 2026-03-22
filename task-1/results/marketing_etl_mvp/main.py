"""
Marketing ETL MVP - Main Entry Point

This file is primarily for local development/testing.
In production, Airflow will execute the DAG directly.
"""

import asyncio

import structlog

from src.container import Container

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)

logger = structlog.get_logger()


async def main():
    """Main entry point for local testing."""
    logger.info("Marketing ETL MVP - Starting local test run")

    # Initialize DI container
    container = Container()

    # Get the use case
    use_case = container.process_customer_data_use_case()

    # Execute the use case
    try:
        result = await use_case.execute()
        logger.info("Execution completed successfully", result=result)
        return result
    except Exception as e:
        logger.error("Execution failed", error=str(e), exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
