import structlog

from src.domain.interfaces import DataSourceInterface, NotificationInterface

logger = structlog.get_logger()


class ProcessCustomerDataUseCase:
    """Use case for processing customer data from multiple sources."""

    def __init__(
        self,
        data_source: DataSourceInterface,
        notification_service: NotificationInterface,
    ):
        self.data_source = data_source
        self.notification_service = notification_service

    async def execute(self) -> dict:
        """
        Execute the customer data processing pipeline.

        Returns:
            dict: Processing results with statistics
        """
        logger.info("Starting customer data processing")

        try:
            # Fetch data from sources
            customers = await self.data_source.get_customers(limit=1000)
            orders = await self.data_source.get_orders(limit=1000)
            deliveries = await self.data_source.get_deliveries()

            logger.info(
                "Data fetched successfully",
                customers_count=len(customers),
                orders_count=len(orders),
                deliveries_count=len(deliveries),
            )

            # Process data (stub - just count)
            active_customers = [c for c in customers if c.is_active]
            pending_orders = [o for o in orders if o.status == "pending"]

            results = {
                "total_customers": len(customers),
                "active_customers": len(active_customers),
                "total_orders": len(orders),
                "pending_orders": len(pending_orders),
                "total_deliveries": len(deliveries),
            }

            logger.info("Data processing completed", results=results)

            # Send success notification
            await self.notification_service.send_success_notification(
                f"Processing completed. Processed {len(customers)} customers, {len(orders)} orders"
            )

            return results

        except Exception as e:
            logger.error("Error processing customer data", error=str(e), exc_info=True)
            await self.notification_service.send_failure_notification("Data processing failed", e)
            raise
