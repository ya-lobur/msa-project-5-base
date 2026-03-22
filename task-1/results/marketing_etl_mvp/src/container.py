from dependency_injector import containers, providers

from src.application.use_cases import ProcessCustomerDataUseCase
from src.infrastructure.cfg import Settings
from src.infrastructure.repositories import CSVRepository, PostgresRepository
from src.infrastructure.services import CombinedDataSource, EmailNotificationService


class Container(containers.DeclarativeContainer):
    """Dependency Injection Container."""

    # Configuration
    config = providers.Singleton(Settings)

    # Repositories
    postgres_repository = providers.Factory(
        PostgresRepository,
        database_url=config.provided.database_url,
    )

    csv_repository = providers.Factory(
        CSVRepository,
        file_path=config.provided.csv_file_path,
    )

    # Services
    combined_data_source = providers.Factory(
        CombinedDataSource,
        postgres_repo=postgres_repository,
        csv_repo=csv_repository,
    )

    email_notification_service = providers.Factory(
        EmailNotificationService,
        smtp_host=config.provided.smtp_host,
        smtp_port=config.provided.smtp_port,
        from_email=config.provided.from_email,
        to_email=config.provided.to_email,
    )

    # Use Cases
    process_customer_data_use_case = providers.Factory(
        ProcessCustomerDataUseCase,
        data_source=combined_data_source,
        notification_service=email_notification_service,
    )
