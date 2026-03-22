from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    # Database settings
    db_host: str = "postgres"
    db_port: int = 5432
    db_name: str = "marketing_db"
    db_user: str = "postgres"
    db_password: str = "postgres"

    # CSV file path
    csv_file_path: str = "/opt/airflow/data/deliveries.csv"

    # Email settings
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    from_email: str = "airflow@example.com"
    to_email: str = "marketing@example.com"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    @property
    def database_url(self) -> str:
        """Get database connection URL."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
