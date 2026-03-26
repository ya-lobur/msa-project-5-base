"""Configuration management using pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database configuration
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "shipments_db"
    db_user: str = "postgres"
    db_password: str = "postgres"

    # Export configuration
    batch_size: int = 1000
    output_dir: str = "/data"

    # Logging
    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection string."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


# Global settings instance
settings = Settings()
