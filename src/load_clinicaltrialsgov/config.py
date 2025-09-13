from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):  # type: ignore[misc]
    model_config = SettingsConfigDict(env_prefix="API_")
    timeout: int = Field(30, description="Timeout for API requests in seconds.")
    max_retries: int = Field(
        5, description="Maximum number of retries for failed API requests."
    )
    backoff_factor: float = Field(0.5, description="Backoff factor for retries.")


class DatabaseSettings(BaseSettings):  # type: ignore[misc]
    model_config = SettingsConfigDict(env_prefix="DB_")
    dsn: str = Field(
        "postgresql://user:password@host:port/database",
        description="Database connection string.",
    )


class ETLSettings(BaseSettings):  # type: ignore[misc]
    model_config = SettingsConfigDict(env_prefix="ETL_")
    batch_size: int = Field(
        1000, description="Number of records to process in a single batch."
    )


class Settings(BaseSettings):  # type: ignore[misc]
    """
    Main settings for the application.
    """

    api: APISettings = APISettings()
    db: DatabaseSettings = DatabaseSettings()
    etl: ETLSettings = ETLSettings()
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_nested_delimiter="__")


settings = Settings()
