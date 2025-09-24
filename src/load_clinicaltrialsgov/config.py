# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_")
    base_url: str = Field(
        "https://clinicaltrials.gov/api/v2", description="base url for the api"
    )
    timeout: int = Field(30, description="Timeout for API requests in seconds.")
    max_retries: int = Field(
        5, description="Maximum number of retries for failed API requests."
    )
    backoff_factor: float = Field(0.5, description="Backoff factor for retries.")


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_")
    dsn: str = Field(
        "postgresql://user:password@host:port/database",
        description="Database connection string.",
    )


class ETLSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ETL_")
    batch_size: int = Field(
        1000, description="Number of records to process in a single batch."
    )


class Settings(BaseSettings):
    """
    Main settings for the application.
    """

    api: APISettings = APISettings()
    db: DatabaseSettings = DatabaseSettings()
    etl: ETLSettings = ETLSettings()
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_nested_delimiter="__")


settings = Settings()
