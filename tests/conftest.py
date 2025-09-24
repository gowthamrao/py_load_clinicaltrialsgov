import pytest
import os
import time
from testcontainers.postgres import PostgresContainer
from load_clinicaltrialsgov.config import settings
from alembic.config import Config
from alembic import command
from typing import Generator, cast
from load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    image_name = "bitnami/postgresql:15"
    with PostgresContainer(image_name, driver=None) as container:
        time.sleep(5)
        original_dsn = settings.db.dsn

        # The plain DSN for the application
        app_dsn = container.get_connection_url()
        # The DSN with the correct dialect for Alembic/SQLAlchemy
        alembic_dsn = app_dsn.replace("postgresql://", "postgresql+psycopg://")

        # Set DSN for Alembic and run migrations
        settings.db.dsn = alembic_dsn
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", alembic_dsn)
        command.upgrade(alembic_cfg, "head")

        # Set DSN for the application to use
        settings.db.dsn = app_dsn
        yield container

        # Restore original DSN after tests
        settings.db.dsn = original_dsn


@pytest.fixture(scope="session")
def postgres_url(postgres_container: PostgresContainer) -> str:
    return cast(str, postgres_container.get_connection_url()).replace(
        "postgresql://", "postgresql+psycopg://"
    )


@pytest.fixture(scope="session")
def test_data_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "integration")


@pytest.fixture(scope="session")
def db_connector(
    postgres_container: "PostgresContainer",
) -> DatabaseConnectorInterface:
    from load_clinicaltrialsgov.connectors.postgres import PostgresConnector

    # The DSN is already set correctly by the postgres_container fixture
    connector = PostgresConnector()
    return connector
