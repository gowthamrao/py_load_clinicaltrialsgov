import pytest
import time
from testcontainers.postgres import PostgresContainer
import pandas as pd
from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from load_clinicaltrialsgov.config import settings

from alembic.config import Config
from alembic import command


from typing import Generator


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    # Use a public ECR mirror to avoid Docker Hub rate limits in CI
    image_name = "public.ecr.aws/bitnami/postgresql:latest"
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
        command.upgrade(alembic_cfg, "head")

        # Set DSN for the application to use
        settings.db.dsn = app_dsn
        yield container

        # Restore original DSN after tests
        settings.db.dsn = original_dsn


@pytest.fixture(scope="module")
def db_connector(
    postgres_container: PostgresContainer,
) -> DatabaseConnectorInterface:
    # The DSN is already set correctly by the postgres_container fixture
    connector = PostgresConnector()
    return connector


def test_full_etl_flow(db_connector: DatabaseConnectorInterface) -> None:
    """
    Tests the full ETL flow for a single study with child records,
    including the "delete then insert" logic for child tables.
    """
    from typing import cast

    # Cast the connector to the concrete class to access the `conn` attribute
    # This is safe because we know the fixture provides a PostgresConnector
    pg_connector = cast(PostgresConnector, db_connector)

    # 1. Initial Load
    studies_columns = [
        "nct_id",
        "brief_title",
        "official_title",
        "overall_status",
        "start_date",
        "start_date_str",
        "primary_completion_date",
        "primary_completion_date_str",
        "study_type",
        "brief_summary",
    ]
    studies_df = pd.DataFrame(
        [
            (
                "NCT00000123",
                "Initial Title",
                None,
                "COMPLETED",
                None,
                None,
                None,
                None,
                "Observational",
                None,
            )
        ],
        columns=studies_columns,
    )
    interventions_df = pd.DataFrame(
        [
            {
                "nct_id": "NCT00000123",
                "intervention_type": "DRUG",
                "name": "Aspirin",
                "description": "Low dose aspirin",
            },
            {
                "nct_id": "NCT00000123",
                "intervention_type": "DRUG",
                "name": "Placebo",
                "description": "Sugar pill",
            },
        ]
    )
    outcomes_df = pd.DataFrame(
        [
            {
                "nct_id": "NCT00000123",
                "outcome_type": "PRIMARY",
                "measure": "Heart Attack",
                "time_frame": "1 year",
                "description": "Primary outcome",
            }
        ]
    )

    # Load initial data
    db_connector.bulk_load_staging("studies", studies_df)
    db_connector.execute_merge("studies", primary_keys=["nct_id"])
    db_connector.bulk_load_staging("interventions", interventions_df)
    db_connector.execute_merge(
        "interventions", primary_keys=["nct_id", "intervention_type", "name"]
    )
    db_connector.bulk_load_staging("design_outcomes", outcomes_df)
    db_connector.execute_merge(
        "design_outcomes", primary_keys=["nct_id", "outcome_type", "measure"]
    )

    # Verify initial load
    with pg_connector.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT COUNT(*) FROM design_outcomes WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT name FROM interventions WHERE nct_id = 'NCT00000123' AND name = 'Aspirin'"
        )
        assert cur.fetchone() is not None

    # 2. Second Load (Delta update for the same study)
    # The title is updated, and the interventions have changed completely.
    updated_studies_df = pd.DataFrame(
        [
            (
                "NCT00000123",
                "Updated Title",
                None,
                "COMPLETED",
                None,
                None,
                None,
                None,
                "Observational",
                None,
            )
        ],
        columns=studies_columns,
    )
    updated_interventions_df = pd.DataFrame(
        [
            {
                "nct_id": "NCT00000123",
                "intervention_type": "DEVICE",
                "name": "Stent",
                "description": "A new device",
            }
        ]
    )

    # Load updated data
    db_connector.bulk_load_staging("studies", updated_studies_df)
    db_connector.execute_merge("studies", primary_keys=["nct_id"])
    db_connector.bulk_load_staging("interventions", updated_interventions_df)
    db_connector.execute_merge(
        "interventions", primary_keys=["nct_id", "intervention_type", "name"]
    )

    # Verify the update
    with pg_connector.conn.cursor() as cur:
        # Check that the study was updated (UPSERT)
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == "Updated Title"
        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1

        # Check that interventions were replaced (DELETE then INSERT)
        cur.execute("SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT name FROM interventions WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == "Stent"
        cur.execute(
            "SELECT name FROM interventions WHERE nct_id = 'NCT00000123' AND name = 'Aspirin'"
        )
        assert cur.fetchone() is None  # The old intervention should be gone
