import pytest
import json
from testcontainers.postgres import PostgresContainer
from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from load_clinicaltrialsgov.config import settings
from load_clinicaltrialsgov.orchestrator import Orchestrator
from load_clinicaltrialsgov.extractor.api_client import APIClient
from load_clinicaltrialsgov.transformer.transformer import Transformer

from alembic.config import Config
from alembic import command


import time
from typing import Generator, cast
from unittest.mock import patch


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    # Use a public ECR mirror to avoid Docker Hub rate limits in CI
    image_name = "public.ecr.aws/bitnami/postgresql:15"
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


@pytest.fixture
def db_connector(
    postgres_container: PostgresContainer,
) -> Generator[DatabaseConnectorInterface, None, None]:
    # The DSN is already set correctly by the postgres_container fixture
    connector = PostgresConnector()
    yield connector
    connector.conn.close()


@pytest.fixture
def api_client() -> APIClient:
    return APIClient()


@pytest.fixture
def transformer() -> Transformer:
    return Transformer()


def test_full_etl_with_api_data(
    db_connector: DatabaseConnectorInterface,
    api_client: APIClient,
    transformer: Transformer,
) -> None:
    """
    Tests the full ETL flow using a real API response from a file.
    """
    # Load the test data from the JSON file
    with open("tests/integration/NCT04267848.json") as f:
        study_data = json.load(f)

    # Mock the API client to return the test data
    with patch(
        "load_clinicaltrialsgov.extractor.api_client.APIClient.get_all_studies",
        return_value=iter([study_data]),
    ):
        # Run the ETL
        orchestrator = Orchestrator(db_connector, api_client, transformer)
        orchestrator.run_etl(load_type="full")

    # Verify the data was loaded correctly
    pg_connector = cast(PostgresConnector, db_connector)
    with pg_connector.conn.cursor() as cur:
        # Check raw_studies
        cur.execute("SELECT COUNT(*) FROM raw_studies WHERE nct_id = 'NCT04267848'")
        count_result = cur.fetchone()
        assert count_result is not None
        assert count_result[0] == 1

        # Check studies
        cur.execute(
            "SELECT brief_title, study_type FROM studies WHERE nct_id = 'NCT04267848'"
        )
        study_result = cur.fetchone()
        assert study_result is not None
        assert "Immunotherapy" in study_result[0]
        assert study_result[1] == "INTERVENTIONAL"

        # Check sponsors
        cur.execute("SELECT name, is_lead FROM sponsors WHERE nct_id = 'NCT04267848'")
        sponsors = cur.fetchall()
        assert len(sponsors) > 0
        lead_sponsors = [s for s in sponsors if s[1]]
        assert len(lead_sponsors) == 1
        assert lead_sponsors[0][0] == "National Cancer Institute (NCI)"

        # Check conditions
        cur.execute("SELECT name FROM conditions WHERE nct_id = 'NCT04267848'")
        conditions = [row[0] for row in cur.fetchall()]
        assert "Lung Non-Small Cell Carcinoma" in conditions

        # Check interventions
        cur.execute(
            "SELECT name, intervention_type FROM interventions WHERE nct_id = 'NCT04267848'"
        )
        interventions = cur.fetchall()
        assert len(interventions) > 0
        intervention_names = [row[0] for row in interventions]
        assert "Pembrolizumab" in intervention_names

        # Check design_outcomes
        cur.execute(
            "SELECT measure, outcome_type FROM design_outcomes WHERE nct_id = 'NCT04267848'"
        )
        outcomes = cur.fetchall()
        assert len(outcomes) > 0
        primary_outcomes = [o for o in outcomes if o[1] == "PRIMARY"]
        assert len(primary_outcomes) > 0
        assert "Disease free survival" in primary_outcomes[0][0]
