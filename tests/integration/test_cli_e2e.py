import pytest
from typer.testing import CliRunner
from load_clinicaltrialsgov.cli import app
from testcontainers.postgres import PostgresContainer
from load_clinicaltrialsgov.config import settings
from alembic.config import Config
from alembic import command
import json
from unittest.mock import patch, MagicMock
import psycopg

runner = CliRunner()

import time
from typing import Generator

@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    # Use a public ECR mirror to avoid Docker Hub rate limits in CI
    image_name = "public.ecr.aws/bitnami/postgresql:15"
    with PostgresContainer(image_name, driver=None) as container:
        time.sleep(5)
        yield container


@pytest.fixture(scope="module", autouse=True)
def setup_database(postgres_container: PostgresContainer):
    original_dsn = settings.db.dsn

    # The plain DSN for the application
    app_dsn = postgres_container.get_connection_url()
    # The DSN with the correct dialect for Alembic/SQLAlchemy
    alembic_dsn = app_dsn.replace("postgresql://", "postgresql+psycopg://")

    # Set DSN for Alembic and run migrations
    settings.db.dsn = alembic_dsn
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")

    # Set DSN for the application to use for the test
    settings.db.dsn = app_dsn
    yield
    # Restore original DSN after tests
    settings.db.dsn = original_dsn


def test_cli_run_e2e():
    # Arrange
    with open("tests/integration/NCT04267848.json") as f:
        study_payload = json.load(f)

    mock_api_client = MagicMock()
    mock_api_client.get_all_studies.return_value = iter([study_payload])

    # Act
    with patch("load_clinicaltrialsgov.cli.APIClient", return_value=mock_api_client):
        result = runner.invoke(app, ["run", "--load-type", "full", "--connector-name", "postgres"])

    # Assert
    assert result.exit_code == 0
    assert '"event": "etl_process_completed_successfully"' in result.stdout
    assert '"records_processed": 1' in result.stdout

    # Verify data in the database
    with psycopg.connect(settings.db.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT04267848'")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM sponsors WHERE nct_id = 'NCT04267848'")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM conditions WHERE nct_id = 'NCT04267848'")
            assert cur.fetchone()[0] == 6

            cur.execute("SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT04267848'")
            assert cur.fetchone()[0] == 12

            cur.execute("SELECT COUNT(*) FROM design_outcomes WHERE nct_id = 'NCT04267848'")
            assert cur.fetchone()[0] == 8
