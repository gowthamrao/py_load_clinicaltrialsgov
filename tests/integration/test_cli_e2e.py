from typer.testing import CliRunner
from pytest import MonkeyPatch
import pytest
from load_clinicaltrialsgov.config import settings
import json
from unittest.mock import patch, MagicMock
import psycopg
from pytest_structlog import StructuredLogCapture


runner = CliRunner()


@pytest.mark.integration
def test_cli_run_e2e(
    postgres_url: str, monkeypatch: MonkeyPatch, log: StructuredLogCapture
) -> None:
    monkeypatch.setenv("DB_DSN", postgres_url)
    from load_clinicaltrialsgov.cli import app

    # Arrange
    with open("tests/integration/NCT04267848.json") as f:
        study_payload = json.load(f)

    mock_api_client = MagicMock()
    mock_api_client.get_all_studies.return_value = iter([study_payload])

    # Act
    with patch("load_clinicaltrialsgov.cli.APIClient", return_value=mock_api_client):
        result = runner.invoke(
            app, ["run", "--load-type", "full", "--connector-name", "postgres"]
        )

    # Assert
    assert result.exit_code == 0
    assert log.has("etl_process_completed_successfully", level="info")

    # Find the successful event and check its metrics
    for event in log.events:
        if event["event"] == "etl_process_completed_successfully":
            assert event["metrics"]["records_processed"] == 1
            break
    else:
        assert False, "Could not find the successful event log"

    # Verify data in the database
    with psycopg.connect(settings.db.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT04267848'")
            studies_count = cur.fetchone()
            assert studies_count is not None
            assert studies_count[0] == 1

            cur.execute("SELECT COUNT(*) FROM sponsors WHERE nct_id = 'NCT04267848'")
            sponsors_count = cur.fetchone()
            assert sponsors_count is not None
            assert sponsors_count[0] == 1

            cur.execute("SELECT COUNT(*) FROM conditions WHERE nct_id = 'NCT04267848'")
            conditions_count = cur.fetchone()
            assert conditions_count is not None
            assert conditions_count[0] == 6

            cur.execute(
                "SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT04267848'"
            )
            interventions_count = cur.fetchone()
            assert interventions_count is not None
            assert interventions_count[0] == 12

            cur.execute(
                "SELECT COUNT(*) FROM design_outcomes WHERE nct_id = 'NCT04267848'"
            )
            design_outcomes_count = cur.fetchone()
            assert design_outcomes_count is not None
            assert design_outcomes_count[0] == 8

            cur.execute(
                "SELECT COUNT(*) FROM intervention_arm_groups WHERE nct_id = 'NCT04267848'"
            )
            intervention_arm_groups_count = cur.fetchone()
            assert intervention_arm_groups_count is not None
            assert intervention_arm_groups_count[0] > 0
