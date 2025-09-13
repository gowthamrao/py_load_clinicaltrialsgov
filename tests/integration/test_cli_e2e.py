from typer.testing import CliRunner
from load_clinicaltrialsgov.config import settings
import json
from unittest.mock import patch, MagicMock
import psycopg

runner = CliRunner()


def test_cli_run_e2e(postgres_url, monkeypatch):
    monkeypatch.setenv("DB_DSN", postgres_url)
    from load_clinicaltrialsgov.cli import app
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

            cur.execute("SELECT COUNT(*) FROM intervention_arm_groups WHERE nct_id = 'NCT04267848'")
            assert cur.fetchone()[0] > 0
