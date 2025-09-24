# mypy: ignore-errors
import pytest
from sqlalchemy import create_engine, inspect
from load_clinicaltrialsgov.cli import app
from typer.testing import CliRunner
import os
import json
from unittest.mock import patch, create_autospec
from load_clinicaltrialsgov.extractor.api_client import APIClient


from unittest.mock import MagicMock


@pytest.mark.integration
@patch("load_clinicaltrialsgov.orchestrator.APIClient")  # type: ignore[misc]
def test_database_schema_validation(
    mock_api_client: MagicMock, postgres_url: str, test_data_dir: str
) -> None:
    """
    Test that the database schema is created correctly after a full ETL run.
    """
    # Load the mock data
    with open(os.path.join(test_data_dir, "NCT04267848.json")) as f:
        mock_study_data = json.load(f)

    # Configure the mock API client
    mock_client_instance = create_autospec(APIClient)
    mock_client_instance.get_all_studies.return_value = iter([mock_study_data])
    mock_api_client.return_value = mock_client_instance

    runner = CliRunner()
    # Run the ETL to populate the database
    result = runner.invoke(
        app,
        [
            "run",
            "--load-type",
            "full",
            "--connector-name",
            "postgres",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"CLI command failed: {result.output}"

    engine = create_engine(postgres_url)
    inspector = inspect(engine)
    with engine.connect() as _:
        # Check that all expected tables are created
        expected_tables = [
            "raw_studies",
            "studies",
            "sponsors",
            "conditions",
            "interventions",
            "intervention_arm_groups",
            "design_outcomes",
        ]
        tables = inspector.get_table_names()
        for table in expected_tables:
            assert table in tables

        # Check the columns and types for a few key tables
        # studies table
        studies_columns = {
            c["name"]: c["type"].__class__.__name__
            for c in inspector.get_columns("studies")
        }
        assert "nct_id" in studies_columns and studies_columns["nct_id"] == "VARCHAR"
        assert (
            "brief_title" in studies_columns
            and studies_columns["brief_title"] == "TEXT"
        )
        assert (
            "start_date" in studies_columns and studies_columns["start_date"] == "DATE"
        )
        assert (
            "primary_completion_date" in studies_columns
            and studies_columns["primary_completion_date"] == "DATE"
        )

        # sponsors table
        sponsors_columns = {
            c["name"]: c["type"].__class__.__name__
            for c in inspector.get_columns("sponsors")
        }
        assert "nct_id" in sponsors_columns and sponsors_columns["nct_id"] == "VARCHAR"
        assert "name" in sponsors_columns and sponsors_columns["name"] == "TEXT"
        assert (
            "agency_class" in sponsors_columns
            and sponsors_columns["agency_class"] == "VARCHAR"
        )
        assert (
            "is_lead" in sponsors_columns and sponsors_columns["is_lead"] == "BOOLEAN"
        )

        # Check foreign key constraints
        studies_fkeys = inspector.get_foreign_keys("studies")
        assert any(
            fk["referred_table"] == "raw_studies"
            and fk["referred_columns"] == ["nct_id"]
            for fk in studies_fkeys
        )

        sponsors_fkeys = inspector.get_foreign_keys("sponsors")
        assert any(
            fk["referred_table"] == "studies" and fk["referred_columns"] == ["nct_id"]
            for fk in sponsors_fkeys
        )
