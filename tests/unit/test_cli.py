import pytest
from unittest.mock import MagicMock, patch
from typer.testing import CliRunner
from typing import Any

from load_clinicaltrialsgov.cli import app

# Create a runner for testing the Typer app
runner = CliRunner()


@pytest.fixture
def mock_connector() -> MagicMock:
    """Fixture for a mocked DatabaseConnectorInterface."""
    connector = MagicMock()
    connector.get_last_successful_load_timestamp.return_value = None
    return connector


@pytest.fixture
def mock_api_client() -> MagicMock:
    """Fixture for a mocked APIClient."""
    return MagicMock()


@pytest.fixture
def mock_transformer() -> MagicMock:
    """Fixture for a mocked Transformer."""
    return MagicMock()


def test_run_command_sends_invalid_study_to_dlq(
    mock_connector: MagicMock,
    mock_api_client: MagicMock,
) -> None:
    """
    Verify that when a study fails Pydantic validation, the raw study data is
    sent to the dead-letter queue and the ETL continues.
    """
    # Arrange
    # This payload is invalid because it's missing the 'identificationModule'
    invalid_study_payload: dict[str, Any] = {
        "protocolSection": {"statusModule": {}}
    }

    # Configure mocks
    mock_api_client.get_all_studies.return_value = iter([invalid_study_payload])
    # No need to mock the transformer, as it won't be called for this record.
    mock_transformer = MagicMock()

    with (
        patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector),
        patch("load_clinicaltrialsgov.cli.APIClient", return_value=mock_api_client),
        patch("load_clinicaltrialsgov.cli.Transformer", return_value=mock_transformer),
    ):
        # Act
        result = runner.invoke(app, ["run", "--load-type", "full"])

        # Assert
        assert result.exit_code == 0, (
            "The overall process should not fail on a single validation error"
        )

        # Verify the DLQ method was called correctly
        mock_connector.record_failed_study.assert_called_once()
        call_args, call_kwargs = mock_connector.record_failed_study.call_args

        # NCT ID is None because it couldn't be parsed from the invalid payload
        assert call_kwargs.get("nct_id") is None
        assert call_kwargs.get("payload") == invalid_study_payload
        assert "Pydantic Validation Error" in call_kwargs.get("error_message")

        # Verify the transformer was never called
        mock_transformer.transform_study.assert_not_called()

        # Verify transaction was still committed
        mock_connector.manage_transaction.assert_any_call("begin")
        mock_connector.manage_transaction.assert_any_call("commit")

        # Verify that record_load_history was called with SUCCESS and 0 records processed
        rh_call_args, rh_call_kwargs = mock_connector.record_load_history.call_args
        assert rh_call_args[0] == "SUCCESS"
        assert isinstance(rh_call_args[1], dict)
        assert rh_call_args[1].get("records_processed") == 0


def test_status_command_healthy(mock_connector: MagicMock) -> None:
    """
    Verify the status command shows HEALTHY when the last run was a success.
    """
    # Arrange
    from datetime import datetime

    history_record = {
        "load_timestamp": datetime(2023, 1, 1, 12, 0, 0),
        "status": "SUCCESS",
        "metrics": {"records_processed": 100},
    }
    mock_connector.get_last_load_history.return_value = history_record

    with patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector):
        # Act
        result = runner.invoke(app, ["status"])

        # Assert
        assert result.exit_code == 0
        assert "ETL Status: HEALTHY" in result.stdout
        assert "Last Run Details:" in result.stdout
        assert "SUCCESS" in result.stdout


def test_status_command_failed_with_previous_success(mock_connector: MagicMock) -> None:
    """
    Verify status shows FAILED but includes the last successful run's details.
    """
    # Arrange
    from datetime import datetime

    failed_record = {
        "load_timestamp": datetime(2023, 1, 2, 12, 0, 0),
        "status": "FAILURE",
        "metrics": {"error": "connection timed out"},
    }
    successful_record: dict[str, Any] = {
        "load_timestamp": datetime(2023, 1, 1, 12, 0, 0),
        "status": "SUCCESS",
        "metrics": {"records_processed": 100},
    }
    mock_connector.get_last_load_history.return_value = failed_record
    mock_connector.get_last_successful_load_history.return_value = successful_record

    with patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector):
        # Act
        result = runner.invoke(app, ["status"])

        # Assert
        assert result.exit_code == 0
        assert "ETL Status: FAILED" in result.stdout
        assert "Failed Run Details:" in result.stdout
        assert "FAILURE" in result.stdout
        assert "Details of Last Successful Run:" in result.stdout
        assert str(successful_record["load_timestamp"].isoformat()) in result.stdout


def test_status_command_failed_with_no_previous_success(
    mock_connector: MagicMock,
) -> None:
    """
    Verify status shows FAILED and indicates no prior successful runs exist.
    """
    # Arrange
    from datetime import datetime

    failed_record = {
        "load_timestamp": datetime(2023, 1, 2, 12, 0, 0),
        "status": "FAILURE",
        "metrics": {"error": "connection timed out"},
    }
    mock_connector.get_last_load_history.return_value = failed_record
    mock_connector.get_last_successful_load_history.return_value = None

    with patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector):
        # Act
        result = runner.invoke(app, ["status"])

        # Assert
        assert result.exit_code == 0
        assert "ETL Status: FAILED" in result.stdout
        assert "No prior successful runs were found." in result.stdout


def test_status_command_no_history(mock_connector: MagicMock) -> None:
    """
    Verify the status command handles the case where no history is found.
    """
    # Arrange
    mock_connector.get_last_load_history.return_value = None

    with patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector):
        # Act
        result = runner.invoke(app, ["status"])

        # Assert
        assert result.exit_code == 0
        assert "No ETL run history found." in result.stdout


@patch("load_clinicaltrialsgov.cli.command")
def test_migrate_db_command(mock_alembic_command: MagicMock) -> None:
    """
    Verify the migrate-db command calls alembic correctly.
    """
    # Act
    result = runner.invoke(app, ["migrate-db"])

    # Assert
    assert result.exit_code == 0
    mock_alembic_command.upgrade.assert_called_once()
    # Check that the second argument is 'head'
    assert mock_alembic_command.upgrade.call_args[0][1] == "head"
