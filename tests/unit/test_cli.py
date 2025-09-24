import sys
import subprocess
import pytest
from unittest.mock import MagicMock, patch
from typer.testing import CliRunner
from typing import Any
import pandas as pd
import httpx
from load_clinicaltrialsgov.models.api_models import Study
from pytest_structlog import StructuredLogCapture

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


def test_run_command_successful_full_load(
    mock_connector: MagicMock,
    mock_api_client: MagicMock,
    mock_transformer: MagicMock,
) -> None:
    """
    Verify a successful full ETL run.
    """
    # Arrange
    study_payload = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345678"},
            "statusModule": {"overallStatus": "COMPLETED"},
        }
    }
    # Use a real DataFrame
    df = pd.DataFrame([{"nct_id": "NCT12345678"}])
    transformed_data = {"studies": df}

    mock_api_client.get_all_studies.return_value = iter([study_payload])
    mock_transformer.transform_study.return_value = (
        None  # transform_study doesn't return anything
    )
    mock_transformer.get_dataframes.return_value = transformed_data

    with (
        patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector),
        patch("load_clinicaltrialsgov.cli.APIClient", return_value=mock_api_client),
        patch("load_clinicaltrialsgov.cli.Transformer", return_value=mock_transformer),
    ):
        # Act
        result = runner.invoke(app, ["run", "--load-type", "full"])

        # Assert
        assert result.exit_code == 0
        mock_api_client.get_all_studies.assert_called_once()

        # Verify that transform_study was called with a Study object
        mock_transformer.transform_study.assert_called_once()
        call_args, _ = mock_transformer.transform_study.call_args
        assert isinstance(call_args[0], Study)
        assert call_args[1] == study_payload

        mock_connector.bulk_load_staging.assert_called_once_with("studies", df)
        mock_connector.record_load_history.assert_called_once()
        assert mock_connector.record_load_history.call_args[0][0] == "SUCCESS"


def test_run_command_successful_delta_load(
    mock_connector: MagicMock,
    mock_api_client: MagicMock,
    mock_transformer: MagicMock,
) -> None:
    """
    Verify a successful delta ETL run.
    """
    # Arrange
    from datetime import datetime

    last_load_timestamp = datetime(2023, 1, 1, 0, 0, 0)
    mock_connector.get_last_successful_load_timestamp.return_value = last_load_timestamp

    study_payload = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345678"},
            "statusModule": {"overallStatus": "COMPLETED"},
        }
    }
    df = pd.DataFrame([{"nct_id": "NCT12345678"}])
    transformed_data = {"studies": df}

    mock_api_client.get_all_studies.return_value = iter([study_payload])
    mock_transformer.get_dataframes.return_value = transformed_data

    with (
        patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector),
        patch("load_clinicaltrialsgov.cli.APIClient", return_value=mock_api_client),
        patch("load_clinicaltrialsgov.cli.Transformer", return_value=mock_transformer),
    ):
        # Act
        result = runner.invoke(app, ["run", "--load-type", "delta"])

        # Assert
        assert result.exit_code == 0
        mock_api_client.get_all_studies.assert_called_once_with(
            updated_since=last_load_timestamp
        )
        mock_connector.record_load_history.assert_called_once()
        assert mock_connector.record_load_history.call_args[0][0] == "SUCCESS"


def test_run_command_api_error(
    mock_connector: MagicMock,
    mock_api_client: MagicMock,
    mock_transformer: MagicMock,
) -> None:
    """
    Verify that an API error during the ETL is handled gracefully.
    """
    # Arrange
    mock_api_client.get_all_studies.side_effect = httpx.RequestError(
        "API is down", request=MagicMock()
    )

    with (
        patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector),
        patch("load_clinicaltrialsgov.cli.APIClient", return_value=mock_api_client),
        patch("load_clinicaltrialsgov.cli.Transformer", return_value=mock_transformer),
    ):
        # Act
        result = runner.invoke(app, ["run"])

        # Assert
        assert result.exit_code == 0
        mock_connector.record_load_history.assert_called_once()
        assert mock_connector.record_load_history.call_args[0][0] == "FAILURE"
        assert (
            "API is down" in mock_connector.record_load_history.call_args[0][1]["error"]
        )


def test_run_command_with_connector_option() -> None:
    """
    Verify that the --connector option is passed to the get_connector function.
    """
    with patch("load_clinicaltrialsgov.cli.get_connector") as mock_get_connector:
        # To prevent the rest of the command from running, we can make the
        # mock throw an exception. We'll catch it to prevent test failure.
        mock_get_connector.side_effect = ValueError("stop execution")

        result = runner.invoke(app, ["run", "--connector-name", "my-test-connector"])

        # Assert that get_connector was called with the correct argument
        mock_get_connector.assert_called_once_with("my-test-connector")
        assert isinstance(result.exception, ValueError)
        assert str(result.exception) == "stop execution"


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
    invalid_study_payload: dict[str, Any] = {"protocolSection": {"statusModule": {}}}

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


def test_init_db_aborts_on_user_rejection(
    mock_connector: MagicMock, log: StructuredLogCapture
) -> None:
    """
    Verify that the init-db command aborts if the user does not confirm.
    """
    with patch("load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector):
        # Act
        # The 'input' argument simulates user typing 'n' and pressing Enter
        result = runner.invoke(app, ["init-db"], input="n\n")

        # Assert
        assert result.exit_code == 1  # typer.Abort() raises SystemExit(1)
        assert log.has("database_initialization_aborted", level="warning")
        # Verify that the destructive action was NOT called
        mock_connector._dangerously_drop_all_tables.assert_not_called()


def test_get_connector_unsupported():
    """
    Test that get_connector raises a ValueError for an unsupported connector.
    """
    with pytest.raises(ValueError):
        runner.invoke(
            app, ["run", "--connector-name", "unsupported"], catch_exceptions=False
        )


@patch("load_clinicaltrialsgov.cli.get_connector")
def test_init_db_exception(
    mock_get_connector: MagicMock, log: StructuredLogCapture
) -> None:
    """
    Test that init_db handles exceptions gracefully.
    """
    mock_get_connector.side_effect = Exception("Test exception")
    result = runner.invoke(app, ["init-db", "--force"])
    assert result.exit_code == 1
    assert log.has("failed_to_initialize_database", level="error")


@patch("load_clinicaltrialsgov.cli.get_connector")
def test_status_exception(
    mock_get_connector: MagicMock, log: StructuredLogCapture
) -> None:
    """
    Test that status handles exceptions gracefully.
    """
    mock_get_connector.side_effect = Exception("Test exception")
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 1
    assert log.has("failed_to_get_status", level="error")


def test_main_guard() -> None:
    """
    Test that the __main__ guard calls the app.
    """
    result = subprocess.run(
        [sys.executable, "-m", "load_clinicaltrialsgov.cli"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "Missing command" in result.stderr
