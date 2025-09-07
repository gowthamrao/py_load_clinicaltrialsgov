import pytest
from unittest.mock import MagicMock, patch
from typer.testing import CliRunner

from py_load_clinicaltrialsgov.cli import app
from py_load_clinicaltrialsgov.models.api_models import Study

# Create a runner for testing the Typer app
runner = CliRunner()

@pytest.fixture
def mock_connector():
    """Fixture for a mocked DatabaseConnectorInterface."""
    connector = MagicMock()
    connector.get_last_successful_load_timestamp.return_value = None
    return connector

@pytest.fixture
def mock_api_client():
    """Fixture for a mocked APIClient."""
    return MagicMock()

@pytest.fixture
def mock_transformer():
    """Fixture for a mocked Transformer."""
    return MagicMock()

def test_run_command_sends_failed_study_to_dlq(
    mock_connector, mock_api_client, mock_transformer
):
    """
    Verify that when study transformation fails, the raw study data is sent
    to the dead-letter queue via the connector.
    """
    # Arrange
    # A dummy study that will be "returned" by the API client
    failed_study_payload = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345678"}
        },
        "derivedSection": {},
        "hasResults": False,
    }
    failed_study = Study.model_validate(failed_study_payload)
    nct_id = "NCT12345678"
    error_message = "Something went horribly wrong"

    # Configure mocks
    mock_api_client.get_all_studies.return_value = iter([failed_study])
    mock_transformer.transform_study.side_effect = Exception(error_message)

    with patch("py_load_clinicaltrialsgov.cli.get_connector", return_value=mock_connector), \
         patch("py_load_clinicaltrialsgov.cli.APIClient", return_value=mock_api_client), \
         patch("py_load_clinicaltrialsgov.cli.Transformer", return_value=mock_transformer):

        # Act
        result = runner.invoke(app, ["run", "--load-type", "full"])

        # Assert
        assert result.exit_code == 0 # The overall process should not fail on a single record

        # Verify the DLQ method was called correctly
        mock_connector.record_failed_study.assert_called_once()
        call_args, call_kwargs = mock_connector.record_failed_study.call_args

        assert call_kwargs.get("nct_id") == nct_id
        assert call_kwargs.get("payload") == failed_study.model_dump()
        assert call_kwargs.get("error_message") == error_message

        # Verify transaction was still committed (or rolled back depending on final state)
        # In this case, since the main loop continues, it should try to commit.
        mock_connector.manage_transaction.assert_any_call("begin")
        mock_connector.manage_transaction.assert_any_call("commit")
        mock_connector.record_load_history.assert_any_call("SUCCESS", {"records_processed": 0})
