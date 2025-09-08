import pytest
import json
from unittest.mock import MagicMock, create_autospec

from py_load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from py_load_clinicaltrialsgov.extractor.api_client import APIClient
from py_load_clinicaltrialsgov.transformer.transformer import Transformer
from py_load_clinicaltrialsgov.orchestrator import Orchestrator

# Import fixtures from the other test file
from .test_full_etl import postgres_container, db_connector


# A valid study record that should process successfully
VALID_STUDY_PAYLOAD = {
    "protocolSection": {
        "identificationModule": {"nctId": "NCT000001", "briefTitle": "Valid Study"},
        "statusModule": {
            "overallStatus": "COMPLETED",
            "lastUpdatePostDateStruct": {"date": "2024-01-01"},
        },
        "conditionsModule": {"conditions": ["Condition 1"]},
    },
    "derivedSection": {
        "conditionBrowseModule": {},
        "interventionBrowseModule": {},
        "miscInfoModule": {},
    },
    "hasResults": False,
}

# An invalid study record that should fail Pydantic validation
# 'identificationModule' is missing the required 'nctId' field
INVALID_STUDY_PAYLOAD = {
    "protocolSection": {
        "identificationModule": {"briefTitle": "Invalid Study"},
        "statusModule": {"overallStatus": "UNKNOWN"},
    },
    "derivedSection": {
        "conditionBrowseModule": {},
        "interventionBrowseModule": {},
        "miscInfoModule": {},
    },
    "hasResults": False,
}

# A study that is valid but has a different NCT ID
# to test failed record identification
INVALID_STUDY_PAYLOAD_WITH_ID = {
    "protocolSection": {
        "identificationModule": {
            "nctId": "NCT000002",
            "briefTitle": "Invalid Study with ID",
        },
        # Missing required 'statusModule'
    },
    "derivedSection": {
        "conditionBrowseModule": {},
        "interventionBrowseModule": {},
        "miscInfoModule": {},
    },
    "hasResults": False,
}


@pytest.fixture
def mock_api_client() -> MagicMock:
    """Mocks the APIClient to yield predefined study data."""
    mock_client = create_autospec(APIClient)
    mock_client.get_all_studies.return_value = iter([
        VALID_STUDY_PAYLOAD,
        INVALID_STUDY_PAYLOAD,
        INVALID_STUDY_PAYLOAD_WITH_ID,
    ])
    return mock_client


def test_orchestrator_full_run(
    db_connector: PostgresConnector, mock_api_client: MagicMock
) -> None:
    """
    Tests the full end-to-end ETL flow using the orchestrator,
    including handling of valid and invalid records.
    """
    # Arrange
    transformer = Transformer()
    orchestrator = Orchestrator(
        connector=db_connector, api_client=mock_api_client, transformer=transformer
    )

    # Act
    orchestrator.run_etl(load_type="full")

    # Assert
    with db_connector.conn.cursor() as cur:
        # 1. Check for the successfully processed study
        cur.execute("SELECT COUNT(*) FROM studies")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT000001'")
        assert cur.fetchone()[0] == "Valid Study"
        cur.execute("SELECT COUNT(*) FROM conditions WHERE nct_id = 'NCT000001'")
        assert cur.fetchone()[0] == 1

        # 2. Check that the raw payload for the SUCCESSFUL study was stored
        cur.execute("SELECT COUNT(*) FROM raw_studies")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT payload FROM raw_studies WHERE nct_id = 'NCT000001'")
        raw_payload = cur.fetchone()[0]
        assert raw_payload["protocolSection"]["identificationModule"]["nctId"] == "NCT000001"

        # 3. Check for the quarantined records in the dead-letter queue
        cur.execute("SELECT COUNT(*) FROM dead_letter_queue")
        assert cur.fetchone()[0] == 2

        # 4. Check the record that failed due to missing NCT ID
        cur.execute("SELECT nct_id, error_message FROM dead_letter_queue WHERE nct_id IS NULL")
        result_no_id = cur.fetchone()
        assert result_no_id is not None
        assert "Transformation Error" in result_no_id[1]

        # 5. Check the record that failed due to other missing fields but had an ID
        cur.execute("SELECT payload, error_message FROM dead_letter_queue WHERE nct_id = 'NCT000002'")
        result_with_id = cur.fetchone()
        assert result_with_id is not None
        assert result_with_id[0]["protocolSection"]["identificationModule"]["nctId"] == "NCT000002"
        assert "Pydantic Validation Error" in result_with_id[1]

        # 6. Check load history
        cur.execute("SELECT status, metrics FROM load_history WHERE status = 'SUCCESS'")
        history = cur.fetchone()
        assert history is not None
        # Should be 1 record processed successfully, not 3
        assert history[1]["records_processed"] == 1
        assert history[1]["records_loaded_per_table"]["studies"] == 1
        assert history[1]["records_loaded_per_table"]["raw_studies"] == 1
        assert history[1]["records_loaded_per_table"]["conditions"] == 1
