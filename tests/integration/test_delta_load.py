import pytest
from unittest.mock import MagicMock, create_autospec
from typing import cast
import datetime

from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.extractor.api_client import APIClient
from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.orchestrator import Orchestrator

# Fixtures are automatically discovered by pytest


# A study that was last updated before the delta load
OLD_STUDY_PAYLOAD = {
    "protocolSection": {
        "identificationModule": {"nctId": "NCT000001", "briefTitle": "Old Study"},
        "statusModule": {
            "overallStatus": "COMPLETED",
            "lastUpdatePostDateStruct": {"date": "2024-01-01", "type": "ACTUAL"},
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

# A study that was last updated after the delta load
NEW_STUDY_PAYLOAD = {
    "protocolSection": {
        "identificationModule": {"nctId": "NCT000002", "briefTitle": "New Study"},
        "statusModule": {
            "overallStatus": "COMPLETED",
            "lastUpdatePostDateStruct": {"date": "2024-01-03", "type": "ACTUAL"},
        },
        "conditionsModule": {"conditions": ["Condition 2"]},
    },
    "derivedSection": {
        "conditionBrowseModule": {},
        "interventionBrowseModule": {},
        "miscInfoModule": {},
    },
    "hasResults": False,
}

# An updated version of the old study
UPDATED_STUDY_PAYLOAD = {
    "protocolSection": {
        "identificationModule": {"nctId": "NCT000001", "briefTitle": "Updated Study"},
        "statusModule": {
            "overallStatus": "COMPLETED",
            "lastUpdatePostDateStruct": {"date": "2024-01-04", "type": "ACTUAL"},
        },
        "conditionsModule": {"conditions": ["Condition 1", "Condition 3"]},
    },
    "derivedSection": {
        "conditionBrowseModule": {},
        "interventionBrowseModule": {},
        "miscInfoModule": {},
    },
    "hasResults": False,
}


@pytest.fixture
def mock_api_client_full_load() -> MagicMock:
    """Mocks the APIClient to yield predefined study data for the full load."""
    mock_client = create_autospec(APIClient)
    mock_client.get_all_studies.return_value = iter(
        [
            OLD_STUDY_PAYLOAD,
        ]
    )
    return cast(MagicMock, mock_client)


@pytest.fixture
def mock_api_client_delta_load() -> MagicMock:
    """Mocks the APIClient to yield predefined study data for the delta load."""
    mock_client = create_autospec(APIClient)
    mock_client.get_all_studies.return_value = iter(
        [
            NEW_STUDY_PAYLOAD,
            UPDATED_STUDY_PAYLOAD,
        ]
    )
    return cast(MagicMock, mock_client)


@pytest.mark.integration
def test_orchestrator_delta_load(
    db_connector: PostgresConnector,
    mock_api_client_full_load: MagicMock,
    mock_api_client_delta_load: MagicMock,
) -> None:
    """
    Tests the delta load functionality of the orchestrator.
    """
    # Arrange - Full Load
    transformer_full = Transformer()
    orchestrator_full = Orchestrator(
        connector=db_connector,
        api_client=mock_api_client_full_load,
        transformer=transformer_full,
    )

    # Act - Full Load
    orchestrator_full.run_etl(load_type="full")

    # Assert - Full Load
    with db_connector.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM studies")
        studies_count = cur.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 1

        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT000001'")
        title_result = cur.fetchone()
        assert title_result is not None
        assert title_result[0] == "Old Study"

    # Arrange - Delta Load
    # Manually insert a record into the load_history to simulate a previous successful load
    # The orchestrator should pick up on this timestamp and only fetch studies updated after it.
    with db_connector.conn.cursor() as cur:
        cur.execute(
            """
                INSERT INTO load_history (status, metrics, load_timestamp)
            VALUES (%s, %s, %s)
            """,
            (
                "SUCCESS",
                '{"records_processed": 1}',
                datetime.datetime(2024, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc),
            ),
        )
    db_connector.conn.commit()

    transformer_delta = Transformer()
    orchestrator_delta = Orchestrator(
        connector=db_connector,
        api_client=mock_api_client_delta_load,
        transformer=transformer_delta,
    )

    # Act - Delta Load
    orchestrator_delta.run_etl(load_type="delta")

    # Assert - Delta Load
    with db_connector.conn.cursor() as cur:
        # Check that there are now two studies in the database
        cur.execute("SELECT COUNT(*) FROM studies")
        studies_count = cur.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 2

        # Check that the old study has been updated
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT000001'")
        title_result = cur.fetchone()
        assert title_result is not None
        assert title_result[0] == "Updated Study"

        # Check that the new study has been added
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT000002'")
        title_result = cur.fetchone()
        assert title_result is not None
        assert title_result[0] == "New Study"

        # Check that the conditions for the updated study are correct
        cur.execute("SELECT COUNT(*) FROM conditions WHERE nct_id = 'NCT000001'")
        conditions_count = cur.fetchone()
        assert conditions_count is not None
        assert conditions_count[0] == 2
