# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


# ruff: noqa: F811
import pytest
from unittest.mock import MagicMock, create_autospec, patch
from typing import Any, cast, Generator

from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.extractor.api_client import APIClient
from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.orchestrator import Orchestrator

# Fixtures are automatically discovered by pytest


@pytest.fixture(autouse=True)
def run_around_tests(db_connector: PostgresConnector) -> Generator[None, None, None]:
    # Truncate all tables before each test
    with db_connector.conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE studies, sponsors, conditions, interventions, intervention_arm_groups, design_outcomes, raw_studies, dead_letter_queue, load_history RESTART IDENTITY"
        )
    db_connector.conn.commit()
    yield
    # No cleanup needed after


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
    mock_client.get_all_studies.return_value = iter(
        [
            VALID_STUDY_PAYLOAD,
            INVALID_STUDY_PAYLOAD,
            INVALID_STUDY_PAYLOAD_WITH_ID,
        ]
    )
    return cast(MagicMock, mock_client)


@pytest.mark.integration
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
        studies_count = cur.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 1

        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT000001'")
        title_result = cur.fetchone()
        assert title_result is not None
        assert title_result[0] == "Valid Study"

        cur.execute("SELECT COUNT(*) FROM conditions WHERE nct_id = 'NCT000001'")
        conditions_count = cur.fetchone()
        assert conditions_count is not None
        assert conditions_count[0] == 1

        # 2. Check that the raw payload for the SUCCESSFUL study was stored
        cur.execute("SELECT COUNT(*) FROM raw_studies")
        raw_studies_count = cur.fetchone()
        assert raw_studies_count is not None
        assert raw_studies_count[0] == 1

        cur.execute("SELECT payload FROM raw_studies WHERE nct_id = 'NCT000001'")
        raw_payload_result = cur.fetchone()
        assert raw_payload_result is not None
        raw_payload: dict[str, Any] = raw_payload_result[0]
        assert (
            raw_payload["protocolSection"]["identificationModule"]["nctId"]
            == "NCT000001"
        )

        # 3. Check for the quarantined records in the dead-letter queue
        cur.execute("SELECT COUNT(*) FROM dead_letter_queue")
        dead_letter_count = cur.fetchone()
        assert dead_letter_count is not None
        assert dead_letter_count[0] == 2

        # 4. Check the record that failed due to missing NCT ID
        cur.execute(
            "SELECT nct_id, error_message FROM dead_letter_queue WHERE nct_id IS NULL"
        )
        result_no_id = cur.fetchone()
        assert result_no_id is not None
        assert "Pydantic Validation Error" in result_no_id[1]

        # 5. Check the record that failed due to other missing fields but had an ID
        cur.execute(
            "SELECT payload, error_message FROM dead_letter_queue WHERE nct_id = 'NCT000002'"
        )
        result_with_id = cur.fetchone()
        assert result_with_id is not None
        assert (
            result_with_id[0]["protocolSection"]["identificationModule"]["nctId"]
            == "NCT000002"
        )
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


@pytest.mark.integration
def test_orchestrator_full_run_complex(
    db_connector: PostgresConnector,
) -> None:
    """
    Tests the full end-to-end ETL flow using the orchestrator with a complex payload.
    """
    import json

    # Arrange
    with open("tests/integration/NCT04267848_complex.json") as f:
        study_payload = json.load(f)

    mock_client = create_autospec(APIClient)
    mock_client.get_all_studies.return_value = iter([study_payload])
    transformer = Transformer()
    orchestrator = Orchestrator(
        connector=db_connector, api_client=mock_client, transformer=transformer
    )

    # Act
    orchestrator.run_etl(load_type="full")

    # Assert
    with db_connector.conn.cursor() as cur:
        # Check for the successfully processed study
        cur.execute("SELECT COUNT(*) FROM studies")
        studies_count = cur.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 1

        # Check for sponsors
        cur.execute("SELECT COUNT(*) FROM sponsors")
        sponsors_count = cur.fetchone()
        assert sponsors_count is not None
        assert sponsors_count[0] == 3

        # Check for interventions
        cur.execute("SELECT COUNT(*) FROM interventions")
        interventions_count = cur.fetchone()
        assert interventions_count is not None
        assert interventions_count[0] > 0

        # Check for outcomes
        cur.execute("SELECT COUNT(*) FROM design_outcomes")
        outcomes_count = cur.fetchone()
        assert outcomes_count is not None
        assert outcomes_count[0] > 0


@pytest.mark.integration
def test_orchestrator_delta_load(
    db_connector: PostgresConnector,
) -> None:
    """
    Tests the delta load functionality of the orchestrator.
    """
    # Arrange - First run
    initial_study = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT000003",
                "briefTitle": "Initial Study",
            },
            "statusModule": {
                "overallStatus": "COMPLETED",
                "lastUpdatePostDateStruct": {"date": "2024-01-01"},
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    mock_client_run1 = create_autospec(APIClient)
    mock_client_run1.get_all_studies.return_value = iter([initial_study])
    transformer1 = Transformer()
    orchestrator1 = Orchestrator(
        connector=db_connector, api_client=mock_client_run1, transformer=transformer1
    )

    # Act - First run
    orchestrator1.run_etl(load_type="full")

    # Assert - First run
    with db_connector.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM studies")
        studies_count = cur.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 1
        cur.execute("SELECT status, metrics FROM load_history")
        history = cur.fetchone()
        assert history is not None
        assert history[0] == "SUCCESS"
        assert history[1]["records_processed"] == 1

    # Arrange - Second run
    updated_study = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT000003",
                "briefTitle": "Updated Study",
            },
            "statusModule": {
                "overallStatus": "COMPLETED",
                "lastUpdatePostDateStruct": {"date": "2024-01-02"},
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    new_study = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT000004", "briefTitle": "New Study"},
            "statusModule": {
                "overallStatus": "RECRUITING",
                "lastUpdatePostDateStruct": {"date": "2024-01-03"},
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    mock_client_run2 = create_autospec(APIClient)
    mock_client_run2.get_all_studies.return_value = iter([updated_study, new_study])
    transformer2 = Transformer()
    orchestrator2 = Orchestrator(
        connector=db_connector, api_client=mock_client_run2, transformer=transformer2
    )

    # Act - Second run (delta load)
    orchestrator2.run_etl(load_type="delta")

    # Assert - Second run
    with db_connector.conn.cursor() as cur:
        # Check that there are now two studies in the table
        cur.execute("SELECT COUNT(*) FROM studies")
        studies_count = cur.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 2

        # Check that the first study was updated
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT000003'")
        study_title = cur.fetchone()
        assert study_title is not None
        assert study_title[0] == "Updated Study"

        # Check that the new study was inserted
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT000004'")
        new_study_title = cur.fetchone()
        assert new_study_title is not None
        assert new_study_title[0] == "New Study"

        # Check the load history for the second run
        cur.execute("SELECT status, metrics FROM load_history ORDER BY id DESC LIMIT 1")
        history = cur.fetchone()
        assert history is not None
        assert history[0] == "SUCCESS"
        assert history[1]["records_processed"] == 2


@pytest.mark.integration
def test_orchestrator_transformation_error(
    db_connector: PostgresConnector,
) -> None:
    """
    Tests that the orchestrator correctly handles a non-validation error
    during the transformation phase.
    """
    # Arrange
    valid_study = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT000005", "briefTitle": "Valid Study"},
            "statusModule": {
                "overallStatus": "COMPLETED",
                "lastUpdatePostDateStruct": {"date": "2024-01-01"},
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    problematic_study = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT000006",
                "briefTitle": "Problematic Study",
            },
            "statusModule": {
                "overallStatus": "RECRUITING",
                "lastUpdatePostDateStruct": {"date": "2024-01-02"},
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }

    mock_client = create_autospec(APIClient)
    mock_client.get_all_studies.return_value = iter([valid_study, problematic_study])

    transformer = Transformer()
    orchestrator = Orchestrator(
        connector=db_connector, api_client=mock_client, transformer=transformer
    )

    # Mock the transform_study method to raise an exception for the problematic study
    original_transform = transformer.transform_study

    def side_effect_transform(study: Any, payload: Any) -> None:
        if study.protocol_section.identification_module.nct_id == "NCT000006":
            raise ValueError("A deliberate transformation error")
        return original_transform(study, payload)

    with patch.object(
        transformer, "transform_study", side_effect=side_effect_transform
    ):
        # Act
        orchestrator.run_etl(load_type="full")

    # Assert
    with db_connector.conn.cursor() as cur:
        # Check that the valid study was processed
        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT000005'")
        count_result = cur.fetchone()
        assert count_result is not None
        assert count_result[0] == 1

        # Check that the problematic study was not processed
        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT000006'")
        count_result = cur.fetchone()
        assert count_result is not None
        assert count_result[0] == 0

        # Check that the problematic study is in the dead-letter queue
        cur.execute(
            "SELECT error_message FROM dead_letter_queue WHERE nct_id = 'NCT000006'"
        )
        error_message_result = cur.fetchone()
        assert error_message_result is not None
        error_message = error_message_result[0]
        assert (
            "Transformation Error: A deliberate transformation error" in error_message
        )
