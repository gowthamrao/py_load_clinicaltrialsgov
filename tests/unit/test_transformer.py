# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import pytest
from datetime import datetime, timezone
from pydantic import ValidationError
from pytest_structlog import StructuredLogCapture

from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.models.api_models import Study

MINIMAL_STUDY_PAYLOAD = {
    "protocolSection": {
        "identificationModule": {"nctId": "NCT00000105"},
        "statusModule": {"overallStatus": "UNKNOWN"},
    },
    "derivedSection": {},
    "hasResults": False,
}


def test_transform_study() -> None:
    # Create a mock study object
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT12345",
                "briefTitle": "Test Study",
                "officialTitle": "A Test Study for Science",
            },
            "statusModule": {
                "overallStatus": "COMPLETED",
                "startDateStruct": {"date": "2023-01"},
                "primaryCompletionDateStruct": {"date": "2024-01-01"},
            },
            "designModule": {
                "studyType": "INTERVENTIONAL",
            },
            "descriptionModule": {
                "briefSummary": "This is a test study.",
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"class": "INDUSTRY", "name": "TestCorp"}
            },
            "conditionsModule": {
                "conditions": ["Test Condition 1", "Test Condition 2"]
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)

    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    # Assertions for raw_studies
    assert len(dataframes["raw_studies"]) == 1
    assert dataframes["raw_studies"].iloc[0]["nct_id"] == "NCT12345"

    # Assertions for studies
    assert len(dataframes["studies"]) == 1
    assert dataframes["studies"].iloc[0]["brief_title"] == "Test Study"
    assert dataframes["studies"].iloc[0]["start_date"] is not None
    assert dataframes["studies"].iloc[0]["start_date_str"] == "2023-01"
    assert dataframes["studies"].iloc[0]["primary_completion_date_str"] == "2024-01-01"

    # Assertions for sponsors
    assert len(dataframes["sponsors"]) == 1
    assert dataframes["sponsors"].iloc[0]["name"] == "TestCorp"

    # Assertions for conditions
    assert len(dataframes["conditions"]) == 2


@pytest.mark.parametrize(
    ("date_str", "expected_date"),
    [
        ("2023-05-15", datetime(2023, 5, 15, tzinfo=timezone.utc)),
        ("2023-07", datetime(2023, 7, 1, tzinfo=timezone.utc)),
        ("February 2024", datetime(2024, 2, 1, tzinfo=timezone.utc)),
        ("2025", datetime(2025, 1, 1, tzinfo=timezone.utc)),
        ("Jan 2023", datetime(2023, 1, 1, tzinfo=timezone.utc)),
        ("2023/10/26", datetime(2023, 10, 26, tzinfo=timezone.utc)),
        ("invalid-date", None),
        (None, None),
        ("", None),
    ],
)
def test_normalize_date(
    date_str: str | None, expected_date: datetime | None, log: StructuredLogCapture
) -> None:
    transformer = Transformer()
    assert transformer._normalize_date(date_str) == expected_date
    if date_str == "invalid-date":
        assert log.has("unparseable_date_string", level="warning")


def test_transform_study_with_collaborators() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT54321"},
            "statusModule": {
                "startDateStruct": {"date": "2022"},
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"class": "INDUSTRY", "name": "Lead Sponsor Inc."},
                "collaborators": [
                    {"class": "NIH", "name": "Collaborator 1"},
                    {"class": "OTHER", "name": "Collaborator 2"},
                ],
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    # Assertions for sponsors
    sponsors_df = dataframes["sponsors"]
    assert len(sponsors_df) == 3

    lead_sponsor = sponsors_df[sponsors_df["is_lead"]]
    assert len(lead_sponsor) == 1
    assert lead_sponsor.iloc[0]["name"] == "Lead Sponsor Inc."

    collaborators = sponsors_df[~sponsors_df["is_lead"]]
    assert len(collaborators) == 2
    assert "Collaborator 1" in collaborators["name"].values
    assert "Collaborator 2" in collaborators["name"].values

    # Assertions for date parsing
    studies_df = dataframes["studies"]
    start_date = studies_df.iloc[0]["start_date"]
    assert start_date.year == 2022
    assert start_date.month == 1
    assert start_date.day == 1


def test_transform_study_with_no_sponsors() -> None:
    study = Study.model_validate(MINIMAL_STUDY_PAYLOAD)
    transformer = Transformer()
    transformer.transform_study(study, MINIMAL_STUDY_PAYLOAD)
    dataframes = transformer.get_dataframes()
    assert "sponsors" not in dataframes


def test_transform_study_with_no_conditions() -> None:
    study = Study.model_validate(MINIMAL_STUDY_PAYLOAD)
    transformer = Transformer()
    transformer.transform_study(study, MINIMAL_STUDY_PAYLOAD)
    dataframes = transformer.get_dataframes()
    assert "conditions" not in dataframes


def test_transform_study_with_no_interventions() -> None:
    study = Study.model_validate(MINIMAL_STUDY_PAYLOAD)
    transformer = Transformer()
    transformer.transform_study(study, MINIMAL_STUDY_PAYLOAD)
    dataframes = transformer.get_dataframes()
    assert "interventions" not in dataframes


def test_transform_study_with_no_outcomes() -> None:
    study = Study.model_validate(MINIMAL_STUDY_PAYLOAD)
    transformer = Transformer()
    transformer.transform_study(study, MINIMAL_STUDY_PAYLOAD)
    dataframes = transformer.get_dataframes()
    assert "design_outcomes" not in dataframes


def test_transform_study_with_missing_identification_module() -> None:
    payload = {
        "protocolSection": {
            "statusModule": {"overallStatus": "UNKNOWN"},
        },
        "derivedSection": {},
        "hasResults": False,
    }
    with pytest.raises(ValidationError):
        Study.model_validate(payload)


def test_normalize_date_with_timezone() -> None:
    """
    Test that _normalize_date correctly handles a date string with a timezone.
    """
    transformer = Transformer()
    date_str = "2023-01-01T12:00:00+02:00"
    expected_date = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    assert transformer._normalize_date(date_str) == expected_date


def test_transform_study_with_minimal_data() -> None:
    """
    Test that transform_study works correctly with a study that has the bare minimum of data.
    """
    study = Study.model_validate(MINIMAL_STUDY_PAYLOAD)
    transformer = Transformer()
    transformer.transform_study(study, MINIMAL_STUDY_PAYLOAD)
    dataframes = transformer.get_dataframes()

    assert "raw_studies" in dataframes
    assert len(dataframes["raw_studies"]) == 1
    assert "studies" in dataframes
    assert len(dataframes["studies"]) == 1
    assert "sponsors" not in dataframes
    assert "conditions" not in dataframes
    assert "interventions" not in dataframes
    assert "design_outcomes" not in dataframes


def test_transform_study_with_missing_status_module() -> None:
    payload = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT00000105"},
        },
        "derivedSection": {},
        "hasResults": False,
    }
    with pytest.raises(ValidationError):
        Study.model_validate(payload)
