# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


from datetime import datetime, timezone
import pytest
from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.models.api_models import Study
from typing import Any, Dict

# A minimal valid study payload for testing purposes
MINIMAL_STUDY_PAYLOAD: Dict[str, Any] = {
    "protocolSection": {
        "identificationModule": {"nctId": "NCT00000105"},
        "statusModule": {"overallStatus": "UNKNOWN"},
    },
    "derivedSection": {},
    "hasResults": False,
}


@pytest.fixture
def transformer() -> Transformer:
    """Returns a new Transformer instance for each test."""
    return Transformer()


def test_transform_study_with_many_interventions(transformer: Transformer) -> None:
    """
    Test transformation of a study with a large number of interventions.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    num_interventions = 100
    interventions = [
        {"type": "DRUG", "name": f"TestDrug {i}", "description": f"A test drug {i}."}
        for i in range(num_interventions)
    ]
    study_payload["protocolSection"]["armsInterventionsModule"] = {
        "interventions": interventions
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["interventions"]
    assert df.shape[0] == num_interventions


def test_transform_study_with_long_strings(transformer: Transformer) -> None:
    """
    Test transformation of a study with very long string values.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    long_string = "a" * 10000
    study_payload["protocolSection"]["identificationModule"]["briefTitle"] = long_string
    study_payload["protocolSection"]["conditionsModule"] = {"conditions": [long_string]}
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()

    assert dfs["studies"].iloc[0]["brief_title"] == long_string
    assert dfs["conditions"].iloc[0]["name"] == long_string


def test_transform_handles_nulls_in_child_records(transformer: Transformer) -> None:
    """
    Test that the transformer correctly handles records with null values in child tables.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["armsInterventionsModule"] = {
        "interventions": [
            {"type": "DRUG", "name": "TestDrug", "description": "A test drug."},
            {"type": None, "name": "TestDevice", "description": "A test device."},
            {"type": "DRUG", "name": None, "description": "Another test drug."},
        ]
    }
    study_payload["protocolSection"]["outcomesModule"] = {
        "primaryOutcomes": [
            {
                "measure": "Primary Measure",
                "timeFrame": "1 year",
                "description": "Primary desc.",
            },
            {"measure": None, "timeFrame": "2 years", "description": "Secondary desc."},
        ],
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()

    interventions_df = dfs["interventions"]
    assert interventions_df.shape[0] == 3
    assert interventions_df["intervention_type"].isnull().sum() == 1
    assert interventions_df["name"].isnull().sum() == 1

    outcomes_df = dfs["design_outcomes"]
    assert outcomes_df.shape[0] == 2
    assert outcomes_df["measure"].isnull().sum() == 1


@pytest.mark.parametrize(
    ("date_str", "expected_date"),
    [
        # Ambiguous formats
        ("01-02-2023", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        ("02/01/2023", datetime(2023, 2, 1, tzinfo=timezone.utc)),
        ("2023-01-02 10:30", datetime(2023, 1, 2, 10, 30, tzinfo=timezone.utc)),
        ("January 5, 2023", datetime(2023, 1, 5, tzinfo=timezone.utc)),
        ("5 January 2023", datetime(2023, 1, 5, tzinfo=timezone.utc)),
        ("2023", datetime(2023, 1, 1, tzinfo=timezone.utc)),
        ("2023-01", datetime(2023, 1, 1, tzinfo=timezone.utc)),
        ("Jan 2023", datetime(2023, 1, 1, tzinfo=timezone.utc)),
        ("2023 Jan", datetime(2023, 1, 1, tzinfo=timezone.utc)),
        # Different separators
        ("2023.01.02", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        ("2023-01-02", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        ("2023/01/02", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        # Textual months
        ("Jan 2, 2023", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        ("January 2, 2023", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        ("2-Jan-2023", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        ("2-January-2023", datetime(2023, 1, 2, tzinfo=timezone.utc)),
        # With time
        ("2023-01-02T10:30:00", datetime(2023, 1, 2, 10, 30, tzinfo=timezone.utc)),
        ("2023-01-02 10:30:00", datetime(2023, 1, 2, 10, 30, tzinfo=timezone.utc)),
        ("2023-01-02 10:30 AM", datetime(2023, 1, 2, 10, 30, tzinfo=timezone.utc)),
        ("2023-01-02 10:30 PM", datetime(2023, 1, 2, 22, 30, tzinfo=timezone.utc)),
        # Invalid dates
        ("2023-13-01", None),
        ("2023-02-30", None),
        ("not a date", None),
        ("2023-01-01T10:30:00Z", datetime(2023, 1, 1, 10, 30, tzinfo=timezone.utc)),
    ],
)
def test_normalize_date_edge_cases(
    date_str: str | None, expected_date: datetime | None, transformer: Transformer
) -> None:
    assert transformer._normalize_date(date_str) == expected_date


def test_transform_study_with_missing_modules(transformer: Transformer) -> None:
    """
    Test transformation of a study with missing optional modules.
    """
    study_payload: Dict[str, Any] = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT00000105"},
            "statusModule": {"overallStatus": "UNKNOWN"},
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()

    assert "sponsors" not in dfs
    assert "conditions" not in dfs
    assert "interventions" not in dfs
    assert "design_outcomes" not in dfs


def test_transform_study_with_special_characters(transformer: Transformer) -> None:
    """
    Test transformation of a study with special characters in strings.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    special_string = "Test with \"quotes\", 'apostrophes', \nnewlines, and \ttabs."
    study_payload["protocolSection"]["identificationModule"]["briefTitle"] = (
        special_string
    )
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["studies"]

    assert df.iloc[0]["brief_title"] == special_string


def test_transform_study_with_unexpected_field(transformer: Transformer) -> None:
    """
    Test that the transformer ignores unexpected fields and processes the rest of the study.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["statusModule"]["unexpectedField"] = "some_value"
    study_payload["protocolSection"]["unexpectedModule"] = {"field": "value"}

    try:
        study = Study.model_validate(study_payload)
        transformer.transform_study(study, study_payload)
    except Exception as e:
        pytest.fail(f"Transformer failed to handle unexpected fields: {e}")

    dfs = transformer.get_dataframes()
    assert "studies" in dfs
    assert "unexpectedField" not in dfs["studies"].columns
    assert "unexpectedModule" not in dfs


def test_transform_study_with_unexpected_nulls(transformer: Transformer) -> None:
    """
    Test transformation of a study with null values in optional fields.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    # briefTitle is optional, so this should work
    study_payload["protocolSection"]["identificationModule"]["briefTitle"] = None
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["studies"]

    assert df.iloc[0]["brief_title"] is None


def test_transform_study_with_empty_strings(transformer: Transformer) -> None:
    """
    Test transformation of a study with empty strings.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["identificationModule"]["briefTitle"] = ""
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["studies"]

    assert df.iloc[0]["brief_title"] == ""


def test_transform_study_with_unicode_characters(transformer: Transformer) -> None:
    """
    Test transformation of a study with unicode characters.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    unicode_string = "Test with unicode: ö, ü, á, í, ñ, and 😊"
    study_payload["protocolSection"]["identificationModule"]["briefTitle"] = (
        unicode_string
    )
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["studies"]

    assert df.iloc[0]["brief_title"] == unicode_string


@pytest.mark.parametrize(
    ("date_str", "expected_date"),
    [
        (
            "2023-01-01T12:00:00+02:00",
            datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
        ),
        (
            "2023-01-01T12:00:00-05:00",
            datetime(2023, 1, 1, 17, 0, tzinfo=timezone.utc),
        ),
    ],
)
def test_normalize_date_with_timezone(
    date_str: str, expected_date: datetime, transformer: Transformer
) -> None:
    """
    Test date normalization with explicit timezone information.
    """
    assert transformer._normalize_date(date_str) == expected_date


def test_transform_study_with_empty_and_null_lists(transformer: Transformer) -> None:
    """
    Test transformation of a study with empty or null lists for child records.
    """
    study_payload: Dict[str, Any] = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["conditionsModule"] = {"conditions": []}
    study_payload["protocolSection"]["armsInterventionsModule"] = None
    study_payload["protocolSection"]["outcomesModule"] = {"primaryOutcomes": None}
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()

    assert "conditions" not in dfs
    assert "interventions" not in dfs
    assert "design_outcomes" not in dfs
