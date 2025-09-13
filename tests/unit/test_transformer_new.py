import pytest
from datetime import datetime, timezone
from pydantic import ValidationError
from load_clinicaltrialsgov.models.api_models import Study
from load_clinicaltrialsgov.transformer.transformer import Transformer

# A minimal valid study payload for testing purposes
MINIMAL_STUDY_PAYLOAD = {
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


def test_study_model_validation_missing_protocol_section():
    """
    Test that Study.model_validate raises a ValidationError if protocolSection is missing.
    """
    invalid_payload = {"derivedSection": {}, "hasResults": False}
    with pytest.raises(ValidationError) as exc_info:
        Study.model_validate(invalid_payload)
    assert "protocolSection" in str(exc_info.value)


def test_study_model_validation_missing_nct_id():
    """
    Test that Study.model_validate raises a ValidationError if nctId is missing.
    """
    invalid_payload = {
        "protocolSection": {
            "identificationModule": {},  # Missing nctId
            "statusModule": {"overallStatus": "UNKNOWN"},
        }
    }
    with pytest.raises(ValidationError) as exc_info:
        Study.model_validate(invalid_payload)
    assert "nctId" in str(exc_info.value)


def test_transform_study_with_missing_optional_modules(
    transformer: Transformer,
) -> None:
    """
    Test that transformation succeeds when optional modules are missing.
    """
    study = Study.model_validate(MINIMAL_STUDY_PAYLOAD)
    transformer.transform_study(study, MINIMAL_STUDY_PAYLOAD)
    dfs = transformer.get_dataframes()
    assert "sponsors" not in dfs
    assert "conditions" not in dfs
    assert "interventions" not in dfs
    assert "design_outcomes" not in dfs


def test_transform_interventions(transformer: Transformer) -> None:
    """
    Test the transformation of the interventions module.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["armsInterventionsModule"] = {
        "interventions": [
            {"type": "DRUG", "name": "TestDrug", "description": "A test drug."},
            {"type": "DEVICE", "name": "TestDevice", "description": None},
        ]
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["interventions"]
    assert df.shape[0] == 2
    assert "TestDrug" in df["name"].values
    assert df[df["name"] == "TestDevice"]["description"].iloc[0] is None


def test_transform_outcomes(transformer: Transformer) -> None:
    """
    Test the transformation of primary and secondary outcomes.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["outcomesModule"] = {
        "primaryOutcomes": [
            {
                "measure": "Primary Measure",
                "timeFrame": "1 year",
                "description": "Primary desc.",
            }
        ],
        "secondaryOutcomes": [
            {
                "measure": "Secondary Measure",
                "timeFrame": "2 years",
                "description": "Secondary desc.",
            }
        ],
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["design_outcomes"]
    assert df.shape[0] == 2
    assert df[df["outcome_type"] == "PRIMARY"].shape[0] == 1
    assert df[df["outcome_type"] == "SECONDARY"].shape[0] == 1


def test_transform_interventions_with_nulls(transformer: Transformer) -> None:
    """
    Test transformation of interventions with null or missing values.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["armsInterventionsModule"] = {
        "interventions": [
            {"type": "DRUG", "name": "TestDrug", "description": "A test drug."},
            {"type": None, "name": "TestDevice", "description": "A test device."},
            {"type": "DRUG", "name": None, "description": "Another test drug."},
        ]
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["interventions"]
    assert df.shape[0] == 3
    assert df["intervention_type"].isnull().sum() == 1
    assert df["name"].isnull().sum() == 1


def test_transform_outcomes_with_nulls(transformer: Transformer) -> None:
    """
    Test transformation of outcomes with null or missing values.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["outcomesModule"] = {
        "primaryOutcomes": [
            {
                "measure": "Primary Measure",
                "timeFrame": "1 year",
                "description": "Primary desc.",
            },
            {"measure": None, "timeFrame": "2 years", "description": "Secondary desc."},
        ],
        "secondaryOutcomes": [
            {
                "measure": "Secondary Measure",
                "timeFrame": None,
                "description": "Secondary desc.",
            },
        ],
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["design_outcomes"]
    assert df.shape[0] == 3
    assert df["measure"].isnull().sum() == 1
    assert df["time_frame"].isnull().sum() == 1


def test_transform_with_unicode_characters(transformer: Transformer) -> None:
    """
    Test that the transformer correctly handles Unicode characters in various fields.
    """
    study_payload = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT12345",
                "briefTitle": "Tést Stüdy with Ünicode",
                "officialTitle": "Öfficial Títle with Åccents",
            },
            "statusModule": {"overallStatus": "RECRUITING"},
            "descriptionModule": {"briefSummary": "A sümmáry with ümlauts."},
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"class": "INDUSTRY", "name": "TestÇorp"}
            },
            "conditionsModule": {"conditions": ["Cønditiøn 1", "Cønditiøn 2"]},
            "armsInterventionsModule": {
                "interventions": [
                    {
                        "type": "DRUG",
                        "name": "Drüg Name",
                        "description": "Descriptión with áccent.",
                    }
                ]
            },
            "outcomesModule": {
                "primaryOutcomes": [
                    {
                        "measure": "Measüre with Ünicode",
                        "timeFrame": "1 yeår",
                        "description": "Descriptión with áccent.",
                    }
                ]
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()

    assert dfs["studies"].iloc[0]["brief_title"] == "Tést Stüdy with Ünicode"
    assert dfs["studies"].iloc[0]["official_title"] == "Öfficial Títle with Åccents"
    assert dfs["studies"].iloc[0]["brief_summary"] == "A sümmáry with ümlauts."
    assert dfs["sponsors"].iloc[0]["name"] == "TestÇorp"
    assert "Cønditiøn 1" in dfs["conditions"]["name"].values
    assert dfs["interventions"].iloc[0]["name"] == "Drüg Name"
    assert dfs["design_outcomes"].iloc[0]["measure"] == "Measüre with Ünicode"


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


def test_transform_interventions_with_empty_list(transformer: Transformer) -> None:
    """
    Test that transformation succeeds when the interventions list is empty.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["armsInterventionsModule"] = {"interventions": []}
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()
    assert "interventions" not in dfs


def test_transform_with_unexpected_field(transformer: Transformer) -> None:
    """
    Test that transformation succeeds even when there is an unexpected field in the payload.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["identificationModule"]["someNewField"] = (
        "some value"
    )
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()
    assert dfs is not None


def test_transform_module_with_null_value(transformer: Transformer) -> None:
    """
    Test that transformation succeeds when a module is present but has a null value.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["armsInterventionsModule"] = None
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()
    assert "interventions" not in dfs
