import pytest
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


def test_transform_study_with_missing_optional_modules(transformer: Transformer) -> None:
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
            {"measure": "Primary Measure", "timeFrame": "1 year", "description": "Primary desc."}
        ],
        "secondaryOutcomes": [
            {"measure": "Secondary Measure", "timeFrame": "2 years", "description": "Secondary desc."}
        ],
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    df = transformer.get_dataframes()["design_outcomes"]
    assert df.shape[0] == 2
    assert df[df["outcome_type"] == "PRIMARY"].shape[0] == 1
    assert df[df["outcome_type"] == "SECONDARY"].shape[0] == 1
