import pytest
from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.models.api_models import Study

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


def test_transform_study_with_many_interventions(transformer: Transformer) -> None:
    """
    Test transformation of a study with a large number of interventions.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
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
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    long_string = "a" * 10000
    study_payload["protocolSection"]["identificationModule"]["briefTitle"] = long_string
    study_payload["protocolSection"]["conditionsModule"] = {
        "conditions": [long_string]
    }
    study = Study.model_validate(study_payload)
    transformer.transform_study(study, study_payload)
    dfs = transformer.get_dataframes()

    assert dfs["studies"].iloc[0]["brief_title"] == long_string
    assert dfs["conditions"].iloc[0]["name"] == long_string


def test_transform_handles_nulls_in_child_records(transformer: Transformer) -> None:
    """
    Test that the transformer correctly handles records with null values in child tables.
    """
    study_payload = MINIMAL_STUDY_PAYLOAD.copy()
    study_payload["protocolSection"]["armsInterventionsModule"] = {
        "interventions": [
            {"type": "DRUG", "name": "TestDrug", "description": "A test drug."},
            {"type": None, "name": "TestDevice", "description": "A test device."},
            {"type": "DRUG", "name": None, "description": "Another test drug."},
        ]
    }
    study_payload["protocolSection"]["outcomesModule"] = {
        "primaryOutcomes": [
            {"measure": "Primary Measure", "timeFrame": "1 year", "description": "Primary desc."},
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
