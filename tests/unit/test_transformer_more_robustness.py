import pytest
from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.models.api_models import Study


def test_transform_study_with_empty_input() -> None:
    study = Study.model_validate(
        {
            "protocolSection": {
                "identificationModule": {"nctId": "NCT00000000"},
                "statusModule": {"overallStatus": "UNKNOWN"},
            },
            "derivedSection": {},
            "hasResults": False,
        }
    )
    transformer = Transformer()
    transformer.transform_study(study, {})
    dataframes = transformer.get_dataframes()
    assert dataframes is not None


def test_transform_study_with_missing_required_fields() -> None:
    mock_study_data = {
        "protocolSection": {"statusModule": {"overallStatus": "COMPLETED"}},
        "derivedSection": {},
        "hasResults": False,
    }
    with pytest.raises(ValueError):
        Study.model_validate(mock_study_data)


def test_transform_study_with_large_strings() -> None:
    large_string = "a" * 10000
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT12345",
                "briefTitle": large_string,
                "officialTitle": large_string,
            },
            "statusModule": {"overallStatus": "COMPLETED"},
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    studies_df = dataframes["studies"]
    assert studies_df.iloc[0]["brief_title"] == large_string
    assert studies_df.iloc[0]["official_title"] == large_string
