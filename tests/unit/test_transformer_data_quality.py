# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.models.api_models import Study


def test_transform_study_with_junk_sponsor_name() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345"},
            "statusModule": {"overallStatus": "COMPLETED"},
            "sponsorCollaboratorsModule": {
                "leadSponsor": {
                    "class": "INDUSTRY",
                    "name": "Clinical Development Manager",
                }
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    assert len(dataframes["sponsors"]) == 1
    assert dataframes["sponsors"].iloc[0]["name"] == "Clinical Development Manager"


def test_transform_study_with_prefixed_sponsor_name() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345"},
            "statusModule": {"overallStatus": "COMPLETED"},
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"class": "INDUSTRY", "name": "Dr. John Doe"}
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    assert len(dataframes["sponsors"]) == 1
    assert dataframes["sponsors"].iloc[0]["name"] == "Dr. John Doe"


def test_transform_study_with_missing_sponsor_module() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345"},
            "statusModule": {"overallStatus": "COMPLETED"},
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    assert "sponsors" not in dataframes


def test_transform_study_with_missing_dates() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345"},
            "statusModule": {
                "overallStatus": "COMPLETED",
                "startDateStruct": None,
                "primaryCompletionDateStruct": None,
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    assert len(dataframes["studies"]) == 1
    assert dataframes["studies"].iloc[0]["start_date"] is None
    assert dataframes["studies"].iloc[0]["primary_completion_date"] is None
