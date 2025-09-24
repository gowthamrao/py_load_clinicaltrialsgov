# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import pytest
from pydantic import ValidationError

from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.models.api_models import Study


def test_transform_study_with_invalid_data() -> None:
    """
    Verify that transform_study raises a ValidationError when given invalid data.
    """
    invalid_study_data = {
        "protocolSection": {
            "identificationModule": {
                "orgStudyIdInfo": {"id": "STUDY_ID"},
                "organization": {"fullName": "Test Org", "class": "INDUSTRY"},
                "briefTitle": "Test Title",
                "officialTitle": "Official Test Title",
            },
            "statusModule": {
                "statusVerifiedDate": "2023-01-01",
                "overallStatus": "COMPLETED",
                "lastKnownStatus": "ACTIVE",
            },
            "designModule": {"studyType": "INTERVENTIONAL"},
        }
    }
    with pytest.raises(ValidationError):
        Study.model_validate(invalid_study_data)


def test_transform_study_with_missing_data() -> None:
    """
    Verify that transform_study can handle missing non-required data.
    """
    study_data_with_missing_fields = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT12345678",
                "organization": {"fullName": "Test Org", "class": "INDUSTRY"},
                "briefTitle": "Test Title",
            },
            "statusModule": {
                "overallStatus": "COMPLETED",
            },
            "designModule": {"studyType": "INTERVENTIONAL"},
        }
    }
    # This should not raise an exception
    study = Study.model_validate(study_data_with_missing_fields)
    transformer = Transformer()
    transformer.transform_study(study, study_data_with_missing_fields)

    dataframes = transformer.get_dataframes()
    assert "studies" in dataframes
    studies_df = dataframes["studies"]
    assert len(studies_df) == 1
    assert studies_df.iloc[0]["nct_id"] == "NCT12345678"
    assert studies_df.iloc[0]["official_title"] is None
