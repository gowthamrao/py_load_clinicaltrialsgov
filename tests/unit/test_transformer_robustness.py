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


def test_transform_study_with_all_outcome_types() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345"},
            "statusModule": {"overallStatus": "COMPLETED"},
            "outcomesModule": {
                "primaryOutcomes": [
                    {
                        "measure": "Primary Measure 1",
                        "timeFrame": "Time Frame 1",
                        "description": "Desc 1",
                    }
                ],
                "secondaryOutcomes": [
                    {
                        "measure": "Secondary Measure 1",
                        "timeFrame": "Time Frame 2",
                        "description": "Desc 2",
                    }
                ],
                "otherOutcomes": [
                    {
                        "measure": "Other Measure 1",
                        "timeFrame": "Time Frame 3",
                        "description": "Desc 3",
                    }
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

    assert "design_outcomes" in dataframes
    outcomes_df = dataframes["design_outcomes"]
    assert len(outcomes_df) == 3

    primary_outcome = outcomes_df[outcomes_df["outcome_type"] == "PRIMARY"]
    assert len(primary_outcome) == 1
    assert primary_outcome.iloc[0]["measure"] == "Primary Measure 1"

    secondary_outcome = outcomes_df[outcomes_df["outcome_type"] == "SECONDARY"]
    assert len(secondary_outcome) == 1
    assert secondary_outcome.iloc[0]["measure"] == "Secondary Measure 1"

    other_outcome = outcomes_df[outcomes_df["outcome_type"] == "OTHER"]
    assert len(other_outcome) == 1
    assert other_outcome.iloc[0]["measure"] == "Other Measure 1"


def test_transform_study_with_interventions() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345"},
            "statusModule": {"overallStatus": "COMPLETED"},
            "armsInterventionsModule": {
                "interventions": [
                    {"type": "Drug", "name": "Drug A", "description": "Desc A"},
                    {"type": "Device", "name": "Device B", "description": "Desc B"},
                ]
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    assert "interventions" in dataframes
    interventions_df = dataframes["interventions"]
    assert len(interventions_df) == 2

    drug_intervention = interventions_df[interventions_df["name"] == "Drug A"]
    assert len(drug_intervention) == 1
    assert drug_intervention.iloc[0]["intervention_type"] == "Drug"

    device_intervention = interventions_df[interventions_df["name"] == "Device B"]
    assert len(device_intervention) == 1
    assert device_intervention.iloc[0]["intervention_type"] == "Device"


def test_transform_study_with_missing_optional_fields_and_empty_lists() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {"nctId": "NCT12345"},
            "statusModule": {"overallStatus": "COMPLETED"},
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"class": "INDUSTRY", "name": "TestCorp"},
                "collaborators": [],
            },
            "armsInterventionsModule": {
                "interventions": [
                    {"type": "Drug", "name": "Drug A", "description": None}
                ]
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    assert "sponsors" in dataframes
    sponsors_df = dataframes["sponsors"]
    assert len(sponsors_df) == 1
    assert sponsors_df.iloc[0]["name"] == "TestCorp"

    assert "interventions" in dataframes
    interventions_df = dataframes["interventions"]
    assert len(interventions_df) == 1
    assert interventions_df.iloc[0]["name"] == "Drug A"
    assert interventions_df.iloc[0]["description"] is None


def test_transform_study_with_unicode_characters() -> None:
    mock_study_data = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT12345",
                "briefTitle": "Stüdy with Ünicode",
                "officialTitle": "Öfficial Title with Ünicode",
            },
            "statusModule": {"overallStatus": "COMPLETED"},
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"class": "INDUSTRY", "name": "TestCörp"}
            },
        },
        "derivedSection": {},
        "hasResults": False,
    }
    study = Study.model_validate(mock_study_data)
    transformer = Transformer()
    transformer.transform_study(study, mock_study_data)
    dataframes = transformer.get_dataframes()

    studies_df = dataframes["studies"]
    assert studies_df.iloc[0]["brief_title"] == "Stüdy with Ünicode"
    assert studies_df.iloc[0]["official_title"] == "Öfficial Title with Ünicode"

    sponsors_df = dataframes["sponsors"]
    assert sponsors_df.iloc[0]["name"] == "TestCörp"
