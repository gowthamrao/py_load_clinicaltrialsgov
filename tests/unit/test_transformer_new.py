import pytest
import pandas as pd
from load_clinicaltrialsgov.models.api_models import Study
from load_clinicaltrialsgov.transformer.transformer import Transformer

# It's good practice to have test assets in a separate folder
# For this exercise, we'll define it in-line.
SAMPLE_STUDY_PAYLOAD = {
    "protocolSection": {
        "identificationModule": {
            "nctId": "NCT00000105",
            "briefTitle": "Phase I Study of Tomudex in Patients With Advanced Cancer",
            "officialTitle": "A Phase I Study of the Anti-Cancer Drug Tomudex (ZD1694) in Patients With Advanced Cancer",
        },
        "statusModule": {
            "overallStatus": "COMPLETED",
            "startDateStruct": {"date": "1995-01-01", "type": "ACTUAL"},
            "primaryCompletionDateStruct": {"date": "1996-05-01", "type": "ACTUAL"},
            "lastUpdatePostDateStruct": {"date": "2023-03-15", "type": "ACTUAL"},
        },
        "sponsorCollaboratorsModule": {
            "leadSponsor": {"name": "National Cancer Institute", "class": "NIH"},
            "collaborators": [
                {"name": "Some Pharma Co", "class": "INDUSTRY"},
                {"name": "Another University", "class": "U_OF_CA"},
            ],
        },
        "descriptionModule": {"briefSummary": "This is a brief summary of the study."},
        "conditionsModule": {"conditions": ["Neoplasms", "Stomach Neoplasms"]},
        "designModule": {"studyType": "INTERVENTIONAL"},
        "armsInterventionsModule": {
            "interventions": [
                {
                    "type": "DRUG",
                    "name": "Tomudex",
                    "description": "Given intravenously",
                },
                {
                    "type": "PROCEDURE",
                    "name": "Biopsy",
                    "description": "Tissue sample taken",
                },
            ]
        },
        "outcomesModule": {
            "primaryOutcomes": [
                {
                    "measure": "Maximum Tolerated Dose (MTD)",
                    "timeFrame": "28 days",
                    "description": "To determine the MTD of Tomudex",
                }
            ],
            "secondaryOutcomes": [
                {
                    "measure": "Tumor Response",
                    "timeFrame": "6 months",
                    "description": "To assess tumor response to Tomudex",
                }
            ],
        },
    },
    "derivedSection": {},
    "hasResults": True,
}


@pytest.fixture
def sample_study() -> Study:
    return Study.model_validate(SAMPLE_STUDY_PAYLOAD)


@pytest.fixture
def transformer() -> Transformer:
    return Transformer()


def test_transform_single_study_smoke(transformer: Transformer, sample_study: Study) -> None:
    """Smoke test to ensure transform_study runs without errors."""
    try:
        transformer.transform_study(sample_study, SAMPLE_STUDY_PAYLOAD)
    except Exception as e:
        pytest.fail(f"transform_study raised an exception: {e}")


def test_get_dataframes(transformer: Transformer, sample_study: Study) -> None:
    """Test that get_dataframes returns the correct structure."""
    transformer.transform_study(sample_study, SAMPLE_STUDY_PAYLOAD)
    dfs = transformer.get_dataframes()

    expected_tables = [
        "raw_studies",
        "studies",
        "sponsors",
        "conditions",
        "interventions",
        "design_outcomes",
    ]
    assert all(table in dfs for table in expected_tables)
    assert all(not df.empty for df in dfs.values())


def test_studies_table_content(transformer: Transformer, sample_study: Study) -> None:
    """Verify the content of the 'studies' dataframe."""
    transformer.transform_study(sample_study, SAMPLE_STUDY_PAYLOAD)
    df = transformer.get_dataframes()["studies"]

    assert df.shape[0] == 1
    study_row = df.iloc[0]

    assert study_row["nct_id"] == "NCT00000105"
    assert (
        study_row["brief_title"]
        == "Phase I Study of Tomudex in Patients With Advanced Cancer"
    )
    assert study_row["overall_status"] == "COMPLETED"
    assert study_row["study_type"] == "INTERVENTIONAL"
    assert pd.notna(study_row["start_date"])
    assert study_row["start_date_str"] == "1995-01-01"


def test_sponsors_table_content(transformer: Transformer, sample_study: Study) -> None:
    """Verify the content of the 'sponsors' dataframe, including collaborators."""
    transformer.transform_study(sample_study, SAMPLE_STUDY_PAYLOAD)
    df = transformer.get_dataframes()["sponsors"]

    assert df.shape[0] == 3  # 1 lead sponsor + 2 collaborators

    lead_sponsor = df[df["is_lead"]]
    assert lead_sponsor.shape[0] == 1
    assert lead_sponsor.iloc[0]["name"] == "National Cancer Institute"
    assert lead_sponsor.iloc[0]["agency_class"] == "NIH"

    collaborators = df[~df["is_lead"]]
    assert collaborators.shape[0] == 2


def test_child_tables_row_counts(transformer: Transformer, sample_study: Study) -> None:
    """Verify row counts for all child tables."""
    transformer.transform_study(sample_study, SAMPLE_STUDY_PAYLOAD)
    dfs = transformer.get_dataframes()

    assert dfs["conditions"].shape[0] == 2
    assert dfs["interventions"].shape[0] == 2
    assert dfs["design_outcomes"].shape[0] == 2  # 1 primary + 1 secondary


def test_design_outcomes_content(transformer: Transformer, sample_study: Study) -> None:
    """Verify the content of the 'design_outcomes' table."""
    transformer.transform_study(sample_study, SAMPLE_STUDY_PAYLOAD)
    df = transformer.get_dataframes()["design_outcomes"]

    primary = df[df["outcome_type"] == "PRIMARY"]
    assert primary.shape[0] == 1
    assert primary.iloc[0]["measure"] == "Maximum Tolerated Dose (MTD)"

    secondary = df[df["outcome_type"] == "SECONDARY"]
    assert secondary.shape[0] == 1
    assert secondary.iloc[0]["measure"] == "Tumor Response"


def test_date_normalization() -> None:
    """Test the _normalize_date helper directly."""
    t = Transformer()
    date1 = t._normalize_date("2023-10-25")
    assert date1 is not None
    assert date1.year == 2023

    date2 = t._normalize_date("October 2023")
    assert date2 is not None
    assert date2.month == 10

    date3 = t._normalize_date("2023")
    assert date3 is not None
    assert date3.day == 1

    assert t._normalize_date(None) is None
    assert t._normalize_date("Invalid Date") is None


def test_clear_function(transformer: Transformer, sample_study: Study) -> None:
    """Test that the clear function resets the internal state."""
    transformer.transform_study(sample_study, SAMPLE_STUDY_PAYLOAD)
    assert not transformer.get_dataframes()["studies"].empty

    transformer.clear()
    assert not transformer.studies
    assert not transformer.sponsors
    # After clearing, get_dataframes should return an empty dict
    assert not transformer.get_dataframes()
