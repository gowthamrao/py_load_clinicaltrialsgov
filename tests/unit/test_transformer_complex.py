import json
from load_clinicaltrialsgov.models.api_models import Study
from load_clinicaltrialsgov.transformer.transformer import Transformer


def test_transform_complex_study() -> None:
    # Load the complex study from the JSON file
    with open("tests/integration/NCT04267848_complex.json") as f:
        study_data = json.load(f)

    # Create a Study object from the data
    study = Study.model_validate(study_data)

    # Transform the study
    transformer = Transformer()
    transformer.transform_study(study, study_data)
    dataframes = transformer.get_dataframes()

    # Assertions for the main studies table
    assert len(dataframes["studies"]) == 1
    studies_df = dataframes["studies"]
    assert studies_df.iloc[0]["nct_id"] == "NCT04267848"
    assert studies_df.iloc[0]["brief_title"] is not None

    # Assertions for sponsors
    assert len(dataframes["sponsors"]) == 3
    sponsors_df = dataframes["sponsors"]
    assert sponsors_df["is_lead"].sum() == 1

    # Assertions for conditions
    assert len(dataframes["conditions"]) > 0

    # Assertions for interventions
    assert len(dataframes["interventions"]) > 0

    # Assertions for intervention_arm_groups
    assert "intervention_arm_groups" in dataframes
    assert len(dataframes["intervention_arm_groups"]) > 0

    # Assertions for design_outcomes
    assert len(dataframes["design_outcomes"]) > 0
    outcomes_df = dataframes["design_outcomes"]
    assert "PRIMARY" in outcomes_df["outcome_type"].unique()
    assert "SECONDARY" in outcomes_df["outcome_type"].unique()
    assert "OTHER" in outcomes_df["outcome_type"].unique()
