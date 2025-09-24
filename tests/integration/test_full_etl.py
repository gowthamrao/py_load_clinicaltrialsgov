# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import pandas as pd
from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface


import pytest


@pytest.mark.integration
def test_full_etl_flow(db_connector: DatabaseConnectorInterface) -> None:
    """
    Tests the full ETL flow for a single study with child records,
    including the "delete then insert" logic for child tables.
    """
    from typing import cast

    # Cast the connector to the concrete class to access the `conn` attribute
    # This is safe because we know the fixture provides a PostgresConnector
    pg_connector = cast(PostgresConnector, db_connector)

    # 1. Initial Load
    raw_studies_df = pd.DataFrame(
        [("NCT00000123", None, None, None, "{}")],
        columns=[
            "nct_id",
            "last_updated_api",
            "last_updated_api_str",
            "ingestion_timestamp",
            "payload",
        ],
    )
    studies_columns = [
        "nct_id",
        "brief_title",
        "official_title",
        "overall_status",
        "start_date",
        "start_date_str",
        "primary_completion_date",
        "primary_completion_date_str",
        "study_type",
        "brief_summary",
    ]
    studies_df = pd.DataFrame(
        [
            (
                "NCT00000123",
                "Initial Title",
                None,
                "COMPLETED",
                None,
                None,
                None,
                None,
                "Observational",
                None,
            )
        ],
        columns=studies_columns,
    )
    interventions_df = pd.DataFrame(
        [
            {
                "nct_id": "NCT00000123",
                "intervention_type": "DRUG",
                "name": "Aspirin",
                "description": "Low dose aspirin",
            },
            {
                "nct_id": "NCT00000123",
                "intervention_type": "DRUG",
                "name": "Placebo",
                "description": "Sugar pill",
            },
        ]
    )
    outcomes_df = pd.DataFrame(
        [
            {
                "nct_id": "NCT00000123",
                "outcome_type": "PRIMARY",
                "measure": "Heart Attack",
                "time_frame": "1 year",
                "description": "Primary outcome",
            }
        ]
    )

    # Load initial data
    db_connector.bulk_load_staging("raw_studies", raw_studies_df)
    db_connector.execute_merge("raw_studies", primary_keys=["nct_id"])
    db_connector.bulk_load_staging("studies", studies_df)
    db_connector.execute_merge("studies", primary_keys=["nct_id"])
    db_connector.bulk_load_staging("interventions", interventions_df)
    db_connector.execute_merge(
        "interventions", primary_keys=["nct_id", "intervention_type", "name"]
    )
    db_connector.bulk_load_staging("design_outcomes", outcomes_df)
    db_connector.execute_merge(
        "design_outcomes", primary_keys=["nct_id", "outcome_type", "measure"]
    )

    # Verify initial load
    with pg_connector.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT00000123'")
        studies_count = cur.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 1

        cur.execute("SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT00000123'")
        interventions_count = cur.fetchone()
        assert interventions_count is not None
        assert interventions_count[0] == 2

        cur.execute("SELECT COUNT(*) FROM design_outcomes WHERE nct_id = 'NCT00000123'")
        outcomes_count = cur.fetchone()
        assert outcomes_count is not None
        assert outcomes_count[0] == 1

        cur.execute(
            "SELECT name FROM interventions WHERE nct_id = 'NCT00000123' AND name = 'Aspirin'"
        )
        assert cur.fetchone() is not None

    # 2. Second Load (Delta update for the same study)
    # The title is updated, and the interventions have changed completely.
    updated_studies_df = pd.DataFrame(
        [
            (
                "NCT00000123",
                "Updated Title",
                None,
                "COMPLETED",
                None,
                None,
                None,
                None,
                "Observational",
                None,
            )
        ],
        columns=studies_columns,
    )
    updated_interventions_df = pd.DataFrame(
        [
            {
                "nct_id": "NCT00000123",
                "intervention_type": "DEVICE",
                "name": "Stent",
                "description": "A new device",
            }
        ]
    )

    # Load updated data
    db_connector.bulk_load_staging("studies", updated_studies_df)
    db_connector.execute_merge("studies", primary_keys=["nct_id"])
    db_connector.bulk_load_staging("interventions", updated_interventions_df)
    db_connector.execute_merge(
        "interventions", primary_keys=["nct_id", "intervention_type", "name"]
    )

    # Verify the update
    with pg_connector.conn.cursor() as cur:
        # Check that the study was updated (UPSERT)
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT00000123'")
        title_result = cur.fetchone()
        assert title_result is not None
        assert title_result[0] == "Updated Title"

        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT00000123'")
        studies_count_after_update = cur.fetchone()
        assert studies_count_after_update is not None
        assert studies_count_after_update[0] == 1

        # Check that interventions were replaced (DELETE then INSERT)
        cur.execute("SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT00000123'")
        interventions_count_after_update = cur.fetchone()
        assert interventions_count_after_update is not None
        assert interventions_count_after_update[0] == 1

        cur.execute("SELECT name FROM interventions WHERE nct_id = 'NCT00000123'")
        name_result = cur.fetchone()
        assert name_result is not None
        assert name_result[0] == "Stent"

        cur.execute(
            "SELECT name FROM interventions WHERE nct_id = 'NCT00000123' AND name = 'Aspirin'"
        )
        assert cur.fetchone() is None  # The old intervention should be gone
