# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import pytest
import json

from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from load_clinicaltrialsgov.orchestrator import Orchestrator
from load_clinicaltrialsgov.extractor.api_client import APIClient
from load_clinicaltrialsgov.transformer.transformer import Transformer


from typing import cast, Generator
from unittest.mock import patch
from testcontainers.postgres import PostgresContainer


@pytest.fixture
def db_connector(
    postgres_container: PostgresContainer,
) -> Generator[DatabaseConnectorInterface, None, None]:
    # The DSN is already set correctly by the postgres_container fixture
    connector = PostgresConnector()
    yield connector
    connector.conn.close()


@pytest.fixture
def api_client() -> APIClient:
    return APIClient()


@pytest.fixture
def transformer() -> Transformer:
    return Transformer()


@pytest.mark.integration
def test_full_etl_with_api_data(
    db_connector: DatabaseConnectorInterface,
    api_client: APIClient,
    transformer: Transformer,
) -> None:
    """
    Tests the full ETL flow using a real API response from a file.
    """
    # Load the test data from the JSON file
    with open("tests/integration/NCT04267848.json") as f:
        study_data = json.load(f)

    # Mock the API client to return the test data
    with patch(
        "load_clinicaltrialsgov.extractor.api_client.APIClient.get_all_studies",
        return_value=iter([study_data]),
    ):
        # Run the ETL
        orchestrator = Orchestrator(db_connector, api_client, transformer)
        orchestrator.run_etl(load_type="full")

    # Verify the data was loaded correctly
    pg_connector = cast(PostgresConnector, db_connector)
    with pg_connector.conn.cursor() as cur:
        # Check raw_studies
        cur.execute("SELECT COUNT(*) FROM raw_studies WHERE nct_id = 'NCT04267848'")
        count_result = cur.fetchone()
        assert count_result is not None
        assert count_result[0] == 1

        # Check studies
        cur.execute(
            "SELECT brief_title, study_type FROM studies WHERE nct_id = 'NCT04267848'"
        )
        study_result = cur.fetchone()
        assert study_result is not None
        assert "Immunotherapy" in study_result[0]
        assert study_result[1] == "INTERVENTIONAL"

        # Check sponsors
        cur.execute("SELECT name, is_lead FROM sponsors WHERE nct_id = 'NCT04267848'")
        sponsors = cur.fetchall()
        assert len(sponsors) > 0
        lead_sponsors = [s for s in sponsors if s[1]]
        assert len(lead_sponsors) == 1
        assert lead_sponsors[0][0] == "National Cancer Institute (NCI)"

        # Check conditions
        cur.execute("SELECT name FROM conditions WHERE nct_id = 'NCT04267848'")
        conditions = [row[0] for row in cur.fetchall()]
        assert "Lung Non-Small Cell Carcinoma" in conditions

        # Check interventions
        cur.execute(
            "SELECT name, intervention_type FROM interventions WHERE nct_id = 'NCT04267848'"
        )
        interventions = cur.fetchall()
        assert len(interventions) > 0
        intervention_names = [row[0] for row in interventions]
        assert "Pembrolizumab" in intervention_names

        # Check design_outcomes
        cur.execute(
            "SELECT measure, outcome_type FROM design_outcomes WHERE nct_id = 'NCT04267848'"
        )
        outcomes = cur.fetchall()
        assert len(outcomes) > 0
        primary_outcomes = [o for o in outcomes if o[1] == "PRIMARY"]
        assert len(primary_outcomes) > 0
        assert "Disease free survival" in primary_outcomes[0][0]
