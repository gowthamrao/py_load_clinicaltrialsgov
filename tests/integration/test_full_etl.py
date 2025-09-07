import pytest
from testcontainers.postgres import PostgresContainer
import pandas as pd
from py_load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from py_load_clinicaltrialsgov.config import settings
from datetime import datetime

@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:13") as container:
        original_dsn = settings.db.dsn
        settings.db.dsn = container.get_connection_url()
        yield container
        settings.db.dsn = original_dsn

@pytest.fixture(scope="module")
def db_connector(postgres_container):
    connector = PostgresConnector()
    connector.initialize_schema()
    return connector

def test_full_etl_flow(db_connector):
    """
    Tests the full ETL flow for a single study with child records,
    including the "delete then insert" logic for child tables.
    """
    # 1. Initial Load
    studies_df = pd.DataFrame([{"nct_id": "NCT00000123", "brief_title": "Initial Title", "study_type": "Observational"}])
    interventions_df = pd.DataFrame([
        {"nct_id": "NCT00000123", "intervention_type": "DRUG", "name": "Aspirin", "description": "Low dose aspirin"},
        {"nct_id": "NCT00000123", "intervention_type": "DRUG", "name": "Placebo", "description": "Sugar pill"}
    ])
    outcomes_df = pd.DataFrame([
        {"nct_id": "NCT00000123", "outcome_type": "PRIMARY", "measure": "Heart Attack", "time_frame": "1 year", "description": "Primary outcome"}
    ])

    # Load initial data
    db_connector.bulk_load_staging("studies", studies_df)
    db_connector.execute_merge("studies")
    db_connector.bulk_load_staging("interventions", interventions_df)
    db_connector.execute_merge("interventions")
    db_connector.bulk_load_staging("design_outcomes", outcomes_df)
    db_connector.execute_merge("design_outcomes")

    # Verify initial load
    with db_connector.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT COUNT(*) FROM design_outcomes WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT name FROM interventions WHERE nct_id = 'NCT00000123' AND name = 'Aspirin'")
        assert cur.fetchone() is not None

    # 2. Second Load (Delta update for the same study)
    # The title is updated, and the interventions have changed completely.
    updated_studies_df = pd.DataFrame([{"nct_id": "NCT00000123", "brief_title": "Updated Title", "study_type": "Observational"}])
    updated_interventions_df = pd.DataFrame([
        {"nct_id": "NCT00000123", "intervention_type": "DEVICE", "name": "Stent", "description": "A new device"}
    ])

    # Load updated data
    db_connector.bulk_load_staging("studies", updated_studies_df)
    db_connector.execute_merge("studies")
    db_connector.bulk_load_staging("interventions", updated_interventions_df)
    db_connector.execute_merge("interventions")

    # Verify the update
    with db_connector.conn.cursor() as cur:
        # Check that the study was updated (UPSERT)
        cur.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == "Updated Title"
        cur.execute("SELECT COUNT(*) FROM studies WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1

        # Check that interventions were replaced (DELETE then INSERT)
        cur.execute("SELECT COUNT(*) FROM interventions WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT name FROM interventions WHERE nct_id = 'NCT00000123'")
        assert cur.fetchone()[0] == "Stent"
        cur.execute("SELECT name FROM interventions WHERE nct_id = 'NCT00000123' AND name = 'Aspirin'")
        assert cur.fetchone() is None # The old intervention should be gone
