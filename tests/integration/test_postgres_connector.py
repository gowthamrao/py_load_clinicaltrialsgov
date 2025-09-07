import pytest
from testcontainers.postgres import PostgresContainer
import pandas as pd
from py_load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from py_load_clinicaltrialsgov.config import settings
from datetime import datetime

@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:13") as container:
        # Override the DSN in settings
        original_dsn = settings.db.dsn
        settings.db.dsn = container.get_connection_url()
        yield container
        # Restore original DSN
        settings.db.dsn = original_dsn

def test_postgres_connector(postgres_container):
    connector = PostgresConnector()
    connector.initialize_schema()

    # Test bulk load and merge for studies
    studies_data = pd.DataFrame([{
        "nct_id": "NCT123", "brief_title": "Title", "official_title": "Official",
        "overall_status": "COMPLETED", "start_date": datetime(2023,1,1), "start_date_str": "2023-01-01",
        "primary_completion_date": datetime(2024,1,1), "primary_completion_date_str": "2024-01-01",
        "study_type": "INTERVENTIONAL", "brief_summary": "Summary"
    }])
    connector.bulk_load_staging("studies", studies_data)
    connector.execute_merge("studies", ["nct_id"])

    with connector.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM studies")
        assert cur.fetchone()[0] == 1

    # Test history
    connector.record_load_history("SUCCESS", {"records": 1})
    timestamp = connector.get_last_successful_load_timestamp()
    assert timestamp is not None
