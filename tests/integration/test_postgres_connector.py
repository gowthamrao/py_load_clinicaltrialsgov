# This file will contain the integration tests for the PostgresConnector.
import pytest
from testcontainers.postgres import PostgresContainer
import pandas as pd
from datetime import datetime, timezone

from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.config import settings

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
def postgres_container():
    """Starts and stops a PostgreSQL container for each test function."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="function")
def db_connector(postgres_container: PostgresContainer):
    """Provides a PostgresConnector instance connected to the test container."""
    # Override the DSN from settings with the one from the container
    original_dsn = settings.db.dsn
    # psycopg3 doesn't like the +psycopg2 driver specifier in the URL
    db_url = postgres_container.get_connection_url().replace("+psycopg2", "")
    settings.db.dsn = db_url

    connector = PostgresConnector()

    # Initialize the schema
    with open("src/load_clinicaltrialsgov/sql/schema.sql", "r") as f:
        schema_sql = f.read()

    with connector.conn.cursor() as cursor:
        cursor.execute(schema_sql)
    connector.conn.commit()

    yield connector

    # Teardown: close connection and restore original settings
    connector.close()
    settings.db.dsn = original_dsn


def test_connection(db_connector: PostgresConnector):
    """Tests that the connector can successfully connect to the database."""
    assert db_connector.conn is not None
    assert not db_connector.conn.closed


def test_bulk_load_and_merge_flow(db_connector: PostgresConnector):
    """
    Tests the full flow of bulk loading to a staging table and then merging
    to the final table.
    """
    # 1. Prepare sample data
    raw_studies_data = pd.DataFrame(
        {
            "nct_id": ["NCT00000001", "NCT00000002"],
            "last_updated_api": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
            ],
            "last_updated_api_str": ["2024-01-01", "2024-01-02"],
            "ingestion_timestamp": [
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            ],
            "payload": ['{"key": "value1"}', '{"key": "value2"}'],
        }
    )

    studies_data = pd.DataFrame(
        {
            "nct_id": ["NCT00000001"],
            "brief_title": ["Test Study"],
            "official_title": ["Official Title for Test Study"],
            "overall_status": ["COMPLETED"],
            "start_date": [datetime(2023, 1, 1).date()],
            "start_date_str": ["January 2023"],
            "primary_completion_date": [datetime(2024, 1, 1).date()],
            "primary_completion_date_str": ["January 2024"],
            "study_type": ["INTERVENTIONAL"],
            "brief_summary": ["A brief summary."],
        }
    )

    sponsors_data = pd.DataFrame(
        {
            "nct_id": ["NCT00000001", "NCT00000001"],
            "agency_class": ["INDUSTRY", "INDUSTRY"],
            "name": ["TestCorp", "AnotherTestCorp"],
            "is_lead": [True, False],
        }
    )

    # 2. Bulk load into staging tables
    db_connector.bulk_load_staging("raw_studies", raw_studies_data)
    db_connector.bulk_load_staging("studies", studies_data)
    db_connector.bulk_load_staging("sponsors", sponsors_data)

    # 3. Verify staging tables have data
    with db_connector.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM staging_raw_studies")
        raw_studies_count = cursor.fetchone()
        assert raw_studies_count is not None
        assert raw_studies_count[0] == 2

        cursor.execute("SELECT COUNT(*) FROM staging_studies")
        studies_count = cursor.fetchone()
        assert studies_count is not None
        assert studies_count[0] == 1

        cursor.execute("SELECT COUNT(*) FROM staging_sponsors")
        sponsors_count = cursor.fetchone()
        assert sponsors_count is not None
        assert sponsors_count[0] == 2

    # 4. Execute merge
    db_connector.execute_merge("raw_studies", ["nct_id"])
    db_connector.execute_merge("studies", ["nct_id"])
    db_connector.execute_merge("sponsors", ["nct_id", "name", "agency_class"])

    # 5. Verify final tables have data
    with db_connector.conn.cursor() as cursor:
        cursor.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT00000001'")
        brief_title_result = cursor.fetchone()
        assert brief_title_result is not None
        assert brief_title_result[0] == "Test Study"

        cursor.execute("SELECT COUNT(*) FROM sponsors WHERE nct_id = 'NCT00000001'")
        sponsors_count_final = cursor.fetchone()
        assert sponsors_count_final is not None
        assert sponsors_count_final[0] == 2

    # 6. Test UPSERT: Load updated data and new data
    updated_studies_data = pd.DataFrame(
        {
            "nct_id": ["NCT00000001", "NCT00000002"],
            "brief_title": ["Test Study (Updated)", "A New Study"],
            "official_title": [
                "Official Title for Test Study",
                "Official Title for New Study",
            ],
            "overall_status": ["COMPLETED", "RECRUITING"],
            "start_date": [datetime(2023, 1, 1).date(), datetime(2023, 2, 1).date()],
            "start_date_str": ["January 2023", "February 2023"],
            "primary_completion_date": [
                datetime(2024, 1, 1).date(),
                datetime(2024, 2, 1).date(),
            ],
            "primary_completion_date_str": ["January 2024", "February 2024"],
            "study_type": ["INTERVENTIONAL", "OBSERVATIONAL"],
            "brief_summary": ["A brief summary (updated).", "Summary for new study."],
        }
    )

    db_connector.bulk_load_staging("studies", updated_studies_data)
    db_connector.execute_merge("studies", ["nct_id"])

    # 7. Verify UPSERT logic
    with db_connector.conn.cursor() as cursor:
        # Check that the total count is now 2
        cursor.execute("SELECT COUNT(*) FROM studies")
        total_count = cursor.fetchone()
        assert total_count is not None
        assert total_count[0] == 2

        # Check that the original study was updated
        cursor.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT00000001'")
        updated_title = cursor.fetchone()
        assert updated_title is not None
        assert updated_title[0] == "Test Study (Updated)"

        # Check that the new study was inserted
        cursor.execute("SELECT brief_title FROM studies WHERE nct_id = 'NCT00000002'")
        new_title = cursor.fetchone()
        assert new_title is not None
        assert new_title[0] == "A New Study"


def test_load_history(db_connector: PostgresConnector):
    """Tests the load history and high-water mark functionality."""
    # 1. Check that history is initially empty
    assert db_connector.get_last_successful_load_timestamp() is None
    assert db_connector.get_last_load_history() is None

    # 2. Record a successful load
    metrics_success = {"studies_processed": 100}
    db_connector.record_load_history("SUCCESS", metrics_success)

    # 3. Verify the successful load
    history = db_connector.get_last_load_history()
    assert history is not None
    assert history["status"] == "SUCCESS"
    assert history["metrics"] == metrics_success

    timestamp = db_connector.get_last_successful_load_timestamp()
    assert timestamp is not None
    # Add timezone info to the naive datetime from the DB
    timestamp = timestamp.replace(tzinfo=timezone.utc)
    assert (datetime.now(timezone.utc) - timestamp).total_seconds() < 10

    # 4. Record a failed load
    metrics_failure = {"error": "Something went wrong"}
    db_connector.record_load_history("FAILURE", metrics_failure)

    # 5. Verify the failed load is the latest
    history = db_connector.get_last_load_history()
    assert history is not None
    assert history["status"] == "FAILURE"

    # 6. Verify the high-water mark is still from the last SUCCESSFUL run
    new_timestamp = db_connector.get_last_successful_load_timestamp()
    assert new_timestamp is not None
    new_timestamp = new_timestamp.replace(tzinfo=timezone.utc)
    assert new_timestamp == timestamp
