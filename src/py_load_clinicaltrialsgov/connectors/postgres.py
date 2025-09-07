import pandas as pd
from typing import Dict, List, Any, Literal
from datetime import datetime, UTC

try:
    import psycopg
except ImportError:
    raise ImportError(
        "PostgreSQL dependencies are not installed. "
        "Please install the package with the 'postgres' extra: "
        "pip install py-load-clinicaltrialsgov[postgres]"
    )


from py_load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from py_load_clinicaltrialsgov.config import settings

class PostgresConnector(DatabaseConnectorInterface):
    """
    A PostgreSQL connector that implements the DatabaseConnectorInterface.
    """

    def __init__(self):
        self.conn = psycopg.connect(settings.db.dsn)
        # Define table metadata, including natural keys for merge logic
        self.table_metadata = {
            "raw_studies": {"pk": ["nct_id"]},
            "studies": {"pk": ["nct_id"]},
            # For child tables, the PK is the natural key, not the surrogate `id`
            "sponsors": {"pk": ["nct_id", "name", "agency_class"]},
            "conditions": {"pk": ["nct_id", "name"]},
            "interventions": {"pk": ["nct_id", "intervention_type", "name"]},
            "design_outcomes": {"pk": ["nct_id", "outcome_type", "measure"]},
        }

    def initialize_schema(self) -> None:
        """
        This method is now a no-op. Schema management is handled by Alembic.
        Use the `migrate-db` CLI command to create or update the schema.
        """
        pass

    def bulk_load_staging(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Bulk loads a DataFrame into a staging table using the COPY protocol.
        """
        import io

        staging_table_name = f"staging_{table_name}"
        with self.conn.cursor() as cur:
            # Truncate the staging table before loading
            cur.execute(f"TRUNCATE TABLE {staging_table_name}")

            # Create an in-memory CSV file
            csv_buffer = io.StringIO()
            data.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)

            # Use COPY to load the data
            with cur.copy(f"COPY {staging_table_name} FROM STDIN WITH (FORMAT CSV)") as copy:
                copy.write(csv_buffer.read())

    def execute_merge(self, table_name: str) -> None:
        """
        Merges data from a staging table to a final table using a generic
        and efficient INSERT ON CONFLICT (UPSERT) strategy.
        This implementation is now generic for all tables.
        """
        if table_name not in self.table_metadata:
            return

        staging_table_name = f"staging_{table_name}"
        # The conflict target is the natural primary key of the table.
        conflict_keys = self.table_metadata[table_name]["pk"]

        with self.conn.cursor() as cur:
            # Get column names from the final table, excluding the serial `id`
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s AND column_name != 'id'",
                (table_name,)
            )
            columns = [row[0] for row in cur.fetchall()]

            if not columns:
                return

            col_names = ", ".join(f'"{c}"' for c in columns)
            conflict_target = ", ".join(f'"{pk}"' for pk in conflict_keys)

            # All columns that are not part of the natural key will be updated on conflict.
            update_cols = ", ".join(
                f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in conflict_keys
            )

            # If all columns are part of the natural key, there's nothing to update.
            on_conflict_action = "DO NOTHING" if not update_cols else f"DO UPDATE SET {update_cols}"

            # First, for child tables, we must clear old records for the studies being updated.
            # This is necessary because we are replacing the entire set of child records for a study.
            if table_name not in ["studies", "raw_studies"]:
                cur.execute(f"""
                    DELETE FROM {table_name}
                    WHERE nct_id IN (SELECT DISTINCT nct_id FROM {staging_table_name})
                """)

            # Now, insert all the new records from the staging table.
            # Since we deleted the old ones, this is a simple insert.
            # The previous logic was flawed. A true UPSERT is not what is needed for these
            # child tables, as we want to replace the entire collection.
            # The correct pattern for "replace-all-child-records" is DELETE then INSERT.
            # The `ON CONFLICT` is for the parent `studies` table.

            if table_name in ["studies", "raw_studies"]:
                 merge_sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    SELECT {col_names} FROM {staging_table_name}
                    ON CONFLICT ({conflict_target}) {on_conflict_action}
                """
            else:
                # For child tables, we've already deleted, so we just insert.
                merge_sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    SELECT {col_names} FROM {staging_table_name}
                """

            cur.execute(merge_sql)

    def get_last_successful_load_timestamp(self) -> datetime | None:
        """
        Gets the timestamp of the last successful load from the load_history table.
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT MAX(load_timestamp) FROM load_history WHERE status = 'SUCCESS'")
            result = cur.fetchone()
            return result[0] if result else None

    def get_last_load_history(self) -> Dict[str, Any] | None:
        """
        Gets the most recent load history record.
        """
        with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT * FROM load_history ORDER BY load_timestamp DESC LIMIT 1")
            result = cur.fetchone()
            return result

    def record_load_history(self, status: str, metrics: Dict[str, Any]) -> None:
        """
        Records the outcome of an ETL run in the load_history table.
        """
        import json
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO load_history (load_timestamp, status, metrics) VALUES (%s, %s, %s)",
                (datetime.now(UTC), status, json.dumps(metrics))
            )

    def record_failed_study(
        self, nct_id: str, payload: Dict[str, Any], error_message: str
    ) -> None:
        """
        Logs a study that failed validation/transformation to the dead-letter queue.
        """
        import json
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dead_letter_queue (nct_id, payload, error_message)
                VALUES (%s, %s, %s)
                """,
                (nct_id, json.dumps(payload), error_message),
            )

    def manage_transaction(self, action: Literal["begin", "commit", "rollback"]) -> None:
        """
        Manages a database transaction.
        """
        if action == "begin":
            self.conn.autocommit = False
        elif action == "commit":
            self.conn.commit()
            self.conn.autocommit = True
        elif action == "rollback":
            self.conn.rollback()
            self.conn.autocommit = True
