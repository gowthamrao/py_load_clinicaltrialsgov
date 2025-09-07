import pandas as pd
from typing import Dict, List, Any, Literal
from datetime import datetime, UTC
import psycopg

from py_load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from py_load_clinicaltrialsgov.config import settings

class PostgresConnector(DatabaseConnectorInterface):
    """
    A PostgreSQL connector that implements the DatabaseConnectorInterface.
    """

    def __init__(self):
        self.conn = psycopg.connect(settings.db.dsn)

    def initialize_schema(self) -> None:
        """
        Initializes the database schema by executing the schema.sql file.
        """
        import importlib.resources
        from .. import sql

        schema_sql = importlib.resources.read_text(sql, "schema.sql")
        with self.conn.cursor() as cur:
            cur.execute(schema_sql)
        self.conn.commit()

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

    def execute_merge(self, table_name: str, primary_keys: List[str]) -> None:
        """
        Merges data from a staging table to a final table.
        """
        staging_table_name = f"staging_{table_name}"
        with self.conn.cursor() as cur:
            # Get column names from the staging table
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{staging_table_name}'")
            columns = [row[0] for row in cur.fetchall()]

            col_names = ", ".join(f'"{c}"' for c in columns)
            conflict_target = ", ".join(primary_keys)
            update_cols = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in primary_keys])

            if table_name == "studies" or table_name == "raw_studies":
                merge_sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    SELECT {col_names} FROM {staging_table_name}
                    ON CONFLICT ({conflict_target}) DO UPDATE SET {update_cols}
                """
                cur.execute(merge_sql)
            else:
                # For tables with surrogate keys, delete and insert
                # This is a simplification and could be optimized
                nct_ids_to_update_query = f"SELECT DISTINCT nct_id FROM {staging_table_name}"
                cur.execute(nct_ids_to_update_query)
                nct_ids = [row[0] for row in cur.fetchall()]

                if nct_ids:
                    delete_sql = f"DELETE FROM {table_name} WHERE nct_id = ANY(%s)"
                    cur.execute(delete_sql, (nct_ids,))

                insert_sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    SELECT {col_names} FROM {staging_table_name}
                """
                cur.execute(insert_sql)

    def get_last_successful_load_timestamp(self) -> datetime | None:
        """
        Gets the timestamp of the last successful load from the load_history table.
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT MAX(load_timestamp) FROM load_history WHERE status = 'SUCCESS'")
            result = cur.fetchone()
            return result[0] if result else None

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
