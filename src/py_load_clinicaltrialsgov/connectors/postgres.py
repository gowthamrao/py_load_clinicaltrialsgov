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

    def __init__(self) -> None:
        self.conn = psycopg.connect(settings.db.dsn)

    def _dangerously_drop_all_tables(self) -> None:
        """
        Drop all tables in the public schema.

        This is a destructive operation intended to be used ONLY by the
        `init-db` CLI command to completely reset the database before
        running migrations from scratch. It dynamically finds all tables
        in the 'public' schema and drops them.
        """
        with self.conn.cursor() as cur:
            # Get all table names in the public schema
            cur.execute(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"
            )
            tables = [row[0] for row in cur.fetchall()]

            if tables:
                # DROP TABLE is a DDL command and runs in its own transaction.
                # We use CASCADE to drop dependent objects like views or constraints.
                table_list = ", ".join(f'"{table}"' for table in tables)
                cur.execute(f"DROP TABLE IF EXISTS {table_list} CASCADE")

        self.conn.commit()

    def truncate_all_tables(self) -> None:
        """
        Truncates all data tables in the schema.
        """
        tables_to_truncate = [
            "raw_studies",
            "studies",
            "sponsors",
            "conditions",
            "interventions",
            "design_outcomes",
        ]
        with self.conn.cursor() as cur:
            # TRUNCATE is a DDL command and runs in its own transaction.
            # We use RESTART IDENTITY to reset any auto-incrementing counters.
            # We use CASCADE to drop dependent objects, though none are expected.
            cur.execute(
                f"TRUNCATE TABLE {', '.join(tables_to_truncate)} RESTART IDENTITY CASCADE"
            )
        self.conn.commit()

    def bulk_load_staging(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Bulk loads a DataFrame into a staging table using the COPY protocol.

        This implementation uses an iterative approach with `copy.write_row`
        to stream data efficiently without creating a large intermediate
        CSV file in memory.
        """
        staging_table_name = f"staging_{table_name}"
        # The columns in the DataFrame must match the order in the table.
        # It's safer to specify them explicitly in the COPY command.
        cols = ",".join(f'"{c}"' for c in data.columns)

        with self.conn.cursor() as cur:
            # Truncate the staging table before loading
            cur.execute(f"TRUNCATE TABLE {staging_table_name}")

            # Use COPY with write_row for efficient, iterative loading.
            with cur.copy(f"COPY {staging_table_name} ({cols}) FROM STDIN") as copy:
                for row in data.itertuples(index=False, name=None):
                    copy.write_row(row)

    def execute_merge(self, table_name: str, primary_keys: List[str]) -> None:
        """
        Merges data from a staging table to a final table.
        - For simple parent tables (like 'studies'), it uses UPSERT.
        - For child tables (like 'conditions', 'interventions'), it uses a
          "delete then insert" strategy to handle cases where child records
          are removed in a new data load.
        """
        staging_table_name = f"staging_{table_name}"
        is_child_table = "nct_id" in primary_keys and len(primary_keys) > 1

        with self.conn.cursor() as cur:
            if is_child_table:
                # "Delete then insert" for child tables
                # Get the parent IDs (nct_id) from the staging table
                cur.execute(f"SELECT DISTINCT nct_id FROM {staging_table_name}")
                parent_ids = [row[0] for row in cur.fetchall()]

                if parent_ids:
                    # Delete all existing child records for these parent IDs
                    cur.execute(
                        f"DELETE FROM {table_name} WHERE nct_id = ANY(%s)",
                        (parent_ids,),
                    )

            # Now, perform the insert/upsert
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s AND column_name != 'id'",
                (table_name,),
            )
            columns = [row[0] for row in cur.fetchall()]

            if not columns:
                return

            col_names = ", ".join(f'"{c}"' for c in columns)
            conflict_target = ", ".join(f'"{pk}"' for pk in primary_keys)
            update_cols = [col for col in columns if col not in primary_keys]

            if not update_cols or is_child_table:
                # If it's a child table, we've already deleted, so we just insert.
                on_conflict_action = "DO NOTHING"
            else:
                update_assignments = [
                    f'"{col}" = EXCLUDED."{col}"' for col in update_cols
                ]
                update_set = ", ".join(update_assignments)
                on_conflict_action = f"DO UPDATE SET {update_set}"

            merge_sql = f"""
                INSERT INTO {table_name} ({col_names})
                SELECT {col_names} FROM {staging_table_name}
                ON CONFLICT ({conflict_target}) {on_conflict_action};
            """
            cur.execute(merge_sql)

    def get_last_successful_load_timestamp(self) -> datetime | None:
        """
        Gets the timestamp of the last successful load from the load_history table.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(load_timestamp) FROM load_history WHERE status = 'SUCCESS'"
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_last_load_history(self) -> Dict[str, Any] | None:
        """
        Gets the most recent load history record.
        """
        with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT * FROM load_history ORDER BY load_timestamp DESC LIMIT 1"
            )
            result = cur.fetchone()
            return dict(result) if result else None

    def get_last_successful_load_history(self) -> Dict[str, Any] | None:
        """
        Gets the most recent successful load history record.
        """
        with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT * FROM load_history WHERE status = 'SUCCESS' ORDER BY load_timestamp DESC LIMIT 1"
            )
            result = cur.fetchone()
            return dict(result) if result else None

    def record_load_history(self, status: str, metrics: Dict[str, Any]) -> None:
        """
        Records the outcome of an ETL run in the load_history table.
        """
        import json

        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO load_history (load_timestamp, status, metrics) VALUES (%s, %s, %s)",
                (datetime.now(UTC), status, json.dumps(metrics)),
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

    def manage_transaction(
        self, action: Literal["begin", "commit", "rollback"]
    ) -> None:
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
