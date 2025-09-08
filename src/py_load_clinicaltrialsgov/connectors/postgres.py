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

    def execute_merge(
        self,
        table_name: str,
        primary_keys: List[str],
        strategy: Literal["upsert", "delete_insert"],
    ) -> None:
        """
        Merges data from a staging table to a final table using either a pure
        UPSERT strategy or a DELETE/INSERT strategy.
        """
        staging_table_name = f"staging_{table_name}"

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s AND column_name != 'id'",
                (table_name,),
            )
            columns = [row[0] for row in cur.fetchall()]

            if not columns:
                return

            col_names = ", ".join(f'"{c}"' for c in columns)

            if strategy == "upsert":
                conflict_target = ", ".join(f'"{pk}"' for pk in primary_keys)
                update_cols = [col for col in columns if col not in primary_keys]

                if not update_cols:
                    on_conflict_action = "DO NOTHING"
                else:
                    # Create the "col" = EXCLUDED."col" part for each column
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
            elif strategy == "delete_insert":
                cur.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE nct_id IN (SELECT DISTINCT nct_id FROM {staging_table_name});
                    """
                )
                merge_sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    SELECT {col_names} FROM {staging_table_name};
                """
            else:
                raise ValueError(f"Unknown merge strategy: {strategy}")

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

    def get_last_successful_load_history(self) -> Dict[str, Any] | None:
        """
        Gets the most recent successful load history record.
        """
        with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT * FROM load_history WHERE status = 'SUCCESS' ORDER BY load_timestamp DESC LIMIT 1"
            )
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
