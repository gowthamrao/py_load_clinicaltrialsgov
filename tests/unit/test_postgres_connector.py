from unittest.mock import MagicMock, patch
import pytest
from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.config import settings
import pandas as pd
from psycopg import sql


@patch("psycopg.connect")
def test_postgres_connector_initialization(mock_connect: MagicMock) -> None:
    """
    Test that the PostgresConnector initializes correctly and calls psycopg.connect.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    dsn = "postgresql://user:password@host:5432/dbname"
    settings.db.dsn = dsn

    # Act
    connector = PostgresConnector()

    # Assert
    mock_connect.assert_called_once_with(dsn)
    assert connector.conn == mock_conn


@patch("psycopg.connect")
def test_bulk_load_staging(mock_connect: MagicMock) -> None:
    """
    Test the bulk_load_staging method.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    table_name = "test_table"

    # Act
    connector.bulk_load_staging(table_name, df)

    # Assert
    assert mock_cursor.copy.call_count == 1
    call_args = mock_cursor.copy.call_args
    composed_sql = call_args[0][0]

    assert isinstance(composed_sql, sql.Composed)
    # The composed object should be like:
    # SQL('COPY ') + Identifier('staging_test_table') + SQL(' (') + Composed([Identifier('col1'), SQL(', '), Identifier('col2')]) + SQL(') FROM STDIN')
    assert composed_sql._obj[0] == sql.SQL("COPY ")
    assert composed_sql._obj[1] == sql.Identifier("staging_test_table")
    assert composed_sql._obj[2] == sql.SQL(" (")
    assert isinstance(composed_sql._obj[3], sql.Composed)
    assert composed_sql._obj[3]._obj[0] == sql.Identifier("col1")
    assert composed_sql._obj[3]._obj[1] == sql.SQL(", ")
    assert composed_sql._obj[3]._obj[2] == sql.Identifier("col2")
    assert composed_sql._obj[4] == sql.SQL(") FROM STDIN")


@patch("psycopg.connect")
def test_bulk_load_staging_with_special_chars_in_column_name(
    mock_connect: MagicMock,
) -> None:
    """
    Test that bulk_load_staging correctly quotes column names with special characters.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()
    df = pd.DataFrame({"col 1": [1, 2]})
    table_name = "test_table"

    # Act
    connector.bulk_load_staging(table_name, df)

    # Assert
    assert mock_cursor.copy.call_count == 1
    call_args = mock_cursor.copy.call_args
    composed_sql = call_args[0][0]

    assert isinstance(composed_sql, sql.Composed)
    assert composed_sql._obj[3]._obj[0] == sql.Identifier("col 1")


@patch("psycopg.connect")
def test_bulk_load_staging_with_empty_dataframe(mock_connect: MagicMock) -> None:
    """
    Test that bulk_load_staging does nothing when given an empty DataFrame.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()
    df = pd.DataFrame()
    table_name = "test_table"

    # Act
    connector.bulk_load_staging(table_name, df)

    # Assert
    mock_cursor.copy.assert_not_called()


@patch("psycopg.connect")
def test_dangerously_drop_all_tables_no_tables(mock_connect: MagicMock) -> None:
    """
    Test that _dangerously_drop_all_tables does nothing when there are no tables.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()

    # Act
    connector._dangerously_drop_all_tables()

    # Assert
    # The initial SELECT is executed, but no DROP TABLE statement should be.
    assert mock_cursor.execute.call_count == 1


@patch("psycopg.connect")
def test_execute_merge_with_no_primary_keys(mock_connect: MagicMock) -> None:
    """
    Test that execute_merge performs a simple INSERT when no primary keys are provided.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("col1",), ("col2",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()
    table_name = "test_table"

    # Act
    connector.execute_merge(table_name, [])

    # Assert
    mock_cursor.execute.assert_any_call(
        sql.SQL(
            "INSERT INTO {table} ({col_names}) SELECT {col_names} FROM {staging_table};"
        ).format(
            table=sql.Identifier(table_name),
            col_names=sql.SQL(", ").join(
                [sql.Identifier("col1"), sql.Identifier("col2")]
            ),
            staging_table=sql.Identifier(f"staging_{table_name}"),
        )
    )


def test_postgres_connector_import_error() -> None:
    """
    Test that an ImportError is raised if psycopg is not installed.
    """
    import importlib

    with patch.dict("sys.modules", {"psycopg": None}):
        import load_clinicaltrialsgov.connectors.postgres as postgres_module

        with pytest.raises(ImportError):
            importlib.reload(postgres_module)


@patch("psycopg.connect")
def test_execute_merge_child_table(mock_connect: MagicMock) -> None:
    """
    Test that execute_merge uses 'delete then insert' for child tables.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("NCT123",)],  # parent_ids
        [("nct_id",), ("col2",)],  # columns
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()
    table_name = "test_child_table"
    primary_keys = ["nct_id", "col2"]

    # Act
    connector.execute_merge(table_name, primary_keys)

    # Assert
    # Check that DELETE was called
    mock_cursor.execute.assert_any_call(
        sql.SQL("DELETE FROM {} WHERE nct_id = ANY(%s)").format(
            sql.Identifier(table_name)
        ),
        (["NCT123"],),
    )

    # Check that the final INSERT uses DO NOTHING
    mock_cursor.execute.assert_any_call(
        sql.SQL(
            "INSERT INTO {table} ({col_names}) "
            "SELECT {col_names} FROM {staging_table} "
            "ON CONFLICT ({conflict_target}) {on_conflict_action};"
        ).format(
            table=sql.Identifier(table_name),
            col_names=sql.SQL(", ").join(
                [sql.Identifier("nct_id"), sql.Identifier("col2")]
            ),
            staging_table=sql.Identifier(f"staging_{table_name}"),
            conflict_target=sql.SQL(", ").join(
                [sql.Identifier("nct_id"), sql.Identifier("col2")]
            ),
            on_conflict_action=sql.SQL("DO NOTHING"),
        )
    )


@patch("psycopg.connect")
def test_execute_merge_parent_table(mock_connect: MagicMock) -> None:
    """
    Test that execute_merge uses UPSERT for parent tables.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("nct_id",), ("col2",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()
    table_name = "test_parent_table"
    primary_keys = ["nct_id"]

    # Act
    connector.execute_merge(table_name, primary_keys)

    # Assert
    expected_sql = sql.SQL(
        "INSERT INTO {table} ({col_names}) "
        "SELECT {col_names} FROM {staging_table} "
        "ON CONFLICT ({conflict_target}) {on_conflict_action};"
    ).format(
        table=sql.Identifier(table_name),
        col_names=sql.SQL(", ").join(
            [sql.Identifier("nct_id"), sql.Identifier("col2")]
        ),
        staging_table=sql.Identifier(f"staging_{table_name}"),
        conflict_target=sql.SQL(", ").join([sql.Identifier("nct_id")]),
        on_conflict_action=sql.SQL("DO UPDATE SET {}").format(
            sql.SQL(", ").join(
                [
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier("col2"), sql.Identifier("col2")
                    )
                ]
            )
        ),
    )
    mock_cursor.execute.assert_any_call(expected_sql)


@patch("psycopg.connect")
def test_get_last_successful_load_timestamp_empty(mock_connect: MagicMock) -> None:
    """
    Test get_last_successful_load_timestamp returns None when history is empty.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    connector = PostgresConnector()

    result = connector.get_last_successful_load_timestamp()

    assert result is None


@patch("psycopg.connect")
def test_get_last_load_history_empty(mock_connect: MagicMock) -> None:
    """
    Test get_last_load_history returns None when history is empty.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    connector = PostgresConnector()

    result = connector.get_last_load_history()

    assert result is None


@patch("psycopg.connect")
def test_get_last_successful_load_history_empty(mock_connect: MagicMock) -> None:
    """
    Test get_last_successful_load_history returns None when history is empty.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    connector = PostgresConnector()

    result = connector.get_last_successful_load_history()

    assert result is None


@patch("psycopg.connect")
def test_manage_transaction(mock_connect: MagicMock) -> None:
    """
    Test transaction management.
    """
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    connector = PostgresConnector()

    connector.manage_transaction("begin")
    assert connector.conn.autocommit is False

    connector.manage_transaction("commit")
    mock_conn.commit.assert_called_once()
    assert connector.conn.autocommit is True

    connector.manage_transaction("rollback")
    mock_conn.rollback.assert_called_once()
    assert connector.conn.autocommit is True


@patch("psycopg.connect")
@patch("json.dumps")
def test_record_load_history(
    mock_json_dumps: MagicMock, mock_connect: MagicMock
) -> None:
    """
    Test recording load history.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    connector = PostgresConnector()

    connector.record_load_history("SUCCESS", {"studies": 10})

    assert mock_cursor.execute.call_count == 1
    mock_json_dumps.assert_called_once_with({"studies": 10})


@patch("psycopg.connect")
@patch("json.dumps")
def test_record_failed_study(
    mock_json_dumps: MagicMock, mock_connect: MagicMock
) -> None:
    """
    Test recording a failed study.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    connector = PostgresConnector()

    connector.record_failed_study("NCT123", {"payload": "data"}, "error")

    assert mock_cursor.execute.call_count == 1
    mock_json_dumps.assert_called_once_with({"payload": "data"})


@patch("psycopg.connect")
def test_truncate_all_tables(mock_connect: MagicMock) -> None:
    """
    Test truncating all tables.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    connector = PostgresConnector()

    connector.truncate_all_tables()

    assert mock_cursor.execute.call_count == 1


@patch("psycopg.connect")
def test_dangerously_drop_all_tables(mock_connect: MagicMock) -> None:
    """
    Test that _dangerously_drop_all_tables works when there are tables.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()

    # Act
    connector._dangerously_drop_all_tables()

    # Assert
    assert mock_cursor.execute.call_count == 2
    mock_cursor.execute.assert_any_call(
        sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
            sql.SQL(", ").join([sql.Identifier("table1"), sql.Identifier("table2")])
        )
    )


@patch("psycopg.connect")
def test_execute_merge_no_columns(mock_connect: MagicMock) -> None:
    """
    Test that execute_merge returns early if no columns are found.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []  # No columns
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    connector = PostgresConnector()
    table_name = "test_table"
    primary_keys = ["id"]

    # Act
    connector.execute_merge(table_name, primary_keys)

    # Assert
    # The initial SELECT is executed, but no INSERT statement should be.
    assert mock_cursor.execute.call_count == 1
