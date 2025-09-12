import pytest
from unittest.mock import MagicMock, patch
from load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from load_clinicaltrialsgov.config import settings
import pandas as pd


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
    assert f"COPY staging_{table_name}" in call_args[0][0]
