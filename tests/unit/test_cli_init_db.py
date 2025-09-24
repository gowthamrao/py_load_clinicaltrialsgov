# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


from typer.testing import CliRunner
from unittest.mock import MagicMock, patch
from pytest_structlog import StructuredLogCapture

from load_clinicaltrialsgov.cli import app

runner = CliRunner()


@patch("load_clinicaltrialsgov.cli.get_connector")
@patch("load_clinicaltrialsgov.cli.migrate_db")
def test_init_db_aborted(
    mock_migrate_db: MagicMock, mock_get_connector: MagicMock, log: StructuredLogCapture
) -> None:
    """
    Tests that the init-db command aborts if the user does not confirm.
    """
    result = runner.invoke(app, ["init-db"], input="n\n")

    assert result.exit_code != 0
    # typer.Abort() doesn't print "Aborted!" to stdout, but exits with a non-zero code.
    # The log message confirms the reason, and the exit code is the primary check.
    assert log.has("database_initialization_aborted", level="warning")
    mock_get_connector.assert_not_called()
    mock_migrate_db.assert_not_called()


@patch("load_clinicaltrialsgov.cli.get_connector")
@patch("load_clinicaltrialsgov.cli.migrate_db")
def test_init_db_successful(
    mock_migrate_db: MagicMock, mock_get_connector: MagicMock
) -> None:
    """
    Tests that the init-db command calls the correct functions when confirmed.
    """
    mock_connector = MagicMock()
    mock_get_connector.return_value = mock_connector

    result = runner.invoke(app, ["init-db"], input="y\n")

    assert result.exit_code == 0
    mock_get_connector.assert_called_once_with("postgres")
    mock_connector._dangerously_drop_all_tables.assert_called_once()
    mock_migrate_db.assert_called_once_with(revision="head")


@patch("load_clinicaltrialsgov.cli.get_connector")
@patch("load_clinicaltrialsgov.cli.migrate_db")
def test_init_db_force(
    mock_migrate_db: MagicMock, mock_get_connector: MagicMock
) -> None:
    """
    Tests that the init-db command runs without a prompt when --force is used.
    """
    mock_connector = MagicMock()
    mock_get_connector.return_value = mock_connector

    result = runner.invoke(app, ["init-db", "--force"])

    assert result.exit_code == 0
    mock_get_connector.assert_called_once_with("postgres")
    mock_connector._dangerously_drop_all_tables.assert_called_once()
    mock_migrate_db.assert_called_once_with(revision="head")
