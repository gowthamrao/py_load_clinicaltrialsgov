import pytest
from load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface

from typing import Any, Dict, List, Literal
import pandas as pd
from datetime import datetime


class DummyConnector(DatabaseConnectorInterface):
    """A dummy connector for testing the interface's abstract methods."""

    def _dangerously_drop_all_tables(self) -> None:
        pass

    def truncate_all_tables(self) -> None:
        pass

    def get_last_successful_load_history(self) -> Dict[str, Any] | None:
        return None

    def bulk_load_staging(self, table_name: str, data: pd.DataFrame) -> None:
        pass

    def execute_merge(self, table_name: str, primary_keys: List[str]) -> None:
        pass

    def get_last_successful_load_timestamp(self) -> datetime | None:
        return None

    def get_last_load_history(self) -> Dict[str, Any] | None:
        return None

    def record_load_history(self, status: str, metrics: Dict[str, Any]) -> None:
        pass

    def manage_transaction(
        self, action: Literal["begin", "commit", "rollback"]
    ) -> None:
        pass

    def record_failed_study(
        self, nct_id: str, payload: Dict[str, Any], error_message: str
    ) -> None:
        pass


@pytest.fixture
def dummy_connector() -> DummyConnector:
    """Returns an instance of the DummyConnector."""
    return DummyConnector()
