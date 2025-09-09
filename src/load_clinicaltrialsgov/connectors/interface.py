from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Literal

import pandas as pd


class DatabaseConnectorInterface(ABC):
    """
    Abstract Base Class for database connectors.

    This interface defines the contract that all database-specific loaders
    must implement to be compatible with the ETL pipeline.
    """

    @abstractmethod
    def _dangerously_drop_all_tables(self) -> None:
        """
        Drop all known tables from the database.

        This is a destructive operation intended to be used ONLY by the
        `init-db` CLI command to completely reset the database before
        running migrations from scratch.
        """
        raise NotImplementedError

    @abstractmethod
    def truncate_all_tables(self) -> None:
        """Truncate all main data tables."""
        raise NotImplementedError

    @abstractmethod
    def get_last_successful_load_history(self) -> Dict[str, Any] | None:
        """
        Retrieve the most recent successful entry from the load history.

        Returns:
            A dictionary representing the last successful load history record,
            or None if no successful loads have occurred.
        """
        raise NotImplementedError

    @abstractmethod
    def bulk_load_staging(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Efficiently load standardized data into a staging table.

        Args:
            table_name: The name of the staging table to load into.
            data: A pandas DataFrame containing the data to load.
        """
        raise NotImplementedError

    @abstractmethod
    def execute_merge(self, table_name: str, primary_keys: List[str]) -> None:
        """
        Perform an UPSERT/MERGE from a staging table to the final table.

        Args:
            table_name: The name of the final target table.
            primary_keys: A list of column names that form the natural
                          primary key for the merge operation.
        """
        raise NotImplementedError

    @abstractmethod
    def get_last_successful_load_timestamp(self) -> datetime | None:
        """
        Retrieve the high-water mark for delta loads.

        Returns:
            The timestamp of the last successful load, or None if no
            successful loads have occurred.
        """
        raise NotImplementedError

    @abstractmethod
    def get_last_load_history(self) -> Dict[str, Any] | None:
        """
        Retrieve the most recent entry from the load history.
        Returns:
            A dictionary representing the last load history record, or None.
        """
        raise NotImplementedError

    @abstractmethod
    def record_load_history(self, status: str, metrics: Dict[str, Any]) -> None:
        """
        Log the outcome of the ETL run in a history table.

        Args:
            status: The final status of the load (e.g., 'SUCCESS', 'FAILURE').
            metrics: A dictionary of metrics about the load.
        """
        raise NotImplementedError

    @abstractmethod
    def manage_transaction(
        self, action: Literal["begin", "commit", "rollback"]
    ) -> None:
        """
        Manage a database transaction.

        Args:
            action: The transaction action to perform.
        """
        raise NotImplementedError

    @abstractmethod
    def record_failed_study(
        self, nct_id: str, payload: Dict[str, Any], error_message: str
    ) -> None:
        """
        Log a study that failed validation/transformation to a dead-letter queue.

        Args:
            nct_id: The NCT ID of the failed study.
            payload: The raw JSON payload of the study.
            error_message: The error message from the validation/transformation failure.
        """
        raise NotImplementedError
