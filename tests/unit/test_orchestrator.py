# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import unittest
from unittest.mock import MagicMock, call

from load_clinicaltrialsgov.orchestrator import Orchestrator
from load_clinicaltrialsgov.models.api_models import Study


class TestOrchestrator(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_connector = MagicMock()
        self.mock_api_client = MagicMock()
        self.mock_transformer = MagicMock()

        self.orchestrator = Orchestrator(
            connector=self.mock_connector,
            api_client=self.mock_api_client,
            transformer=self.mock_transformer,
        )

        # A valid study dict that can be processed
        self.valid_study_dict = {
            "protocolSection": {
                "identificationModule": {"nctId": "NCT00000001"},
                "statusModule": {"overallStatus": "COMPLETED"},
            },
            "derivedSection": {},
            "hasResults": False,
        }
        self.validated_study = Study.model_validate(self.valid_study_dict)

        # An invalid study dict that will cause a validation error
        self.invalid_study_dict = {
            "protocolSection": {
                # Missing statusModule, which is required, to trigger validation error
                "identificationModule": {"nctId": "NCT00000002"},
            },
            "derivedSection": {},
            "hasResults": False,
        }

    def test_orchestrator_handles_validation_error_gracefully(self) -> None:
        """
        Verify that the orchestrator can handle a Pydantic validation error.

        It should:
        1.  Record the failed study using the connector.
        2.  Continue processing other valid studies.
        3.  Successfully complete and commit the transaction.
        """
        # Arrange: API client will yield one valid and one invalid study
        self.mock_api_client.get_all_studies.return_value = iter(
            [self.valid_study_dict, self.invalid_study_dict]
        )

        # Arrange: The transformer will return a mock dataframe
        mock_df = MagicMock()
        mock_df.empty = False  # Simulate a non-empty dataframe
        self.mock_transformer.get_dataframes.return_value = {"studies": mock_df}
        self.mock_transformer.clear.return_value = None

        # Act
        self.orchestrator.run_etl(load_type="delta")

        # Assert
        # 1. The transformer was called only for the valid study
        self.mock_transformer.transform_study.assert_called_once()
        # We need to check the call argument is a Study object, not a dict
        args, _ = self.mock_transformer.transform_study.call_args
        self.assertIsInstance(args[0], Study)
        self.assertEqual(
            args[0].protocol_section.identification_module.nct_id, "NCT00000001"
        )

        # 2. The dead-letter queue was used for the invalid study
        self.mock_connector.record_failed_study.assert_called_once()
        # Check that the payload sent to dead-letter queue is the invalid one
        _, kwargs = self.mock_connector.record_failed_study.call_args
        self.assertEqual(
            kwargs["nct_id"], "NCT00000002"
        )  # nct_id should be parsed even if validation fails
        self.assertEqual(kwargs["payload"], self.invalid_study_dict)
        self.assertIn("Pydantic Validation Error", kwargs["error_message"])

        # 3. The transaction was successfully committed
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("commit")]
        )
        self.mock_connector.manage_transaction.assert_any_call("commit")
        self.mock_connector.manage_transaction.assert_any_call("begin")
        # Ensure rollback was NOT called
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)

    def test_orchestrator_handles_database_error(self) -> None:
        """
        Verify that the orchestrator can handle a database error during load.

        It should:
        1.  Attempt to load data.
        2.  Encounter a database error and roll back the transaction.
        3.  Record the failure in the load history.
        """
        # Arrange: API client will yield one valid study
        self.mock_api_client.get_all_studies.return_value = iter(
            [self.valid_study_dict]
        )

        # Arrange: The transformer will return a mock dataframe
        mock_df = MagicMock()
        mock_df.empty = False
        self.mock_transformer.get_dataframes.return_value = {"studies": mock_df}
        self.mock_transformer.clear.return_value = None

        # Arrange: The connector will raise an exception on bulk_load_staging
        self.mock_connector.bulk_load_staging.side_effect = Exception(
            "Database is on fire"
        )

        # Act
        self.orchestrator.run_etl(load_type="full")

        # Assert
        # 1. The transaction was rolled back
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("rollback")]
        )
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)

        # 2. The failure was recorded in the load history
        self.mock_connector.record_load_history.assert_called_once_with(
            "FAILURE",
            {"error": "Database is on fire", "duration_seconds": unittest.mock.ANY},
        )

    def test_orchestrator_handles_api_error(self) -> None:
        """
        Verify that the orchestrator can handle an API error during extraction.

        It should:
        1.  Attempt to fetch studies from the API.
        2.  Encounter an API error and roll back the transaction.
        3.  Record the failure in the load history.
        """
        # Arrange: API client will raise an exception
        self.mock_api_client.get_all_studies.side_effect = Exception("API is down")

        # Act
        self.orchestrator.run_etl(load_type="full")

        # Assert
        # 1. The transaction was rolled back
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("rollback")]
        )
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)

        # 2. The failure was recorded in the load history
        self.mock_connector.record_load_history.assert_called_once_with(
            "FAILURE", {"error": "API is down", "duration_seconds": unittest.mock.ANY}
        )

        # 3. No studies were processed
        self.mock_transformer.transform_study.assert_not_called()

    def test_orchestrator_handles_empty_iterator(self) -> None:
        """
        Verify that the orchestrator can handle an empty iterator from the API.
        """
        # Arrange: API client will yield an empty iterator
        self.mock_api_client.get_all_studies.return_value = iter([])

        # Act
        self.orchestrator.run_etl(load_type="full")

        # Assert
        # 1. The transaction was committed
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("commit")]
        )
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)

        # 2. A successful load was recorded
        self.mock_connector.record_load_history.assert_called_once_with(
            "SUCCESS", unittest.mock.ANY
        )

        # 3. No studies were processed
        self.mock_transformer.transform_study.assert_not_called()
        self.mock_connector.bulk_load_staging.assert_not_called()

    def test_orchestrator_handles_only_invalid_studies(self) -> None:
        """
        Verify that the orchestrator can handle an iterator with only invalid studies.
        """
        # Arrange: API client will yield only an invalid study
        self.mock_api_client.get_all_studies.return_value = iter(
            [self.invalid_study_dict]
        )

        # Act
        self.orchestrator.run_etl(load_type="full")

        # Assert
        # 1. The transaction was committed
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("commit")]
        )
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)

        # 2. The invalid study was recorded in the dead-letter queue
        self.mock_connector.record_failed_study.assert_called_once()

        # 3. No studies were transformed or loaded
        self.mock_transformer.transform_study.assert_not_called()
        self.mock_connector.bulk_load_staging.assert_not_called()

    def test_orchestrator_handles_transformer_error(self) -> None:
        """
        Verify that the orchestrator can handle a transformer error.

        It should:
        1.  Record the failed study using the connector.
        2.  Continue processing other valid studies.
        3.  Successfully complete and commit the transaction.
        """
        # Arrange: API client will yield one valid and one invalid study
        self.mock_api_client.get_all_studies.return_value = iter(
            [self.valid_study_dict, self.valid_study_dict]
        )

        # Arrange: The transformer will raise a generic exception on the first call
        self.mock_transformer.transform_study.side_effect = [
            TypeError("Transformer exploded"),
            None,
        ]

        # Arrange: The transformer will return a mock dataframe on the second call
        mock_df = MagicMock()
        mock_df.empty = False
        self.mock_transformer.get_dataframes.return_value = {"studies": mock_df}
        self.mock_transformer.clear.return_value = None

        # Act
        self.orchestrator.run_etl(load_type="full")

        # Assert
        # 1. The transformer was called for both studies
        self.assertEqual(self.mock_transformer.transform_study.call_count, 2)

        # 2. The dead-letter queue was used for the failed study
        self.mock_connector.record_failed_study.assert_called_once()
        _, kwargs = self.mock_connector.record_failed_study.call_args
        self.assertEqual(kwargs["nct_id"], "NCT00000001")
        self.assertIn(
            "Transformation Error: Transformer exploded", kwargs["error_message"]
        )

        # 3. The transaction was successfully committed
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("commit")]
        )
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)

    def test_load_and_clear_batch_no_primary_keys(self) -> None:
        """
        Test that _load_and_clear_batch logs an error if no primary keys are defined.
        """
        # Arrange
        import pandas as pd

        df = pd.DataFrame([{"nct_id": "NCT12345678"}])
        self.mock_transformer.get_dataframes.return_value = {"unknown_table": df}

        # Act
        self.orchestrator._load_and_clear_batch()

        # Assert
        self.mock_connector.bulk_load_staging.assert_not_called()

    def test_run_etl_exception_in_finally(self) -> None:
        """
        Test that run_etl handles exceptions in the finally block.
        """
        # Arrange
        self.mock_api_client.get_all_studies.return_value = iter([])
        self.mock_connector.record_load_history.side_effect = Exception(
            "Test exception"
        )

        # Act & Assert
        with self.assertRaises(Exception) as context:
            self.orchestrator.run_etl(load_type="full")
        self.assertTrue("Test exception" in str(context.exception))

    def test_run_etl_with_missing_nct_id(self) -> None:
        """
        Test that run_etl sends a study to the DLQ if it's missing nct_id.
        """
        # Arrange
        payload = {
            "protocolSection": {
                "identificationModule": {},
                "statusModule": {"overallStatus": "UNKNOWN"},
            },
        }
        self.mock_api_client.get_all_studies.return_value = iter([payload])

        # Act
        self.orchestrator.run_etl(load_type="full")

        # Assert
        self.mock_connector.record_failed_study.assert_called_once()
        _, kwargs = self.mock_connector.record_failed_study.call_args
        self.assertIsNone(kwargs["nct_id"])
        self.assertEqual(kwargs["payload"], payload)

    def test_orchestrator_handles_generic_transformation_error(self) -> None:
        """
        Verify that the orchestrator can handle a generic error during transformation.
        """
        # Arrange: API client will yield one valid study
        self.mock_api_client.get_all_studies.return_value = iter(
            [self.valid_study_dict]
        )

        # Arrange: The transformer will raise an exception
        self.mock_transformer.transform_study.side_effect = KeyError(
            "Generic transformation error"
        )

        # Act
        self.orchestrator.run_etl(load_type="full")

        # Assert
        # 1. The transaction was committed because the error is caught inside the loop
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("commit")]
        )
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)

        # 2. The failed study was recorded
        self.mock_connector.record_failed_study.assert_called_once()
        _, kwargs = self.mock_connector.record_failed_study.call_args
        self.assertEqual(kwargs["nct_id"], "NCT00000001")
        self.assertIn("Transformation Error", kwargs["error_message"])


if __name__ == "__main__":
    unittest.main()
