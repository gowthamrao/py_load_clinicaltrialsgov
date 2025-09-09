import unittest
from unittest.mock import MagicMock, patch, call
from pydantic import ValidationError

from py_load_clinicaltrialsgov.orchestrator import Orchestrator
from py_load_clinicaltrialsgov.models.api_models import Study


class TestOrchestrator(unittest.TestCase):
    def setUp(self):
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

    def test_orchestrator_handles_validation_error_gracefully(self):
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
        self.assertEqual(args[0].protocol_section.identification_module.nct_id, "NCT00000001")


        # 2. The dead-letter queue was used for the invalid study
        self.mock_connector.record_failed_study.assert_called_once()
        # Check that the payload sent to dead-letter queue is the invalid one
        _, kwargs = self.mock_connector.record_failed_study.call_args
        self.assertEqual(kwargs['nct_id'], "NCT00000002") # nct_id should be parsed even if validation fails
        self.assertEqual(kwargs['payload'], self.invalid_study_dict)
        self.assertIn("Pydantic Validation Error", kwargs['error_message'])

        # 3. The transaction was successfully committed
        self.mock_connector.manage_transaction.assert_has_calls(
            [call("begin"), call("commit")]
        )
        self.mock_connector.manage_transaction.assert_any_call("commit")
        self.mock_connector.manage_transaction.assert_any_call("begin")
        # Ensure rollback was NOT called
        self.assertEqual(self.mock_connector.manage_transaction.call_count, 2)


if __name__ == "__main__":
    unittest.main()
