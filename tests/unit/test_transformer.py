import unittest
from py_load_clinicaltrialsgov.transformer.transformer import Transformer
from py_load_clinicaltrialsgov.models.api_models import Study

class TestTransformer(unittest.TestCase):

    def test_transform_study(self):
        # Create a mock study object
        mock_study_data = {
            "protocolSection": {
                "identificationModule": {
                    "nctId": "NCT12345",
                    "briefTitle": "Test Study",
                    "officialTitle": "A Test Study for Science",
                },
                "statusModule": {
                    "overallStatus": "COMPLETED",
                    "startDateStruct": {"date": "2023-01"},
                    "primaryCompletionDateStruct": {"date": "2024-01-01"},
                },
                "designModule": {
                    "studyType": "INTERVENTIONAL",
                },
                "descriptionModule": {
                    "briefSummary": "This is a test study.",
                },
                "sponsorCollaboratorsModule": {
                    "leadSponsor": {"class": "INDUSTRY", "name": "TestCorp"}
                },
                "conditionsModule": {
                    "conditions": ["Test Condition 1", "Test Condition 2"]
                }
            },
            "derivedSection": {},
            "hasResults": False,
        }
        study = Study.model_validate(mock_study_data)

        transformer = Transformer()
        transformer.transform_study(study)
        dataframes = transformer.get_dataframes()

        # Assertions for raw_studies
        self.assertEqual(len(dataframes["raw_studies"]), 1)
        self.assertEqual(dataframes["raw_studies"].iloc[0]["nct_id"], "NCT12345")

        # Assertions for studies
        self.assertEqual(len(dataframes["studies"]), 1)
        self.assertEqual(dataframes["studies"].iloc[0]["brief_title"], "Test Study")
        self.assertIsNotNone(dataframes["studies"].iloc[0]["start_date"])


        # Assertions for sponsors
        self.assertEqual(len(dataframes["sponsors"]), 1)
        self.assertEqual(dataframes["sponsors"].iloc[0]["name"], "TestCorp")

        # Assertions for conditions
        self.assertEqual(len(dataframes["conditions"]), 2)

if __name__ == '__main__':
    unittest.main()
