import unittest
from py_load_clinicaltrialsgov.transformer.transformer import Transformer
from py_load_clinicaltrialsgov.models.api_models import Study


class TestTransformer(unittest.TestCase):
    def test_transform_study(self) -> None:
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
                },
            },
            "derivedSection": {},
            "hasResults": False,
        }
        study = Study.model_validate(mock_study_data)

        transformer = Transformer()
        transformer.transform_study(study, mock_study_data)
        dataframes = transformer.get_dataframes()

        # Assertions for raw_studies
        self.assertEqual(len(dataframes["raw_studies"]), 1)
        self.assertEqual(dataframes["raw_studies"].iloc[0]["nct_id"], "NCT12345")

        # Assertions for studies
        self.assertEqual(len(dataframes["studies"]), 1)
        self.assertEqual(dataframes["studies"].iloc[0]["brief_title"], "Test Study")
        self.assertIsNotNone(dataframes["studies"].iloc[0]["start_date"])
        self.assertEqual(dataframes["studies"].iloc[0]["start_date_str"], "2023-01")
        self.assertEqual(
            dataframes["studies"].iloc[0]["primary_completion_date_str"], "2024-01-01"
        )

        # Assertions for sponsors
        self.assertEqual(len(dataframes["sponsors"]), 1)
        self.assertEqual(dataframes["sponsors"].iloc[0]["name"], "TestCorp")

        # Assertions for conditions
        self.assertEqual(len(dataframes["conditions"]), 2)

    def test_normalize_date(self) -> None:
        from datetime import datetime

        transformer = Transformer()

        # Test valid full date
        self.assertEqual(
            transformer._normalize_date("2023-05-15"), datetime(2023, 5, 15)
        )

        # Test valid month-year date
        self.assertEqual(transformer._normalize_date("2023-07"), datetime(2023, 7, 1))

        # Test valid long month-year date
        self.assertEqual(
            transformer._normalize_date("February 2024"), datetime(2024, 2, 1)
        )

        # Test invalid date string
        self.assertIsNone(transformer._normalize_date("invalid-date"))

        # Test None input
        self.assertIsNone(transformer._normalize_date(None))

        # Test year-only date
        self.assertEqual(transformer._normalize_date("2025"), datetime(2025, 1, 1))

    def test_transform_study_with_collaborators(self) -> None:
        mock_study_data = {
            "protocolSection": {
                "identificationModule": {"nctId": "NCT54321"},
                "statusModule": {
                    "startDateStruct": {"date": "2022"},
                },
                "sponsorCollaboratorsModule": {
                    "leadSponsor": {"class": "INDUSTRY", "name": "Lead Sponsor Inc."},
                    "collaborators": [
                        {"class": "NIH", "name": "Collaborator 1"},
                        {"class": "OTHER", "name": "Collaborator 2"},
                    ],
                },
            },
            "derivedSection": {},
            "hasResults": False,
        }
        study = Study.model_validate(mock_study_data)
        transformer = Transformer()
        transformer.transform_study(study, mock_study_data)
        dataframes = transformer.get_dataframes()

        # Assertions for sponsors
        sponsors_df = dataframes["sponsors"]
        self.assertEqual(len(sponsors_df), 3)

        lead_sponsor = sponsors_df[sponsors_df["is_lead"] == True]
        self.assertEqual(len(lead_sponsor), 1)
        self.assertEqual(lead_sponsor.iloc[0]["name"], "Lead Sponsor Inc.")

        collaborators = sponsors_df[sponsors_df["is_lead"] == False]
        self.assertEqual(len(collaborators), 2)
        self.assertIn("Collaborator 1", collaborators["name"].values)
        self.assertIn("Collaborator 2", collaborators["name"].values)

        # Assertions for date parsing
        studies_df = dataframes["studies"]
        self.assertEqual(studies_df.iloc[0]["start_date"].year, 2022)
        self.assertEqual(studies_df.iloc[0]["start_date"].month, 1)
        self.assertEqual(studies_df.iloc[0]["start_date"].day, 1)


if __name__ == "__main__":
    unittest.main()
