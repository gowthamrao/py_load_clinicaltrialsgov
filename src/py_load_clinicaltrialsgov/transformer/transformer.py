import pandas as pd
from typing import Dict, List, Tuple
from py_load_clinicaltrialsgov.models.api_models import Study
import json
from datetime import datetime, UTC

class Transformer:
    """
    Transforms raw study data from the API into normalized dataframes.
    """

    def __init__(self):
        self.raw_studies = []
        self.studies = []
        self.sponsors = []
        self.conditions = []
        # Add other lists for other tables as needed

    def transform_study(self, study: Study) -> None:
        """
        Transforms a single study object and appends the data to internal lists.
        """
        nct_id = study.protocol_section.identification_module.get("nctId") if study.protocol_section.identification_module else None

        if not nct_id:
            # Or handle this error more gracefully
            return

        # Store raw JSON
        self.raw_studies.append({
            "nct_id": nct_id,
            "last_updated_api": study.protocol_section.status_module.get("lastUpdatePostDateStruct", {}).get("date") if study.protocol_section.status_module else None,
            "ingestion_timestamp": datetime.now(UTC),
            "payload": study.model_dump_json(by_alias=True)
        })

        # Transform main study information
        start_date_str = study.protocol_section.status_module.get("startDateStruct", {}).get("date") if study.protocol_section.status_module else None
        completion_date_str = study.protocol_section.status_module.get("primaryCompletionDateStruct", {}).get("date") if study.protocol_section.status_module else None

        self.studies.append({
            "nct_id": nct_id,
            "brief_title": study.protocol_section.identification_module.get("briefTitle") if study.protocol_section.identification_module else None,
            "official_title": study.protocol_section.identification_module.get("officialTitle") if study.protocol_section.identification_module else None,
            "overall_status": study.protocol_section.status_module.get("overallStatus") if study.protocol_section.status_module else None,
            "start_date": self._normalize_date(start_date_str),
            "start_date_str": start_date_str,
            "primary_completion_date": self._normalize_date(completion_date_str),
            "primary_completion_date_str": completion_date_str,
            "study_type": study.protocol_section.design_module.get("studyType") if study.protocol_section.design_module else None,
            "brief_summary": study.protocol_section.description_module.get("briefSummary") if study.protocol_section.description_module else None,
        })

        # Transform sponsors
        if study.protocol_section.sponsor_collaborators_module:
            lead_sponsor = study.protocol_section.sponsor_collaborators_module.get("leadSponsor", {})
            if lead_sponsor:
                self.sponsors.append({
                    "nct_id": nct_id,
                    "agency_class": lead_sponsor.get("class"),
                    "name": lead_sponsor.get("name"),
                    "is_lead": True,
                })

        # Transform conditions
        if study.protocol_section.conditions_module:
            conditions = study.protocol_section.conditions_module.get("conditions", [])
            for condition in conditions:
                self.conditions.append({
                    "nct_id": nct_id,
                    "name": condition,
                })


    def _normalize_date(self, date_str: str | None) -> datetime | None:
        """
        Normalizes a date string to a datetime object.
        Handles formats like 'YYYY-MM' by defaulting to the first day of the month.
        """
        if not date_str:
            return None
        try:
            # First, try to parse as a full date
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            try:
                # If that fails, try to parse as 'YYYY-MM'
                return datetime.strptime(date_str, '%Y-%m')
            except ValueError:
                # Add more formats here if needed
                return None

    def get_dataframes(self) -> Dict[str, pd.DataFrame]:
        """
        Returns a dictionary of pandas DataFrames for each table.
        """
        dataframes = {
            "raw_studies": pd.DataFrame(self.raw_studies),
            "studies": pd.DataFrame(self.studies),
            "sponsors": pd.DataFrame(self.sponsors),
            "conditions": pd.DataFrame(self.conditions),
        }
        # Filter out empty dataframes
        return {name: df for name, df in dataframes.items() if not df.empty}
