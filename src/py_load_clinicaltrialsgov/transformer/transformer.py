import pandas as pd
import json
from typing import Dict, List, Any
from py_load_clinicaltrialsgov.models.api_models import Study
from datetime import datetime, UTC


class Transformer:
    """
    Transforms raw study data from the API into normalized dataframes.
    """

    def __init__(self) -> None:
        self.raw_studies: List[Dict[str, Any]] = []
        self.studies: List[Dict[str, Any]] = []
        self.sponsors: List[Dict[str, Any]] = []
        self.conditions: List[Dict[str, Any]] = []
        self.interventions: List[Dict[str, Any]] = []
        self.design_outcomes: List[Dict[str, Any]] = []

    def transform_study(self, study: Study, raw_study_payload: Dict[str, Any]) -> None:
        """
        Transforms a single study object and appends the data to internal lists.
        """
        if (
            not study.protocol_section.identification_module
            or not study.protocol_section.identification_module.get("nctId")
        ):
            raise ValueError("Study is missing required nctId")

        nct_id = study.protocol_section.identification_module["nctId"]

        self._transform_raw_studies(nct_id, raw_study_payload)
        self._transform_studies_table(nct_id, study)
        self._transform_sponsors(nct_id, study)
        self._transform_conditions(nct_id, study)
        self._transform_interventions(nct_id, study)
        self._transform_outcomes(nct_id, study)

    def _transform_raw_studies(self, nct_id: str, raw_payload: Dict[str, Any]) -> None:
        last_updated_str = (
            raw_payload.get("protocolSection", {})
            .get("statusModule", {})
            .get("lastUpdatePostDateStruct", {})
            .get("date")
        )

        self.raw_studies.append(
            {
                "nct_id": nct_id,
                "last_updated_api": self._normalize_date(last_updated_str),
                "last_updated_api_str": last_updated_str,
                "ingestion_timestamp": datetime.now(UTC),
                "payload": json.dumps(raw_payload),
            }
        )

    def _transform_studies_table(self, nct_id: str, study: Study) -> None:
        start_date_str = None
        completion_date_str = None
        overall_status = None
        if study.protocol_section.status_module:
            start_date_str = study.protocol_section.status_module.get(
                "startDateStruct", {}
            ).get("date")
            completion_date_str = study.protocol_section.status_module.get(
                "primaryCompletionDateStruct", {}
            ).get("date")
            overall_status = study.protocol_section.status_module.get("overallStatus")

        study_type = None
        if study.protocol_section.design_module:
            study_type = study.protocol_section.design_module.get("studyType")

        brief_summary = None
        if study.protocol_section.description_module:
            brief_summary = study.protocol_section.description_module.get(
                "briefSummary"
            )

        # The identification_module is guaranteed to exist by the check
        # in the main transform_study method.
        identification_module = study.protocol_section.identification_module or {}

        self.studies.append(
            {
                "nct_id": nct_id,
                "brief_title": identification_module.get("briefTitle"),
                "official_title": identification_module.get("officialTitle"),
                "overall_status": overall_status,
                "start_date": self._normalize_date(start_date_str),
                "start_date_str": start_date_str,
                "primary_completion_date": self._normalize_date(completion_date_str),
                "primary_completion_date_str": completion_date_str,
                "study_type": study_type,
                "brief_summary": brief_summary,
            }
        )

    def _transform_sponsors(self, nct_id: str, study: Study) -> None:
        module = study.protocol_section.sponsor_collaborators_module
        if not module:
            return

        # Process the lead sponsor
        if module.lead_sponsor:
            self.sponsors.append(
                {
                    "nct_id": nct_id,
                    "agency_class": module.lead_sponsor.class_details,
                    "name": module.lead_sponsor.name,
                    "is_lead": True,
                }
            )

        # Process the collaborators
        if module.collaborators:
            for collaborator in module.collaborators:
                self.sponsors.append(
                    {
                        "nct_id": nct_id,
                        "agency_class": collaborator.class_details,
                        "name": collaborator.name,
                        "is_lead": False,
                    }
                )

    def _transform_conditions(self, nct_id: str, study: Study) -> None:
        if study.protocol_section.conditions_module:
            conditions = study.protocol_section.conditions_module.get("conditions", [])
            for condition in conditions:
                self.conditions.append({"nct_id": nct_id, "name": condition})

    def _transform_interventions(self, nct_id: str, study: Study) -> None:
        if (
            not study.protocol_section.arms_interventions_module
            or not study.protocol_section.arms_interventions_module.interventions
        ):
            return
        for (
            intervention
        ) in study.protocol_section.arms_interventions_module.interventions:
            self.interventions.append(
                {
                    "nct_id": nct_id,
                    "intervention_type": intervention.type,
                    "name": intervention.name,
                    "description": intervention.description,
                }
            )

    def _transform_outcomes(self, nct_id: str, study: Study) -> None:
        if not study.protocol_section.outcomes_module:
            return

        outcomes_module = study.protocol_section.outcomes_module
        if outcomes_module.primary_outcomes:
            for outcome in outcomes_module.primary_outcomes:
                self.design_outcomes.append(
                    {
                        "nct_id": nct_id,
                        "outcome_type": "PRIMARY",
                        "measure": outcome.measure,
                        "time_frame": outcome.time_frame,
                        "description": outcome.description,
                    }
                )

        if outcomes_module.secondary_outcomes:
            for outcome in outcomes_module.secondary_outcomes:
                self.design_outcomes.append(
                    {
                        "nct_id": nct_id,
                        "outcome_type": "SECONDARY",
                        "measure": outcome.measure,
                        "time_frame": outcome.time_frame,
                        "description": outcome.description,
                    }
                )

    def _normalize_date(self, date_str: str | None) -> datetime | None:
        """
        Normalizes a date string to a datetime object.
        Handles formats like 'YYYY-MM' by defaulting to the first day of the month.
        """
        if not date_str:
            return None
        try:
            # First, try to parse as a full date
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            try:
                # If that fails, try to parse as 'Month YYYY' (e.g., "January 2023")
                return datetime.strptime(date_str, "%B %Y")
            except ValueError:
                try:
                    # If that fails, try to parse as 'YYYY-MM'
                    return datetime.strptime(date_str, "%Y-%m")
                except ValueError:
                    try:
                        # Finally, try to parse as 'YYYY'
                        return datetime.strptime(date_str, "%Y")
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
            "interventions": pd.DataFrame(self.interventions),
            "design_outcomes": pd.DataFrame(self.design_outcomes),
        }
        # Filter out empty dataframes
        return {name: df for name, df in dataframes.items() if not df.empty}

    def clear(self) -> None:
        """
        Resets the internal data lists to clear the state for the next batch.
        """
        self.raw_studies.clear()
        self.studies.clear()
        self.sponsors.clear()
        self.conditions.clear()
        self.interventions.clear()
        self.design_outcomes.clear()
