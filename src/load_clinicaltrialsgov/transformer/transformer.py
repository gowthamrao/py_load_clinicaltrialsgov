# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import pandas as pd
import json
import structlog
from typing import Dict, List, Any
from load_clinicaltrialsgov.models.api_models import Study
from datetime import datetime, UTC
from dateutil.parser import parse as date_parse, ParserError

logger = structlog.get_logger(__name__)


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
        self.intervention_arm_groups: List[Dict[str, Any]] = []
        self.design_outcomes: List[Dict[str, Any]] = []

    def transform_study(self, study: Study, raw_study_payload: Dict[str, Any]) -> None:
        """
        Transforms a single study object and appends the data to internal lists.
        """
        nct_id = study.protocol_section.identification_module.nct_id

        self._transform_raw_studies(nct_id, raw_study_payload, study)
        self._transform_studies_table(nct_id, study)
        self._transform_sponsors(nct_id, study)
        self._transform_conditions(nct_id, study)
        self._transform_interventions(nct_id, study)
        self._transform_intervention_arm_groups(nct_id, study)
        self._transform_outcomes(nct_id, study)

    def _transform_raw_studies(
        self, nct_id: str, raw_payload: Dict[str, Any], study: Study
    ) -> None:
        last_updated_str = (
            study.protocol_section.status_module.last_update_post_date_struct.date
            if study.protocol_section.status_module.last_update_post_date_struct
            else None
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
        id_module = study.protocol_section.identification_module
        status_module = study.protocol_section.status_module

        start_date_str = (
            status_module.start_date_struct.date
            if status_module.start_date_struct
            else None
        )
        completion_date_str = (
            status_module.primary_completion_date_struct.date
            if status_module.primary_completion_date_struct
            else None
        )

        study_type = (
            study.protocol_section.design_module.study_type
            if study.protocol_section.design_module
            else None
        )

        brief_summary = (
            study.protocol_section.description_module.brief_summary
            if study.protocol_section.description_module
            else None
        )

        self.studies.append(
            {
                "nct_id": nct_id,
                "brief_title": id_module.brief_title,
                "official_title": id_module.official_title,
                "overall_status": status_module.overall_status,
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

        if module.lead_sponsor:
            self.sponsors.append(
                {
                    "nct_id": nct_id,
                    "agency_class": module.lead_sponsor.class_details,
                    "name": module.lead_sponsor.name,
                    "is_lead": True,
                }
            )

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
        module = study.protocol_section.conditions_module
        if module and module.conditions:
            for condition in module.conditions:
                self.conditions.append({"nct_id": nct_id, "name": condition})

    def _transform_interventions(self, nct_id: str, study: Study) -> None:
        module = study.protocol_section.arms_interventions_module
        if not module or not module.interventions:
            return
        for intervention in module.interventions:
            self.interventions.append(
                {
                    "nct_id": nct_id,
                    "intervention_type": intervention.type,
                    "name": intervention.name,
                    "description": intervention.description,
                }
            )

    def _transform_intervention_arm_groups(self, nct_id: str, study: Study) -> None:
        module = study.protocol_section.arms_interventions_module
        if not module or not module.interventions:
            return
        for intervention in module.interventions:
            if not intervention.arm_group_labels:
                continue
            for arm_group_label in intervention.arm_group_labels:
                self.intervention_arm_groups.append(
                    {
                        "nct_id": nct_id,
                        "intervention_name": intervention.name,
                        "arm_group_label": arm_group_label,
                    }
                )

    def _transform_outcomes(self, nct_id: str, study: Study) -> None:
        module = study.protocol_section.outcomes_module
        if not module:
            return

        if module.primary_outcomes:
            for outcome in module.primary_outcomes:
                self.design_outcomes.append(
                    {
                        "nct_id": nct_id,
                        "outcome_type": "PRIMARY",
                        "measure": outcome.measure,
                        "time_frame": outcome.time_frame,
                        "description": outcome.description,
                    }
                )

        if module.other_outcomes:
            for outcome in module.other_outcomes:
                self.design_outcomes.append(
                    {
                        "nct_id": nct_id,
                        "outcome_type": "OTHER",
                        "measure": outcome.measure,
                        "time_frame": outcome.time_frame,
                        "description": outcome.description,
                    }
                )

        if module.secondary_outcomes:
            for outcome in module.secondary_outcomes:
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
        if not date_str:
            return None
        try:
            dt = date_parse(date_str, default=datetime(1, 1, 1))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except (ParserError, TypeError):
            logger.warning("unparseable_date_string", date_string=date_str)
            return None

    def get_dataframes(self) -> Dict[str, pd.DataFrame]:
        dataframes = {
            "raw_studies": pd.DataFrame(self.raw_studies),
            "studies": pd.DataFrame(self.studies),
            "sponsors": pd.DataFrame(self.sponsors),
            "conditions": pd.DataFrame(self.conditions),
            "interventions": pd.DataFrame(self.interventions),
            "intervention_arm_groups": pd.DataFrame(self.intervention_arm_groups),
            "design_outcomes": pd.DataFrame(self.design_outcomes),
        }
        return {name: df for name, df in dataframes.items() if not df.empty}

    def clear(self) -> None:
        self.raw_studies.clear()
        self.studies.clear()
        self.sponsors.clear()
        self.conditions.clear()
        self.interventions.clear()
        self.intervention_arm_groups.clear()
        self.design_outcomes.clear()
