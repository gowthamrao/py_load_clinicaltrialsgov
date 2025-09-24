# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


from pydantic import BaseModel, Field
from typing import List, Optional, Any


class ArmGroup(BaseModel):
    label: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None


class Intervention(BaseModel):
    type: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    arm_group_labels: Optional[List[str]] = Field(None, alias="armGroupLabels")


class ArmsInterventionsModule(BaseModel):
    arm_groups: Optional[List[ArmGroup]] = Field(None, alias="armGroups")
    interventions: Optional[List[Intervention]] = None


class Outcome(BaseModel):
    measure: Optional[str] = None
    description: Optional[str] = None
    time_frame: Optional[str] = Field(None, alias="timeFrame")


class Sponsor(BaseModel):
    name: Optional[str] = None
    class_details: Optional[str] = Field(None, alias="class")


class SponsorCollaboratorsModule(BaseModel):
    lead_sponsor: Optional[Sponsor] = Field(None, alias="leadSponsor")
    collaborators: Optional[List[Sponsor]] = None


class OutcomesModule(BaseModel):
    primary_outcomes: Optional[List[Outcome]] = Field(None, alias="primaryOutcomes")
    secondary_outcomes: Optional[List[Outcome]] = Field(None, alias="secondaryOutcomes")
    other_outcomes: Optional[List[Outcome]] = Field(None, alias="otherOutcomes")


class DescriptionModule(BaseModel):
    brief_summary: Optional[str] = Field(None, alias="briefSummary")
    detailed_description: Optional[str] = Field(None, alias="detailedDescription")


class ConditionsModule(BaseModel):
    conditions: Optional[List[str]] = None


class DateStruct(BaseModel):
    date: Optional[str] = None
    type: Optional[str] = None


class IdentificationModule(BaseModel):
    nct_id: str = Field(..., alias="nctId")
    brief_title: Optional[str] = Field(None, alias="briefTitle")
    official_title: Optional[str] = Field(None, alias="officialTitle")


class StatusModule(BaseModel):
    overall_status: Optional[str] = Field(None, alias="overallStatus")
    start_date_struct: Optional[DateStruct] = Field(None, alias="startDateStruct")
    primary_completion_date_struct: Optional[DateStruct] = Field(
        None, alias="primaryCompletionDateStruct"
    )
    last_update_post_date_struct: Optional[DateStruct] = Field(
        None, alias="lastUpdatePostDateStruct"
    )


class DesignModule(BaseModel):
    study_type: Optional[str] = Field(None, alias="studyType")
    phases: Optional[List[str]] = None


class ProtocolSection(BaseModel):
    identification_module: IdentificationModule = Field(
        ..., alias="identificationModule"
    )
    status_module: StatusModule = Field(..., alias="statusModule")
    sponsor_collaborators_module: Optional[SponsorCollaboratorsModule] = Field(
        None, alias="sponsorCollaboratorsModule"
    )
    oversight_module: Optional[dict[str, Any]] = Field(None, alias="oversightModule")
    description_module: Optional[DescriptionModule] = Field(
        None, alias="descriptionModule"
    )
    conditions_module: Optional[ConditionsModule] = Field(
        None, alias="conditionsModule"
    )
    design_module: Optional[DesignModule] = Field(None, alias="designModule")
    arms_interventions_module: Optional[ArmsInterventionsModule] = Field(
        None, alias="armsInterventionsModule"
    )
    outcomes_module: Optional[OutcomesModule] = Field(None, alias="outcomesModule")
    eligibility_module: Optional[dict[str, Any]] = Field(
        None, alias="eligibilityModule"
    )
    contacts_locations_module: Optional[dict[str, Any]] = Field(
        None, alias="contactsLocationsModule"
    )
    references_module: Optional[dict[str, Any]] = Field(None, alias="referencesModule")


class DerivedSection(BaseModel):
    misc_info_module: Optional[dict[str, Any]] = Field(None, alias="miscInfoModule")
    condition_browse_module: Optional[dict[str, Any]] = Field(
        None, alias="conditionBrowseModule"
    )
    intervention_browse_module: Optional[dict[str, Any]] = Field(
        None, alias="interventionBrowseModule"
    )


class Study(BaseModel):
    protocol_section: ProtocolSection = Field(..., alias="protocolSection")
    derived_section: Optional[DerivedSection] = Field(None, alias="derivedSection")
    has_results: Optional[bool] = Field(None, alias="hasResults")


class APIResponse(BaseModel):
    studies: List[Study]
    next_page_token: Optional[str] = Field(None, alias="nextPageToken")
