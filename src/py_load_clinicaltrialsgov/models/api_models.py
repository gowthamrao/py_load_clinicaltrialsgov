from pydantic import BaseModel, Field
from typing import List, Optional

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

class OutcomesModule(BaseModel):
    primary_outcomes: Optional[List[Outcome]] = Field(None, alias="primaryOutcomes")
    secondary_outcomes: Optional[List[Outcome]] = Field(None, alias="secondaryOutcomes")
    other_outcomes: Optional[List[Outcome]] = Field(None, alias="otherOutcomes")


class ProtocolSection(BaseModel):
    # This is a placeholder, will be populated based on detailed JSON structure
    identification_module: Optional[dict] = Field(None, alias="identificationModule")
    status_module: Optional[dict] = Field(None, alias="statusModule")
    sponsor_collaborators_module: Optional[dict] = Field(None, alias="sponsorCollaboratorsModule")
    oversight_module: Optional[dict] = Field(None, alias="oversightModule")
    description_module: Optional[dict] = Field(None, alias="descriptionModule")
    conditions_module: Optional[dict] = Field(None, alias="conditionsModule")
    design_module: Optional[dict] = Field(None, alias="designModule")
    arms_interventions_module: Optional[ArmsInterventionsModule] = Field(None, alias="armsInterventionsModule")
    outcomes_module: Optional[OutcomesModule] = Field(None, alias="outcomesModule")
    eligibility_module: Optional[dict] = Field(None, alias="eligibilityModule")
    contacts_locations_module: Optional[dict] = Field(None, alias="contactsLocationsModule")
    references_module: Optional[dict] = Field(None, alias="referencesModule")

class DerivedSection(BaseModel):
    # This is a placeholder, will be populated based on detailed JSON structure
    misc_info_module: Optional[dict] = Field(None, alias="miscInfoModule")
    condition_browse_module: Optional[dict] = Field(None, alias="conditionBrowseModule")
    intervention_browse_module: Optional[dict] = Field(None, alias="interventionBrowseModule")

class Study(BaseModel):
    protocol_section: ProtocolSection = Field(..., alias="protocolSection")
    derived_section: DerivedSection = Field(..., alias="derivedSection")
    has_results: bool = Field(..., alias="hasResults")

class APIResponse(BaseModel):
    studies: List[Study]
    next_page_token: Optional[str] = Field(None, alias="nextPageToken")
