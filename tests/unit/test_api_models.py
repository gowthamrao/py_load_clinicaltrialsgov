# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import pytest
from pydantic import ValidationError

from load_clinicaltrialsgov.models.api_models import (
    DescriptionModule,
    ConditionsModule,
    ProtocolSection,
)


from typing import Any


def test_description_module_valid() -> None:
    """Tests that a valid DescriptionModule is parsed correctly."""
    data: dict[str, Any] = {"briefSummary": "This is a test summary."}
    module = DescriptionModule.model_validate(data)
    assert module.brief_summary == "This is a test summary."


def test_description_module_empty() -> None:
    """Tests that an empty DescriptionModule is parsed correctly."""
    data: dict[str, Any] = {}
    module = DescriptionModule.model_validate(data)
    assert module.brief_summary is None


def test_conditions_module_valid() -> None:
    """Tests that a valid ConditionsModule is parsed correctly."""
    data: dict[str, Any] = {"conditions": ["Cancer", "Diabetes"]}
    module = ConditionsModule.model_validate(data)
    assert module.conditions == ["Cancer", "Diabetes"]


def test_conditions_module_empty_list() -> None:
    """Tests that a ConditionsModule with an empty list is parsed correctly."""
    data: dict[str, Any] = {"conditions": []}
    module = ConditionsModule.model_validate(data)
    assert module.conditions == []


def test_conditions_module_null_conditions() -> None:
    """Tests that a ConditionsModule with null conditions is parsed correctly."""
    data: dict[str, Any] = {"conditions": None}
    module = ConditionsModule.model_validate(data)
    assert module.conditions is None


def test_protocol_section_with_new_models() -> None:
    """
    Tests that the ProtocolSection correctly uses the new, typed models.
    """
    data: dict[str, Any] = {
        "identificationModule": {"nctId": "NCT123"},
        "statusModule": {"overallStatus": "COMPLETED"},
        "descriptionModule": {"briefSummary": "A valid summary."},
        "conditionsModule": {"conditions": ["Condition 1"]},
    }
    protocol = ProtocolSection.model_validate(data)
    assert isinstance(protocol.description_module, DescriptionModule)
    assert protocol.description_module.brief_summary == "A valid summary."
    assert isinstance(protocol.conditions_module, ConditionsModule)
    assert protocol.conditions_module.conditions == ["Condition 1"]


def test_protocol_section_validation_error() -> None:
    """
    Tests that a validation error is raised for invalid data in the new models.
    """
    data: dict[str, Any] = {
        "identificationModule": {"nctId": "NCT123"},
        "statusModule": {"overallStatus": "COMPLETED"},
        # Pass a non-string to briefSummary
        "descriptionModule": {"briefSummary": 12345},
        "conditionsModule": {"conditions": ["Condition 1"]},
    }
    with pytest.raises(ValidationError) as excinfo:
        ProtocolSection.model_validate(data)

    # Check that the error message points to the right field
    # Pydantic's error `loc` uses the JSON field names, not the model attribute names
    errors = excinfo.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("descriptionModule", "briefSummary")
    assert errors[0]["type"] == "string_type"
