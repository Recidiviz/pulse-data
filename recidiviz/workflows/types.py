#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  ============================================================================
"""Type definitions for Workflows products"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import attr
import attrs
import cattrs

from recidiviz.persistence.database.schema.workflows.schema import (
    OpportunityConfiguration,
    OpportunityStatus,
)


class WorkflowsSystemType(Enum):
    INCARCERATION = "INCARCERATION"
    SUPERVISION = "SUPERVISION"


@attr.s
class OpportunityInfo:
    """Basic information about an opportunity."""

    state_code: str = attr.ib()
    opportunity_type: str = attr.ib()
    system_type: WorkflowsSystemType = attr.ib()
    url_section: str = attr.ib()
    firestore_collection: str = attr.ib()
    homepage_position: Optional[int] = attr.ib()

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s
class FullOpportunityInfo(OpportunityInfo):
    """The opportunity info with additional fields not used in the client
    such as gating information."""

    experiment_id: str = attr.ib()
    completion_event: str = attr.ib()
    last_updated_at: Optional[datetime] = attr.ib()
    last_updated_by: Optional[str] = attr.ib()
    gating_feature_variant: Optional[str] = attr.ib(default=None)


@attr.s
class OpportunityConfig:
    """Customizable information used to configure how an opportunity
    is displayed in the client front-end. Managed and updated via
    the admin panel. May vary from user to user."""

    state_code: str = attr.ib()
    opportunity_type: str = attr.ib()
    priority: str = attr.ib()
    display_name: str = attr.ib()
    methodology_url: str = attr.ib()
    initial_header: Optional[str] = attr.ib()
    denial_reasons: List[Dict[str, str]] = attr.ib()
    eligible_criteria_copy: List[Dict[str, Optional[str]]] = attr.ib()
    ineligible_criteria_copy: List[Dict[str, Optional[str]]] = attr.ib()
    strictly_ineligible_criteria_copy: List[Dict[str, Optional[str]]] = attr.ib()
    dynamic_eligibility_text: str = attr.ib()
    eligibility_date_text: Optional[str] = attr.ib()
    hide_denial_revert: bool = attr.ib()
    tooltip_eligibility_text: Optional[str] = attr.ib()
    call_to_action: Optional[str] = attr.ib()
    subheading: Optional[str] = attr.ib()
    snooze: Dict[str, Any] | None = attr.ib()
    is_alert: bool = attr.ib()
    sidebar_components: List[str] = attr.ib()
    denial_text: Optional[str] = attr.ib()
    tab_groups: Optional[List[Dict[str, Any]]] = attr.ib()
    compare_by: Optional[List[Any]] = attr.ib()
    notifications: List[Any] = attr.ib()
    zero_grants_tooltip: Optional[str] = attr.ib()
    supports_ineligible: bool = attr.ib()

    denied_tab_title: Optional[str] = attr.ib()
    denial_adjective: Optional[str] = attr.ib()
    denial_noun: Optional[str] = attr.ib()

    supports_submitted: bool = attr.ib()
    submitted_tab_title: Optional[str] = attr.ib()

    empty_tab_copy: list[dict[str, str]] = attr.ib()
    tab_preface_copy: list[dict[str, str]] = attr.ib()

    subcategory_headings: list[dict[str, str]] = attr.ib()
    subcategory_orderings: list[dict[str, Any]] = attr.ib()
    mark_submitted_options_by_tab: list[dict[str, Any]] = attr.ib()

    oms_criteria_header: Optional[str] = attr.ib()
    non_oms_criteria_header: Optional[str] = attr.ib()
    non_oms_criteria: list[dict[str, Optional[str]]] = attr.ib()

    highlight_cases_on_homepage: bool = attr.ib()
    highlighted_case_cta_copy: Optional[str] = attr.ib()
    overdue_opportunity_callout_copy: Optional[str] = attr.ib()

    snooze_companion_opportunity_types: Optional[List[str]] = attr.ib()

    case_notes_title: Optional[str] = attr.ib()

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)

    @classmethod
    def from_full_config(
        cls, full_config: "FullOpportunityConfig"
    ) -> "OpportunityConfig":
        # Copy only the fields from the other config object that are valid fields
        # for an OpportunityConfig
        return OpportunityConfig(
            **{
                field.name: getattr(full_config, field.name)
                for field in attrs.fields(OpportunityConfig)
            }
        )


@attr.s
class FullOpportunityConfig(OpportunityConfig):
    """The opportunity config with additional fields not used in the client
    such as metadata around creation and use."""

    id: int = attr.ib()
    created_by: str = attr.ib()
    created_at: datetime = attr.ib()
    variant_description: str = attr.ib()
    revision_description: str = attr.ib()
    status: OpportunityStatus = attr.ib()
    feature_variant: str = attr.ib(default=None)
    staging_id: int = attr.ib(default=None)

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)

    @classmethod
    def from_db_entry(cls, config: OpportunityConfiguration) -> "FullOpportunityConfig":
        # Copy only the fields from the other config object that are valid fields
        # for a FullOpportunityConfig
        return FullOpportunityConfig(
            **{
                field.name: getattr(config, field.name)
                for field in attrs.fields(FullOpportunityConfig)
            }
        )


@attr.s
class OpportunityConfigResponse(OpportunityInfo, OpportunityConfig):
    """A combination of all the configuration information the API returns"""

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)

    @classmethod
    def from_opportunity_and_config(
        cls, opportunity: FullOpportunityInfo, config: OpportunityConfig
    ) -> "OpportunityConfigResponse":
        # include fields from both parameters that exist on OpportunityConfigResponse
        # (i.e. only the base OpportunityInfo fields from `opportunity`, dropping the
        # internal fields not used in the client), with the opportunity info taking priority

        conv = cattrs.Converter()

        return conv.structure(
            conv.unstructure(config) | conv.unstructure(opportunity), cls
        )
