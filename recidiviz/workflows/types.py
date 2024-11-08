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
    denial_reasons: Dict[str, str] = attr.ib()
    eligible_criteria_copy: Dict[str, Any] = attr.ib()
    ineligible_criteria_copy: Dict[str, Any] = attr.ib()
    dynamic_eligibility_text: str = attr.ib()
    eligibility_date_text: Optional[str] = attr.ib()
    hide_denial_revert: bool = attr.ib()
    tooltip_eligibility_text: Optional[str] = attr.ib()
    call_to_action: str = attr.ib()
    subheading: str = attr.ib()
    snooze: Dict[str, Any] | None = attr.ib()
    is_alert: bool = attr.ib()
    sidebar_components: List[str] = attr.ib()
    denial_text: Optional[str] = attr.ib()
    tab_groups: Optional[Dict[str, List[str]]] = attr.ib()
    compare_by: Optional[List[Any]] = attr.ib()
    notifications: List[Any] = attr.ib()
    zero_grants_tooltip: Optional[str] = attr.ib()

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)

    @classmethod
    def from_full_config(
        cls, full_config: "FullOpportunityConfig"
    ) -> "OpportunityConfig":
        return OpportunityConfig(
            state_code=full_config.state_code,
            opportunity_type=full_config.opportunity_type,
            display_name=full_config.display_name,
            methodology_url=full_config.methodology_url,
            initial_header=full_config.initial_header,
            denial_reasons=full_config.denial_reasons,
            eligible_criteria_copy=full_config.eligible_criteria_copy,
            ineligible_criteria_copy=full_config.ineligible_criteria_copy,
            dynamic_eligibility_text=full_config.dynamic_eligibility_text,
            eligibility_date_text=full_config.eligibility_date_text,
            hide_denial_revert=full_config.hide_denial_revert,
            tooltip_eligibility_text=full_config.tooltip_eligibility_text,
            call_to_action=full_config.call_to_action,
            subheading=full_config.subheading,
            snooze=full_config.snooze,
            is_alert=full_config.is_alert,
            priority=full_config.priority,
            denial_text=full_config.denial_text,
            sidebar_components=full_config.sidebar_components,
            tab_groups=full_config.tab_groups,
            compare_by=full_config.compare_by,
            notifications=full_config.notifications,
            zero_grants_tooltip=full_config.zero_grants_tooltip,
        )


@attr.s
class FullOpportunityConfig(OpportunityConfig):
    """The opportunity config with additional fields not used in the client
    such as metadata around creation and use."""

    id: int = attr.ib()
    created_by: str = attr.ib()
    created_at: datetime = attr.ib()
    description: str = attr.ib()
    status: OpportunityStatus = attr.ib()
    feature_variant: str = attr.ib(default=None)
    staging_id: int = attr.ib(default=None)

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)

    @classmethod
    def from_db_entry(cls, config: OpportunityConfiguration) -> "FullOpportunityConfig":
        return FullOpportunityConfig(
            id=config.id,
            state_code=config.state_code,
            opportunity_type=config.opportunity_type,
            created_by=config.created_by,
            created_at=config.created_at,
            description=config.description,
            status=config.status,
            display_name=config.display_name,
            methodology_url=config.methodology_url,
            initial_header=config.initial_header,
            denial_reasons=config.denial_reasons,
            eligible_criteria_copy=config.eligible_criteria_copy,
            ineligible_criteria_copy=config.ineligible_criteria_copy,
            dynamic_eligibility_text=config.dynamic_eligibility_text,
            call_to_action=config.call_to_action,
            subheading=config.subheading,
            eligibility_date_text=config.eligibility_date_text,
            hide_denial_revert=config.hide_denial_revert,
            tooltip_eligibility_text=config.tooltip_eligibility_text,
            snooze=config.snooze,
            feature_variant=config.feature_variant,
            is_alert=config.is_alert,
            priority=config.priority,
            denial_text=config.denial_text,
            sidebar_components=config.sidebar_components,
            tab_groups=config.tab_groups,
            compare_by=config.compare_by,
            notifications=config.notifications,
            staging_id=config.staging_id,
            zero_grants_tooltip=config.zero_grants_tooltip,
        )


@attr.s
class OpportunityConfigResponse(OpportunityInfo, OpportunityConfig):
    """A combination of all the configuration information the API returns"""

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)

    @classmethod
    def from_opportunity_and_config(
        cls, opportunity: OpportunityInfo, config: OpportunityConfig
    ) -> "OpportunityConfigResponse":
        return OpportunityConfigResponse(
            state_code=opportunity.state_code,
            opportunity_type=opportunity.opportunity_type,
            system_type=opportunity.system_type,
            url_section=opportunity.url_section,
            firestore_collection=opportunity.firestore_collection,
            homepage_position=opportunity.homepage_position,
            display_name=config.display_name,
            methodology_url=config.methodology_url,
            initial_header=config.initial_header,
            denial_reasons=config.denial_reasons,
            eligible_criteria_copy=config.eligible_criteria_copy,
            ineligible_criteria_copy=config.ineligible_criteria_copy,
            dynamic_eligibility_text=config.dynamic_eligibility_text,
            eligibility_date_text=config.eligibility_date_text,
            hide_denial_revert=config.hide_denial_revert,
            tooltip_eligibility_text=config.tooltip_eligibility_text,
            call_to_action=config.call_to_action,
            subheading=config.subheading,
            snooze=config.snooze,
            priority=config.priority,
            is_alert=config.is_alert,
            denial_text=config.denial_text,
            sidebar_components=config.sidebar_components,
            tab_groups=config.tab_groups,
            compare_by=config.compare_by,
            notifications=config.notifications,
            zero_grants_tooltip=config.zero_grants_tooltip,
        )
