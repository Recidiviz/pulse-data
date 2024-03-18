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
from typing import Any, Dict, Optional

import attr
import cattrs

from recidiviz.persistence.database.schema.workflows.schema import OpportunityStatus


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

    def to_dict(self) -> Dict[str, Any]:
        return cattrs.unstructure(self)


@attr.s
class FullOpportunityInfo(OpportunityInfo):
    """The opportunity info with additional fields not used in the client
    such as gating information."""

    gating_feature_variant: str = attr.ib(default=None)


@attr.s
class OpportunityConfig:
    """Customizable information used to configure how an opportunity
    is displayed in the client front-end. Managed and updated via
    the admin panel. May vary from user to user."""

    state_code: str = attr.ib()
    opportunity_type: str = attr.ib()
    display_name: str = attr.ib()
    methodology_url: str = attr.ib()
    initial_header: str = attr.ib()
    dynamic_eligibility_text: str = attr.ib()
    call_to_action: str = attr.ib()
    snooze: str = attr.ib()
    is_alert: bool = attr.ib()
    denial_text: Optional[str] = attr.ib()

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
            dynamic_eligibility_text=full_config.dynamic_eligibility_text,
            call_to_action=full_config.call_to_action,
            snooze=full_config.snooze,
            is_alert=full_config.is_alert,
            denial_text=full_config.denial_text,
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
            display_name=config.display_name,
            methodology_url=config.methodology_url,
            initial_header=config.initial_header,
            dynamic_eligibility_text=config.dynamic_eligibility_text,
            call_to_action=config.call_to_action,
            snooze=config.snooze,
            is_alert=config.is_alert,
            denial_text=config.denial_text,
        )
