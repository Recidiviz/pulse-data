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
"""Utility function for Workflows products."""

from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIG_MAP,
    PersonRecordType,
    WorkflowsOpportunityConfig,
)
from recidiviz.workflows.types import WorkflowsSystemType


def get_config_for_opportunity(opportunity_type: str) -> WorkflowsOpportunityConfig:
    """Given an opportunity type, returns the respective config from
    WORKFLOWS_OPPORTUNITY_CONFIGS or throws an error if not available"""
    static_config = WORKFLOWS_OPPORTUNITY_CONFIG_MAP.get(opportunity_type)

    if not static_config:
        raise ValueError(
            f"No opportunity config found for opportunity type {opportunity_type}"
        )

    return static_config


def get_system_for_opportunity(opportunity_type: str) -> WorkflowsSystemType:
    """Given an opportunity type, returns the system
    it is assigned to in WORKFLOWS_OPPORTUNITY_CONFIGS"""

    static_config = get_config_for_opportunity(opportunity_type)

    match static_config.person_record_type:
        case PersonRecordType.CLIENT:
            return WorkflowsSystemType.SUPERVISION
        case PersonRecordType.RESIDENT:
            return WorkflowsSystemType.INCARCERATION

    raise ValueError(
        f"Unknown person type ({static_config.person_record_type}) for opportunity type {opportunity_type}"
    )
