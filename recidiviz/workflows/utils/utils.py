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
    WORKFLOWS_OPPORTUNITY_CONFIGS,
    PersonRecordType,
    WorkflowsOpportunityConfig,
)
from recidiviz.workflows.types import WorkflowsSystemType


def get_configs() -> list[WorkflowsOpportunityConfig]:
    return WORKFLOWS_OPPORTUNITY_CONFIGS


def get_system_for_config(config: WorkflowsOpportunityConfig) -> WorkflowsSystemType:
    """Given an opportunity type, returns the system
    it is assigned to in WORKFLOWS_OPPORTUNITY_CONFIGS"""

    match config.person_record_type:
        case PersonRecordType.CLIENT:
            return WorkflowsSystemType.SUPERVISION
        case PersonRecordType.RESIDENT:
            return WorkflowsSystemType.INCARCERATION

    raise ValueError(
        f"Unknown person type ({config.person_record_type}) for opportunity type {config.opportunity_type}"
    )
