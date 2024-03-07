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

import attr

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
    gating_feature_variant: str = attr.ib(default=None)


@attr.s
class OpportunityConfig:
    """Customizable information used to configure how an opportunity
    is displayed in the client front-end. Managed and updated via
    the admin panel. May vary from user to user."""

    id: int = attr.ib()
    state_code: str = attr.ib()
    opportunity_type: str = attr.ib()
    created_by: str = attr.ib()
    created_at: datetime = attr.ib()
    description: str = attr.ib()
    status: OpportunityStatus = attr.ib()
    display_name: str = attr.ib()
    methodology_url: str = attr.ib()
    initial_header: str = attr.ib()
    dynamic_eligibility_text: str = attr.ib()
    call_to_action: str = attr.ib()
    snooze: str = attr.ib()
    feature_variant: str = attr.ib(default=None)
    is_alert: bool = attr.ib(default=False)
    denial_text: str = attr.ib(default=None)
