# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Events related to incarceration."""
from datetime import date
from typing import Optional

import attr
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration


@attr.s(frozen=True)
class IncarcerationEvent(BuildableAttr):
    """Models details related to an incarceration event.

    Describes a date on which a person interacted with incarceration. This includes the information pertaining to the
    interaction that we will want to track when calculating incarceration metrics."""

    # The state where the incarceration took place
    state_code: str = attr.ib()

    # Event date when the interaction took place
    event_date: date = attr.ib()

    # Facility
    facility: Optional[str] = attr.ib(default=None)

    # County of residence
    county_of_residence: Optional[str] = attr.ib(default=None)


@attr.s(frozen=True)
class IncarcerationStayEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person spent time incarcerated during the given day."""


@attr.s(frozen=True)
class IncarcerationAdmissionEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person was admitted to incarceration for any reason."""

    # Admission reason
    admission_reason: StateIncarcerationPeriodAdmissionReason = attr.ib(default=None)

    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[StateSpecializedPurposeForIncarceration] = attr.ib(default=None)


@attr.s(frozen=True)
class IncarcerationReleaseEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person was released from incarceration for any reason."""

    # Release reason
    release_reason: StateIncarcerationPeriodReleaseReason = attr.ib(default=None)
