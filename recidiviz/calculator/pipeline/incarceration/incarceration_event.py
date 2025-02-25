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
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType


@attr.s(frozen=True)
class IncarcerationEvent(BuildableAttr):
    """Models details related to an incarceration event.

    Describes a date on which a person interacted with incarceration. This includes the information pertaining to the
    interaction that we will want to track when calculating incarceration metrics.
    """

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

    # The most serious offense NCIC code connected to the sentence group of the incarceration period from which
    # this stay event is derived
    most_serious_offense_ncic_code: Optional[str] = attr.ib(default=None)

    # The most serious offense statute connected to the sentence group of the incarceration period from which
    # this stay event is derived
    most_serious_offense_statute: Optional[str] = attr.ib(default=None)

    # Admission reason for the incarceration period that overlaps with relevant day.
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(default=None)

    # Admission reason raw text
    admission_reason_raw_text: Optional[str] = attr.ib(default=None)

    # Supervision type at the time of admission, if any.
    supervision_type_at_admission: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(default=None)


@attr.s(frozen=True)
class IncarcerationAdmissionEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person was admitted to incarceration for any reason."""

    # Most relevant admission reason for a continuous stay in prison. For example, in some states, if the initial
    # incarceration period has an admission reason of TEMPORARY_CUSTODY, the admission reason is drawn from the
    # subsequent admission period, if present.
    admission_reason: StateIncarcerationPeriodAdmissionReason = attr.ib(default=None)

    # Admission reason raw text
    admission_reason_raw_text: Optional[str] = attr.ib(default=None)

    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[StateSpecializedPurposeForIncarceration] = attr.ib(default=None)

    # Supervision type at the time of admission, if any.
    supervision_type_at_admission: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(default=None)


@attr.s(frozen=True)
class IncarcerationReleaseEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person was released from incarceration for any reason."""

    # Release reason
    release_reason: StateIncarcerationPeriodReleaseReason = attr.ib(default=None)
