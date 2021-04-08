# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Events related to programs."""
from datetime import date
from typing import Optional

import attr

from recidiviz.calculator.pipeline.utils.event_utils import (
    AssessmentEventMixin,
    IdentifierEventWithSingularDate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentType,
    StateAssessmentLevel,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType


@attr.s(frozen=True)
class ProgramEvent(IdentifierEventWithSingularDate):
    """Models details related to an event related to a program.

    Describes a date on which a person interacted with a
    program. This includes the information pertaining to the interaction
    that we will want to track when calculating program metrics."""

    # Program ID
    program_id: str = attr.ib()


@attr.s(frozen=True)
class ProgramReferralEvent(ProgramEvent, AssessmentEventMixin):
    """Models a ProgramEvent where a the person was referred to a program."""

    # The type of supervision the person was on
    # TODO(#2891): Make this of type StateSupervisionPeriodSupervisionType
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    # Program participation status
    participation_status: Optional[StateProgramAssignmentParticipationStatus] = attr.ib(
        default=None
    )

    # Most recent assessment score at the time of referral
    assessment_score: Optional[int] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # Most recent assessment level
    assessment_level: Optional[StateAssessmentLevel] = attr.ib(default=None)

    # External ID of the officer who was supervising the person
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the district of the officer that was supervising the person
    # TODO(#4709): THIS FIELD IS DEPRECATED - USE level_1_supervision_location_external_id and
    #  level_2_supervision_location_external_id instead.
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the lowest-level sub-geography (e.g. an individual office with a street address) of the officer
    # that was supervising the person described by this metric.
    level_1_supervision_location_external_id: Optional[str] = attr.ib(default=None)

    # For states with a hierachical structure of supervision locations, this is the external ID the next-lowest-level
    # sub-geography after level_1_supervision_sub_geography_external_id. For example, in PA this is a "district" where
    # level 1 is an office.
    level_2_supervision_location_external_id: Optional[str] = attr.ib(default=None)

    @property
    def date_of_referral(self) -> date:
        return self.event_date


@attr.s(frozen=True)
class ProgramParticipationEvent(ProgramEvent):
    """Models a ProgramEvent where a the person was actively participating in a program."""

    # Program location ID
    program_location_id: Optional[str] = attr.ib(default=None)

    # The type of supervision the person was on
    # TODO(#2891): Make this of type StateSupervisionPeriodSupervisionType
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    @property
    def date_of_participation(self) -> date:
        return self.event_date

    # Whether the date_of_participation was the first day the person participated in the program
    is_first_day_in_program: Optional[bool] = attr.ib(default=None)
