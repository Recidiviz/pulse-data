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
"""Events related to programs."""
from typing import Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType


@attr.s(frozen=True)
class ProgramEvent(BuildableAttr):
    """Models details related to an event related to a program.

    Describes either a year or a month in which a person interacted with a
    program. This includes the information pertaining to the interaction
    that we will want to track when calculating program metrics."""

    # The state where the program took place
    state_code: str = attr.ib()

    # Program ID
    program_id: str = attr.ib()

    # Year for when the person interacted with the program
    year: int = attr.ib()

    # Month for when the person interacted with the program
    month: Optional[int] = attr.ib()


@attr.s(frozen=True)
class ProgramReferralEvent(ProgramEvent):
    """Models a ProgramEvent where a the person was referred to a program."""

    # The type of supervision the person was on
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    # Most recent assessment score at the time of referral
    assessment_score: Optional[int] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # External ID of the officer who was supervising the person
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the district of the officer that was supervising the
    # person
    supervising_district_external_id: Optional[str] = attr.ib(default=None)
