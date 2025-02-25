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

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.pipelines.utils.identifier_models import Event


@attr.s(frozen=True)
class ProgramEvent(Event):
    """Models details related to an event related to a program.

    Describes a date on which a person interacted with a
    program. This includes the information pertaining to the interaction
    that we will want to track when calculating program metrics."""

    # Program ID
    program_id: str = attr.ib()


@attr.s(frozen=True)
class ProgramParticipationEvent(ProgramEvent):
    """Models a ProgramEvent where the person was actively participating in a program."""

    # Program location ID
    program_location_id: Optional[str] = attr.ib(default=None)

    # The type of supervision the person was on
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    @property
    def date_of_participation(self) -> date:
        return self.event_date

    # Whether the date_of_participation was the first day the person participated in the program
    is_first_day_in_program: Optional[bool] = attr.ib(default=None)
