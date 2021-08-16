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
"""Program metrics we calculate."""
import abc
from datetime import date
from typing import Optional

import attr

from recidiviz.calculator.pipeline.utils.event_utils import SupervisionLocationMixin
from recidiviz.calculator.pipeline.utils.metric_utils import (
    AssessmentMetricMixin,
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType


class ProgramMetricType(RecidivizMetricType):
    """The type of program metrics."""

    PROGRAM_PARTICIPATION = "PROGRAM_PARTICIPATION"
    PROGRAM_REFERRAL = "PROGRAM_REFERRAL"


@attr.s
class ProgramMetric(RecidivizMetric[ProgramMetricType], PersonLevelMetric):
    """Models a single program metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """

    # Required characteristics
    metric_type_cls = ProgramMetricType

    # The type of ProgramMetric
    metric_type: ProgramMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Optional characteristics

    # Month
    month: Optional[int] = attr.ib(default=None)

    # Program ID
    program_id: str = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class ProgramReferralMetric(
    ProgramMetric, AssessmentMetricMixin, SupervisionLocationMixin
):
    """Subclass of ProgramMetric that contains program referral information."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `ProgramReferralMetric` stores information about a person getting referred to rehabilitative programming. This metric tracks the day that a person was referred by someone (usually a correctional or supervision officer) to a given program, and stores information related to that referral.

With this metric, we can answer questions like:

- Of all of the people referred to Program X last month, how many are now actively participating in the program?
- How many people have been referred to Program Y since it was introduced in January 2017?
- Which supervision district referred the most people to Program Z in 2020?

 
This metric is derived from the `StateProgramAssignment` entities, which store information about the assignment of a person to some form of rehabilitative programming -- and their participation in the program -- intended to address specific needs of the person. The calculations for this metric look for `StateProgramAssignment` instances with a `referral_date` to determine that a program referral occurred. 

If a person was referred to Program X on April 1, 2020, then there will be a `ProgramReferralMetric` for April 1, 2020.

If a person was referred to a program while they were on supervision, then this metric records information about the supervision the person was on on the date of the referral. If a person is serving multiple supervisions simultaneously (and has multiple `StateSupervisionPeriod` entities that overlap a referral date) then there will be one `ProgramReferralMetric` produced for each overlapping supervision period. So, if a person was referred to Program Z on December 3, 2014, and on that day the person was serving both probation and parole simultaneously (represented by two overlapping `StateSupervisionPeriod` entities), then there will be two `ProgramReferralMetrics` produced: one with a `supervision_type` of `PAROLE` and one with a `supervision_type` of `PROBATION`.
"""

    # Required characteristics

    # The type of ProgramMetric
    metric_type: ProgramMetricType = attr.ib(
        init=False, default=ProgramMetricType.PROGRAM_REFERRAL
    )

    # The date on which the referral took place
    date_of_referral: date = attr.ib(default=None)

    # Optional characteristics

    # Supervision Type
    # TODO(#2891): Make this of type StateSupervisionPeriodSupervisionType
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    # Program participation status
    participation_status: Optional[StateProgramAssignmentParticipationStatus] = attr.ib(
        default=None
    )

    # External ID of the officer who was supervising the person described by this metric
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)


@attr.s
class ProgramParticipationMetric(ProgramMetric):
    """Subclass of ProgramMetric that contains program participation information."""

    @classmethod
    def get_description(cls) -> str:
        return """
The `ProgramParticipationMetric` stores information about a person participating in rehabilitative programming. This metric tracks each day that a person was actively participating in a given program, and stores information related to that participation.

With this metric, we can answer questions like:

- How many people participated in Program X in the month of April 2020?
- How has the participation in Program Y grown since it was introduced in January 2017?
- Of all of the people currently participating in Program Z in the state, what percent are under the age of 30?

 
This metric is derived from the `StateProgramAssignment` entities, which store information about the assignment of a person to some form of rehabilitative programming -- and their participation in the program -- intended to address specific needs of the person. The calculations for this metric look for `StateProgramAssignment` instances with a `participation_status` of either `IN_PROGRESS` or `DISCHARGED`, and use the `start_date` and `discharge_date` fields to produce a single `ProgramParticipationMetric` for each day that a person was actively participating in the program.

If a person started participating in Program X on April 1, 2020 and were discharged on May 1, 2020, then there will be a `ProgramParticipationMetric` for each day of participation in Program X (30 `ProgramParticipationMetric` outputs in total).

If a person is participating in a program while they are on supervision, then this metric records the supervision type the person was on on the date of the participation. If a person is serving multiple supervisions simultaneously (and has multiple `StateSupervisionPeriod` entities that overlap a participation date) then there will be one `ProgramParticipationMetric` produced for each overlapping supervision period. So, if a person participated in Program Z for a single day, on October 26, 2014, and on that day the person was serving both probation and parole simultaneously (represented by two overlapping `StateSupervisionPeriod` entities), then there will be two `ProgramParticipationMetrics` produced: one with a `supervision_type` of `PAROLE` and one with a `supervision_type` of `PROBATION`.     
"""

    # Required characteristics

    # The type of ProgramMetric
    metric_type: ProgramMetricType = attr.ib(
        init=False, default=ProgramMetricType.PROGRAM_PARTICIPATION
    )

    # Date of active participation
    date_of_participation: date = attr.ib(default=None)

    # Whether the date_of_participation was the first day the person participated in the program
    is_first_day_in_program: Optional[bool] = attr.ib(default=None)

    # Optional characteristics

    # Program location ID
    program_location_id: Optional[str] = attr.ib(default=None)

    # Supervision Type
    # TODO(#2891): Make this of type StateSupervisionPeriodSupervisionType
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)
