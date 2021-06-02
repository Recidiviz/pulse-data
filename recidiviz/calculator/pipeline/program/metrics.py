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
        return "TODO(#7563): Add ProgramReferralMetric description"

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
        return "TODO(#7563): Add ProgramParticipationMetric description"

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
